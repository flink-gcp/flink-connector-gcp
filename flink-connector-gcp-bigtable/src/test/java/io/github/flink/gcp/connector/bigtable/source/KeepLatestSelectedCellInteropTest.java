/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadChangeStreamResponse;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.stub.changestream.ChangeStreamRecordMerger;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.TestBigtableChangeStreamMutations;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Connects the factory-built sink serializer to the factory-built selected-cell JSON decoder. */
class KeepLatestSelectedCellInteropTest {

    @Test
    void decodesTheCompleteJsonReplacementAndItsReapplication() throws Exception {
        assertThat(decode(serialize("{\"name\":\"Alice\",\"tier\":\"gold\"}", true)))
                .containsExactly(Row.ofKind(RowKind.UPDATE_AFTER, "profile#1", "Alice", "gold"));
        RowMutationEntry replacement = serialize("{\"name\":null,\"tier\":\"silver\"}", true);
        List<Row> reapplied = new ArrayList<>(decode(replacement));
        reapplied.addAll(decode(serialize("{\"name\":null,\"tier\":\"silver\"}", true)));
        assertThat(reapplied)
                .containsExactly(
                        Row.ofKind(RowKind.UPDATE_AFTER, "profile#1", null, "silver"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "profile#1", null, "silver"));
    }

    @Test
    void theOrdinaryUpsertDoesNotSatisfyTheSelectedCellProtocol() {
        assertThatThrownBy(() -> decode(serialize("{\"name\":\"Alice\",\"tier\":\"gold\"}", false)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid Bigtable selected-cell producer protocol");
    }

    @Test
    void aWholeCellSqlNullCannotStandForADelete() {
        assertThatThrownBy(() -> decode(serialize(null, true)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to deserialize JSON");
    }

    @Test
    void rejectsIncompleteOrMalformedJsonRatherThanEmittingAPartialRow() {
        for (String payload : List.of("{\"tier\":\"silver\"}", "not-json")) {
            assertThatThrownBy(() -> decode(serialize(payload, true)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to deserialize JSON");
        }
    }

    @Test
    void aFullSelectedColumnDeleteEmitsOnlyTheKey() throws Exception {
        RowMutationEntry deletion =
                RowMutationEntry.create("profile#1").deleteCells("state", "current");
        assertThat(decode(deletion))
                .containsExactly(Row.ofKind(RowKind.DELETE, "profile#1", null, null));
    }

    @SuppressWarnings("unchecked")
    private static RowMutationEntry serialize(String json, boolean keepLatest) throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        List.of(
                                Column.physical("profile_id", DataTypes.STRING().notNull()),
                                Column.physical(
                                        "state",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("current", DataTypes.STRING())))),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("profile_id")));
        Map<String, String> options = destination();
        options.put("sink.write-mode", keepLatest ? "keep-latest" : "upsert");
        options.put("emulator-endpoint", "localhost:1");
        SinkV2Provider provider =
                (SinkV2Provider)
                        FactoryMocks.createTableSink(schema, options)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        BigtableMutateRowsSink<RowData> sink =
                (BigtableMutateRowsSink<RowData>) provider.createSink();
        return sink.getConfig()
                .getSerializer()
                .serialize(
                        GenericRowData.of(
                                StringData.fromString("profile#1"),
                                GenericRowData.of(
                                        json == null ? null : StringData.fromString(json))),
                        null);
    }

    @SuppressWarnings("unchecked")
    private static BigtableChangeStreamSourceConfig<RowData> source() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        List.of(
                                Column.physical("profile_id", DataTypes.STRING().notNull()),
                                Column.physical("name", DataTypes.STRING()),
                                Column.physical("tier", DataTypes.STRING())),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("profile_id")));
        Map<String, String> options = destination();
        options.put("scan.mode", "change-stream");
        options.put("scan.change-stream.changelog-mode", "selected-cell");
        options.put("scan.app-profile-id", "single-cluster-profile");
        options.put("scan.change-stream.selected-cell.family", "state");
        options.put("scan.change-stream.selected-cell.qualifier-base64", "Y3VycmVudA==");
        options.put("scan.change-stream.selected-cell.source-cluster-id", "cluster-a");
        options.put("value.format", "json");
        options.put("value.json.fail-on-missing-field", "true");
        options.put("value.json.ignore-parse-errors", "false");
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) FactoryMocks.createTableSource(schema, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return ((BigtableChangeStreamSource<RowData>) provider.createSource()).getConfig();
    }

    private static Map<String, String> destination() {
        return new HashMap<>(
                Map.of(
                        "connector", "bigtable",
                        "project", "my-project",
                        "instance", "my-instance",
                        "table", "profiles"));
    }

    private static List<Row> decode(RowMutationEntry entry) throws Exception {
        var decoder = source().getDeserializer();
        decoder.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return new UnregisteredMetricsGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return SimpleUserCodeClassLoader.create(
                                KeepLatestSelectedCellInteropTest.class.getClassLoader());
                    }
                });
        List<Row> output = new ArrayList<>();
        decoder.deserialize(
                asChangeStream(entry),
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData row) {
                        output.add(
                                Row.ofKind(
                                        row.getRowKind(),
                                        row.getString(0).toString(),
                                        row.isNullAt(1) ? null : row.getString(1).toString(),
                                        row.isNullAt(2) ? null : row.getString(2).toString()));
                    }

                    @Override
                    public void close() {}
                });
        return output;
    }

    private static BigtableChangeStreamMutation asChangeStream(RowMutationEntry entry) {
        Timestamp committedAt =
                Timestamp.newBuilder()
                        .setSeconds(Instant.parse("2026-09-06T00:00:00Z").getEpochSecond())
                        .build();
        var dataChange =
                ReadChangeStreamResponse.DataChange.newBuilder()
                        .setType(ReadChangeStreamResponse.DataChange.Type.USER)
                        .setRowKey(entry.toProto().getRowKey())
                        .setSourceClusterId("cluster-a")
                        .setCommitTimestamp(committedAt)
                        .setEstimatedLowWatermark(committedAt)
                        .setToken("token")
                        .setDone(true);
        // Preserve the serializer's order and payload in a synthetic, complete RPC response.
        // Feed the actual pinned SDK merger so its omitted-range conversion is tested too.
        for (Mutation mutation : entry.toProto().getMutationsList()) {
            assertThat(mutation.getMutationCase())
                    .isIn(Mutation.MutationCase.DELETE_FROM_COLUMN, Mutation.MutationCase.SET_CELL);
            if (mutation.hasDeleteFromColumn()) {
                assertThat(mutation.getDeleteFromColumn().hasTimeRange()).isFalse();
            }
            dataChange.addChunks(
                    ReadChangeStreamResponse.MutationChunk.newBuilder().setMutation(mutation));
        }
        var merger =
                new ChangeStreamRecordMerger<ChangeStreamRecord>(
                        new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder());
        merger.push(ReadChangeStreamResponse.newBuilder().setDataChange(dataChange).build());
        assertThat(merger.hasFullFrame()).isTrue();
        ChangeStreamRecord record = merger.pop();
        assertThat(merger.hasPartialFrame()).isFalse();
        return TestBigtableChangeStreamMutations.convert((ChangeStreamMutation) record);
    }
}
