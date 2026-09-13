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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableStagedTableRuntimeTest {
    @Test
    void tableAggregateRuntimeValidatesTypedFamiliesAndCommitsTheDelta() throws Exception {
        try (var service = new StagedRpcTestService()) {
            service.families =
                    Map.of("flink_commit", ColumnFamily.getDefaultInstance(), "agg", sumFamily());
            var sink = sink(service);
            try (var writer =
                            new OneInputStreamOperatorTestHarness<
                                    RowData, CommittableMessage<BigtableCommittable>>(
                                    new SinkWriterOperatorFactory<>(sink), 128, 1, 0);
                    var committer =
                            new OneInputStreamOperatorTestHarness<
                                    CommittableMessage<BigtableCommittable>,
                                    CommittableMessage<BigtableCommittable>>(
                                    new CommitterOperatorFactory<>(sink, false, true), 128, 1, 0)) {
                writer.setup(
                        CommittableMessageTypeInfo.of(sink::getCommittableSerializer)
                                .createSerializer(new SerializerConfigImpl()));
                writer.open();
                committer.open();
                assertThat(service.metadata).contains("projects/p/instances/i/tables/t");
                writer.processElement(
                        new StreamRecord<>(
                                GenericRowData.of(
                                        StringData.fromString("r"), GenericRowData.of(5L))));
                writer.getOperator().prepareSnapshotPreBarrier(1);
                BigtableStagedCommitLifecycleTest.forward(writer, committer);
                committer.snapshot(1, 1);
                assertThat(service.probe.sent).isEmpty();
                committer.notifyOfCompletedCheckpoint(1);
                synchronized (service.probe) {
                    assertThat(service.probe.sum("projects/p/instances/i/tables/t", "r"))
                            .isEqualTo(5);
                    assertThat(service.probe.applied).isEqualTo(1);
                }
            }
        }
    }

    @Test
    void tableWriterRejectsWrongAggregateMetadataAndMissingMarkerBeforeAnyWrite() throws Exception {
        try (var service = new StagedRpcTestService()) {
            var sink = sink(service);
            service.families =
                    Map.of(
                            "flink_commit",
                            ColumnFamily.getDefaultInstance(),
                            "agg",
                            ColumnFamily.getDefaultInstance());
            assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0)))
                    .isInstanceOf(IOException.class)
                    .hasStackTraceContaining("expected int64-sum");
            service.families = Map.of("agg", sumFamily());
            assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("marker");
            assertThat(service.probe.sent).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"upsert", "keep-latest"})
    void nonAggregateTableAcceptsTypedDataButStillRequiresDeclaredFamilies(String mode)
            throws Exception {
        try (var service = new StagedRpcTestService()) {
            service.families =
                    Map.of(
                            "flink_commit",
                            ColumnFamily.getDefaultInstance(),
                            "agg",
                            ColumnFamily.newBuilder()
                                    .setValueType(
                                            Type.newBuilder()
                                                    .setStringType(
                                                            Type.String.getDefaultInstance()))
                                    .build());
            var sink = sink(service, mode);
            try (var writer = sink.createWriter(new StubWriterInitContext(0))) {
                writer.write(
                        GenericRowData.of(
                                StringData.fromString("r"),
                                GenericRowData.of(StringData.fromString("value"))),
                        null);
                assertThat(writer.prepareCommit()).hasSize(1);
            }
            service.families =
                    Map.of("flink_commit", ColumnFamily.getDefaultInstance(), "agg", sumFamily());
            assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("cannot use aggregate data family agg");
            service.families = Map.of("flink_commit", ColumnFamily.getDefaultInstance());
            assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("missing declared data family agg");
            assertThat(service.probe.sent).isEmpty();
        }
    }

    private static BigtableStagedSink<RowData> sink(StagedRpcTestService service) {
        return sink(service, "aggregate");
    }

    @SuppressWarnings("unchecked")
    private static BigtableStagedSink<RowData> sink(StagedRpcTestService service, String mode) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigtable");
        options.put("project", "p");
        options.put("instance", "i");
        options.put("table", "t");
        options.put("emulator-endpoint", "127.0.0.1:" + service.server.getPort());
        options.put("sink.app-profile-id", "transactional");
        options.put("sink.write-mode", mode);
        if (mode.equals("aggregate")) {
            options.put("sink.aggregate.column-family-types", "agg:int64-sum");
        }
        options.put("sink.delivery-guarantee", "exactly-once");
        options.put("sink.staged.marker-family", "flink_commit");
        var schema =
                ResolvedSchema.of(
                        Column.physical("key", DataTypes.STRING()),
                        Column.physical(
                                "agg",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "count",
                                                mode.equals("aggregate")
                                                        ? DataTypes.BIGINT()
                                                        : DataTypes.STRING()))));
        var table = FactoryMocks.createTableSink(schema, options);
        return (BigtableStagedSink<RowData>)
                ((SinkV2Provider)
                                table.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                        .createSink();
    }

    static ColumnFamily sumFamily() {
        return ColumnFamily.newBuilder()
                .setValueType(
                        Type.newBuilder()
                                .setAggregateType(
                                        Type.Aggregate.newBuilder()
                                                .setInputType(
                                                        Type.newBuilder()
                                                                .setInt64Type(
                                                                        Type.Int64.newBuilder()
                                                                                .setEncoding(
                                                                                        Type.Int64
                                                                                                .Encoding
                                                                                                .newBuilder()
                                                                                                .setBigEndianBytes(
                                                                                                        Type
                                                                                                                .Int64
                                                                                                                .Encoding
                                                                                                                .BigEndianBytes
                                                                                                                .getDefaultInstance()))))
                                                .setSum(Type.Aggregate.Sum.getDefaultInstance())))
                .build();
    }
}
