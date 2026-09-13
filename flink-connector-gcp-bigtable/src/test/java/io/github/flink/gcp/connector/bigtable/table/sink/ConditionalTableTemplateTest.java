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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequests;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.ROW_TYPE;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.TABLE;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.cell;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.config;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.options;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.row;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.wire;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionalTableTemplateTest {
    @Test
    void retainsAllSixOperationsBothBranchesAndTypedValuesOnTheWire() throws Exception {
        Map<String, String> options = options();
        cell(options, "then", 0, "set-cell");
        options.put("sink.conditional.then.0.value-column", "delta");
        options.put("sink.conditional.then.0.timestamp-micros", "-1");
        cell(options, "then", 1, "delete-cells");
        options.put("sink.conditional.then.1.start-timestamp-column", "ts");
        options.put("sink.conditional.then.1.end-timestamp-column", "end");
        options.put("sink.conditional.then.2.operation", "delete-family");
        options.put("sink.conditional.then.2.family", "cf");
        options.put("sink.conditional.then.3.operation", "delete-row");
        cell(options, "then", 4, "add-to-cell");
        options.put("sink.conditional.then.4.value-column", "delta");
        options.put("sink.conditional.then.4.timestamp-micros", "0");
        cell(options, "then", 5, "merge-to-cell");
        options.put("sink.conditional.then.5.value-column", "value");
        options.put("sink.conditional.then.5.timestamp-column", "ts");
        cell(options, "otherwise", 0, "delete-cells");

        CheckAndMutateRowRequest request = wire(options, row());
        assertThat(request.hasPredicateFilter()).isFalse();
        assertThat(request.getTrueMutationsList())
                .extracting(Mutation::getMutationCase)
                .containsExactly(
                        Mutation.MutationCase.SET_CELL, Mutation.MutationCase.DELETE_FROM_COLUMN,
                        Mutation.MutationCase.DELETE_FROM_FAMILY,
                                Mutation.MutationCase.DELETE_FROM_ROW,
                        Mutation.MutationCase.ADD_TO_CELL, Mutation.MutationCase.MERGE_TO_CELL);
        assertThat(request.getTrueMutations(0).getSetCell().getTimestampMicros()).isEqualTo(-1);
        assertThat(request.getTrueMutations(0).getSetCell().getValue())
                .isEqualTo(ByteString.copyFrom(new byte[] {-1, -1, -1, -1, -1, -1, -1, -5}));
        assertThat(
                        request.getTrueMutations(1)
                                .getDeleteFromColumn()
                                .getTimeRange()
                                .getStartTimestampMicros())
                .isEqualTo(1000);
        assertThat(
                        request.getTrueMutations(1)
                                .getDeleteFromColumn()
                                .getTimeRange()
                                .getEndTimestampMicros())
                .isEqualTo(2000);
        assertThat(request.getTrueMutations(4).getAddToCell().getInput().getKindCase())
                .isEqualTo(Value.KindCase.INT_VALUE);
        assertThat(request.getTrueMutations(4).getAddToCell().getInput().getIntValue())
                .isEqualTo(-5);
        assertThat(
                        request.getTrueMutations(4)
                                .getAddToCell()
                                .getTimestamp()
                                .getRawTimestampMicros())
                .isZero();
        assertThat(request.getTrueMutations(5).getMergeToCell().getInput().getKindCase())
                .isEqualTo(Value.KindCase.BYTES_VALUE);
        assertThat(request.getTrueMutations(5).getMergeToCell().getInput().getBytesValue())
                .isEqualTo(ByteString.copyFrom(row().getBinary(2)));
        assertThat(request.getFalseMutations(0).getDeleteFromColumn().hasTimeRange()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"cell-exists", "latest-cell-value-equals"})
    void fixesBinaryPredicateSelectionAndComparisonOrder(String predicate) throws Exception {
        Map<String, String> options = options();
        options.put("sink.conditional.predicate", predicate);
        options.put("sink.conditional.predicate.family", "cf");
        options.put("sink.conditional.predicate.qualifier-base64", "AAr/Lg==");
        if (predicate.equals("latest-cell-value-equals")) {
            options.put("sink.conditional.predicate.value-column", "expected");
        }
        var chain = wire(options, row()).getPredicateFilter().getChain().getFiltersList();
        assertThat(chain).hasSize(predicate.equals("cell-exists") ? 2 : 4);
        assertThat(chain.get(0).getFamilyNameRegexFilter()).isEqualTo("cf");
        assertThat(chain.get(1).getColumnQualifierRegexFilter())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, 92, 10, (byte) 255, 92, '.'}));
        if (predicate.equals("latest-cell-value-equals")) {
            assertThat(chain.get(2).getCellsPerColumnLimitFilter()).isEqualTo(1);
            assertThat(chain.get(3).getValueRegexFilter())
                    .isEqualTo(ByteString.copyFrom(new byte[] {0, 92, 10, (byte) 255}));
        }
    }

    @Test
    void preservesNumericOrderBeyondNineAndCopiesFixedDefinitions() throws Exception {
        Map<String, String> options = options();
        for (int i = 11; i >= 0; i--) {
            cell(options, "then", i, "set-cell");
            options.put("sink.conditional.then." + i + ".qualifier", Integer.toString(i));
            options.put("sink.conditional.then." + i + ".value-utf8", "");
        }
        SingleRowRequestConfig<org.apache.flink.table.data.RowData> config = config(options);
        options.clear();
        ConditionalCommandTestSupport.FakeClients client =
                new ConditionalCommandTestSupport.FakeClients();
        config.getSerializer().serialize(row(), TestContexts.NO_OP).start(client, TABLE);
        assertThat(client.sent.get(0).getTrueMutationsList())
                .extracting(m -> m.getSetCell().getColumnQualifier().toStringUtf8())
                .containsExactlyElementsOf(
                        IntStream.range(0, 12)
                                .mapToObj(Integer::toString)
                                .collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void crossesTheJobGraphWithoutSerializedLambdasAndRestoresItsClock() throws Exception {
        Map<String, String> options = options();
        cell(options, "then", 0, "set-cell");
        options.put("sink.conditional.then.0.value-column", "value");
        RowDataConditionalCommandSerializationSchema schema =
                new RowDataConditionalCommandSerializationSchema(
                        ConditionalTableTemplate.compile(ROW_TYPE, options));
        byte[] serialized = InstantiationUtil.serializeObject(schema);
        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        RowDataConditionalCommandSerializationSchema copy =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        assertThat(copy.serialize(row(), null).getThenMutations()).hasSize(1);
        var config = InstantiationUtil.clone(config(options), getClass().getClassLoader());
        ConditionalCommandTestSupport.FakeClients client =
                new ConditionalCommandTestSupport.FakeClients();
        config.getSerializer().serialize(row(), TestContexts.NO_OP).start(client, TABLE);
        assertThat(client.sent.get(0).getTrueMutations(0).getSetCell().getTimestampMicros() % 1000)
                .isZero();
    }

    @Test
    void readsTheWriterClockPerCellAndPreservesExplicitMicroseconds() throws Exception {
        Map<String, String> options = options();
        for (int i = 0; i < 3; i++) {
            cell(options, "then", i, "set-cell");
            options.put("sink.conditional.then." + i + ".value-utf8", "");
        }
        options.put("sink.conditional.then.2.timestamp-micros", "1234");
        CountingClock clock = new CountingClock();
        RowDataConditionalCommandSerializationSchema schema =
                new RowDataConditionalCommandSerializationSchema(
                        ConditionalTableTemplate.compile(ROW_TYPE, options), clock);
        ConditionalCommandTestSupport.FakeClients client =
                new ConditionalCommandTestSupport.FakeClients();
        ConditionalRequests.adapt(schema.serialize(row(), null)).start(client, TABLE);
        assertThat(clock.reads).isEqualTo(2);
        assertThat(client.sent.get(0).getTrueMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsExactly(1000L, 2000L, 1234L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"then", "otherwise"})
    void validatesNullBindingsAndEncodingFailuresInBothBranches(String branch) throws Exception {
        Map<String, String> options = options();
        cell(options, branch, 0, "set-cell");
        options.put("sink.conditional." + branch + ".0.value-column", "value");
        var serializer = config(options).getSerializer();
        GenericRowData input = row();
        input.setField(2, null);
        assertThatThrownBy(() -> serializer.serialize(input, TestContexts.NO_OP))
                .hasMessageContaining("sink.conditional." + branch + ".0.value-column")
                .hasMessageContaining("NULL input column");
        input.setField(2, StringData.fromString("wrong internal representation"));
        assertThatThrownBy(() -> serializer.serialize(input, TestContexts.NO_OP))
                .hasMessageContaining("could not encode its input column");
    }

    @Test
    void rejectsNullOrEmptyKeysAndEveryNonInsertRowKind() throws Exception {
        var serializer = config(options()).getSerializer();
        GenericRowData input = row();
        input.setField(0, null);
        assertThatThrownBy(() -> serializer.serialize(input, null))
                .hasMessageContaining("NULL input column");
        input.setField(0, StringData.fromString(""));
        assertThatThrownBy(() -> serializer.serialize(input, null))
                .hasMessageContaining("empty row key");
        input.setField(0, StringData.fromString("key"));
        for (RowKind kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
            input.setRowKind(kind);
            assertThatThrownBy(() -> serializer.serialize(input, null))
                    .hasMessageContaining("INSERT-only");
        }
    }

    @Test
    void rejectsDynamicRangesNullTimestampsAndNegativeAggregateBuckets() throws Exception {
        Map<String, String> options = options();
        cell(options, "otherwise", 0, "delete-cells");
        options.put("sink.conditional.otherwise.0.start-timestamp-column", "ts");
        options.put("sink.conditional.otherwise.0.end-timestamp-column", "end");
        var serializer = config(options).getSerializer();
        GenericRowData input = row();
        input.setField(5, 1000L);
        assertThatThrownBy(() -> serializer.serialize(input, null))
                .hasMessageContaining("end timestamp greater than start");
        input.setField(5, 0L);
        assertThatThrownBy(() -> serializer.serialize(input, null))
                .hasMessageContaining("must be at least 1");
        input.setField(5, null);
        assertThatThrownBy(() -> serializer.serialize(input, null))
                .hasMessageContaining("NULL input column");
        options = options();
        cell(options, "then", 0, "add-to-cell");
        options.put("sink.conditional.then.0.value-column", "delta");
        options.put("sink.conditional.then.0.timestamp-column", "ts");
        var aggregate = config(options).getSerializer();
        input.setField(4, -1L);
        assertThatThrownBy(() -> aggregate.serialize(input, null))
                .hasMessageContaining("must be at least 0");
    }

    @Test
    void allowsEmptyBytesAndAllLiteralRepresentations() throws Exception {
        Map<String, String> options = options();
        cell(options, "then", 0, "set-cell");
        options.put("sink.conditional.then.0.qualifier", "");
        options.put("sink.conditional.then.0.value-base64", "");
        assertThat(wire(options, row()).getTrueMutations(0).getSetCell().getValue()).isEmpty();
        assertThat(wire(options, row()).getTrueMutations(0).getSetCell().getColumnQualifier())
                .isEmpty();
        options.remove("sink.conditional.then.0.value-base64");
        options.put("sink.conditional.then.0.value-int64", "-5");
        assertThat(wire(options, row()).getTrueMutations(0).getSetCell().getValue().size())
                .isEqualTo(8);
        options.put("sink.conditional.then.0.operation", "merge-to-cell");
        options.put("sink.conditional.then.0.timestamp-micros", "0");
        options.remove("sink.conditional.then.0.value-int64");
        options.put("sink.conditional.then.0.value-utf8", "");
        assertThat(
                        wire(options, row())
                                .getTrueMutations(0)
                                .getMergeToCell()
                                .getInput()
                                .getKindCase())
                .isEqualTo(Value.KindCase.BYTES_VALUE);
    }

    @Test
    void holdsTheServiceBranchCountBoundary() {
        Map<String, String> branch = new HashMap<>();
        for (int i = 0; i < 100_000; i++) {
            branch.put(i + ".operation", "delete-row");
        }
        assertThat(ConditionalSettings.branch("sink.conditional.then", branch)).hasSize(100_000);
        branch.put("100000.operation", "delete-row");
        assertThatThrownBy(() -> ConditionalSettings.branch("sink.conditional.then", branch))
                .hasMessageContaining("permits at most 100000 mutations");
    }

    private static final class CountingClock implements RowDataSerializationSchema.CellClock {
        private static final long serialVersionUID = 1L;
        private int reads;

        @Override
        public long micros() {
            return ++reads * 1000L;
        }
    }
}
