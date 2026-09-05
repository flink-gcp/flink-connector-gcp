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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Value;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins the complete row-entry contract through serialization, submission and solo rejection. */
@Timeout(30)
class BigtableAdvancedMutationTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "table");
    private static final ByteString QUALIFIER = ByteString.copyFrom(new byte[] {0, (byte) 0xff});
    private static final ByteString VALUE = ByteString.copyFromUtf8("replacement");
    private static final ByteString ACCUMULATOR =
            ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(9).array());
    private static final long TIMESTAMP = 123_000L;

    private final FakeMutationBatcherFactory factory = new FakeMutationBatcherFactory();
    private final FakeMutationBatcher batcher = factory.batcherFor(TABLE);
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final List<FailedMutation> failures = new ArrayList<>();

    @Test
    void theSdkMergeConvenienceOverloadUsesRawInputInsteadOfBytesInput() {
        // Int64 Sum rejected this encoding on 2026-09-05 (ADR-0041); pin this overload's encoding.
        Value input =
                RowMutationEntry.create("row")
                        .mergeToCell("agg", QUALIFIER, TIMESTAMP, ACCUMULATOR)
                        .toProto()
                        .getMutations(0)
                        .getMergeToCell()
                        .getInput();
        assertThat(input.getKindCase()).isEqualTo(Value.KindCase.RAW_VALUE);
        assertThat(input.getRawValue()).isEqualTo(ACCUMULATOR);
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void aSerializedSchemaBuildsTheExactMutationTypesAndOrder(Operation operation)
            throws Exception {
        BigtableSerializationSchema<String> schema = schema(operation);
        BigtableSerializationSchema<String> restored =
                InstantiationUtil.clone(schema, getClass().getClassLoader());

        assertThat(schema.serialize("row", TestContexts.NO_OP).toProto())
                .isEqualTo(expected(operation, "row"));
        assertThat(restored.serialize("row", TestContexts.NO_OP).toProto())
                .isEqualTo(expected(operation, "row"));
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void submitsTheCompleteEntryWithoutRewritingIt(Operation operation) throws Exception {
        RowMutationEntry entry = schema(operation).serialize("row", TestContexts.NO_OP);
        try (SinkWriter<String> writer = writer((element, context) -> entry)) {
            writer.write("row", TestContexts.NO_OP);

            assertThat(batcher.entries).singleElement().isSameAs(entry);
            assertThat(batcher.entries.get(0).toProto()).isEqualTo(expected(operation, "row"));
            assertThat(metrics.counterValue("numRecordsSend")).isEqualTo(1);
            assertThat(metrics.counterValue("numBytesSend"))
                    .isEqualTo(expected(operation, "row").getSerializedSize());
            writer.flush(false);
            assertThat(batcher.sentRowKeys()).containsExactly(List.of("row"));
            assertThat(failures).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void isolationAndFailurePayloadPreserveEveryMutation(Operation operation) throws Exception {
        batcher.rejectedRowKeys.add("bad");
        try (SinkWriter<String> writer = writer(schema(operation))) {
            writer.write("good", TestContexts.NO_OP);
            writer.write("bad", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(batcher.sentRowKeys())
                    .containsExactly(List.of("good", "bad"), List.of("good"), List.of("bad"));
            assertThat(batcher.entries).hasSize(4);
            assertThat(batcher.entries.get(2)).isSameAs(batcher.entries.get(0));
            assertThat(batcher.entries.get(3)).isSameAs(batcher.entries.get(1));
            assertThat(batcher.entries.get(2).toProto()).isEqualTo(expected(operation, "good"));
            assertThat(batcher.entries.get(3).toProto()).isEqualTo(expected(operation, "bad"));
            assertThat(failures).hasSize(1);
            assertThat(MutateRowsRequest.Entry.parseFrom(failures.get(0).getPayloadBytes()))
                    .isEqualTo(expected(operation, "bad"));
            assertThat(metrics.counterValue("numRecordsSend")).isEqualTo(2);
        }
    }

    private SinkWriter<String> writer(BigtableSerializationSchema<String> schema) {
        FailureHandler<FailedMutation> handler = failures::add;
        BigtableMutateRowsSink<String> sink =
                (BigtableMutateRowsSink<String>)
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(schema)
                                .failedMutationHandler(handler)
                                .build();
        return sink.createWriter(factory, new FakeTableAdmin(), new FakeMailboxExecutor(), metrics);
    }

    private static BigtableSerializationSchema<String> schema(Operation operation) {
        return (rowKey, context) -> {
            switch (operation) {
                case ADD:
                    return RowMutationEntry.create(rowKey)
                            .addToCell("agg", QUALIFIER, TIMESTAMP, -7);
                case MERGE:
                    Mutation merge =
                            Mutation.newBuilder()
                                    .setMergeToCell(
                                            Mutation.MergeToCell.newBuilder()
                                                    .setFamilyName("agg")
                                                    .setColumnQualifier(raw(QUALIFIER))
                                                    .setTimestamp(timestamp())
                                                    .setInput(
                                                            Value.newBuilder()
                                                                    .setBytesValue(ACCUMULATOR)))
                                    .build();
                    return RowMutationEntry.createFromMutationUnsafe(
                            ByteString.copyFromUtf8(rowKey),
                            com.google.cloud.bigtable.data.v2.models.Mutation.fromProtoUnsafe(
                                    List.of(merge)));
                case KEEP_LATEST:
                    return RowMutationEntry.create(rowKey)
                            .deleteCells("cf", QUALIFIER, TimestampRange.unbounded())
                            .setCell("cf", QUALIFIER, TIMESTAMP, VALUE);
                default:
                    throw new AssertionError(operation);
            }
        };
    }

    private static MutateRowsRequest.Entry expected(Operation operation, String rowKey) {
        MutateRowsRequest.Entry.Builder entry =
                MutateRowsRequest.Entry.newBuilder().setRowKey(ByteString.copyFromUtf8(rowKey));
        switch (operation) {
            case ADD:
                entry.addMutations(
                        Mutation.newBuilder()
                                .setAddToCell(
                                        Mutation.AddToCell.newBuilder()
                                                .setFamilyName("agg")
                                                .setColumnQualifier(raw(QUALIFIER))
                                                .setTimestamp(timestamp())
                                                .setInput(Value.newBuilder().setIntValue(-7))));
                break;
            case MERGE:
                entry.addMutations(
                        Mutation.newBuilder()
                                .setMergeToCell(
                                        Mutation.MergeToCell.newBuilder()
                                                .setFamilyName("agg")
                                                .setColumnQualifier(raw(QUALIFIER))
                                                .setTimestamp(timestamp())
                                                .setInput(
                                                        Value.newBuilder()
                                                                .setBytesValue(ACCUMULATOR))));
                break;
            case KEEP_LATEST:
                entry.addMutations(
                                Mutation.newBuilder()
                                        .setDeleteFromColumn(
                                                Mutation.DeleteFromColumn.newBuilder()
                                                        .setFamilyName("cf")
                                                        .setColumnQualifier(QUALIFIER)))
                        .addMutations(
                                Mutation.newBuilder()
                                        .setSetCell(
                                                Mutation.SetCell.newBuilder()
                                                        .setFamilyName("cf")
                                                        .setColumnQualifier(QUALIFIER)
                                                        .setTimestampMicros(TIMESTAMP)
                                                        .setValue(VALUE))
                                        .setTimestampOrigin(
                                                Mutation.TimestampOrigin.USER_SPECIFIED));
                break;
            default:
                throw new AssertionError(operation);
        }
        return entry.build();
    }

    private static Value raw(ByteString value) {
        return Value.newBuilder().setRawValue(value).build();
    }

    private static Value timestamp() {
        return Value.newBuilder().setRawTimestampMicros(TIMESTAMP).build();
    }

    private enum Operation {
        ADD,
        MERGE,
        KEEP_LATEST
    }
}
