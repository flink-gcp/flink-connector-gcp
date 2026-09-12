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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableStagedSinkTest {
    @Test
    void writerFreezesOnceAndSkipsNullWithoutRetainingAMarker() throws Exception {
        List<String> calls = new ArrayList<>();
        RowMutationEntry mutable = RowMutationEntry.create("r").setCell("data", "q", 12000, "old");
        var sink =
                (BigtableStagedSink<String>)
                        builder()
                                .destinationResolver(
                                        (value, context) -> {
                                            calls.add("resolve:" + value);
                                            return TableDestination.of("p", "i", value);
                                        })
                                .serializer(
                                        new BigtableSerializationSchema<String>() {
                                            @Override
                                            public void open(
                                                    SerializationSchema.InitializationContext
                                                            context) {
                                                calls.add("open");
                                            }

                                            @Override
                                            public RowMutationEntry serialize(
                                                    String value, SinkWriter.Context context) {
                                                calls.add("serialize:" + value);
                                                return value.equals("skip") ? null : mutable;
                                            }
                                        })
                                .build();
        try (var writer = sink.createWriter(new StubWriterInitContext(0))) {
            writer.write("first", null);
            mutable.setCell("data", "q", 13000, "new");
            writer.write("skip", null);
            var envelope = writer.prepareCommit().iterator().next();
            assertThat(envelope.getRequest().getTableName())
                    .isEqualTo("projects/p/instances/i/tables/first");
            assertThat(envelope.getRequest().getFalseMutationsCount()).isEqualTo(2);
            assertThat(envelope.getRequest().getFalseMutations(0).getSetCell().getTimestampMicros())
                    .isEqualTo(12000);
            assertThat(calls)
                    .containsExactly(
                            "open",
                            "resolve:first",
                            "serialize:first",
                            "resolve:skip",
                            "serialize:skip");
            var serializer = sink.getCommittableSerializer();
            assertThat(serializer.deserialize(1, serializer.serialize(envelope)).getRequest())
                    .isEqualTo(envelope.getRequest());
            assertThat(calls).hasSize(5);
            assertThat(writer.prepareCommit()).isEmpty();
        }
    }

    @Test
    void restoreRejectsAParsedButUnsafeEnvelope() throws Exception {
        var sink = (BigtableStagedSink<String>) builder().build();
        BigtableCommittable original;
        try (var writer = sink.createWriter(new StubWriterInitContext(0))) {
            writer.write("r", null);
            original = writer.prepareCommit().iterator().next();
        }
        var serializer = sink.getCommittableSerializer();
        var request = original.getRequest();
        var marker = request.getFalseMutations(1);
        for (var invalid :
                List.of(
                        request.toBuilder().clearFalseMutations().build(),
                        request.toBuilder().setAppProfileId("other/profile").build(),
                        request.toBuilder()
                                .setTableName("projects/p/instances/i/tables/t/extra")
                                .build(),
                        request.toBuilder().setRowKey(ByteString.EMPTY).build(),
                        request.toBuilder().addTrueMutations(marker).build(),
                        request.toBuilder().clearPredicateFilter().build(),
                        request.toBuilder()
                                .setFalseMutations(
                                        0,
                                        Mutation.newBuilder()
                                                .setDeleteFromRow(
                                                        Mutation.DeleteFromRow
                                                                .getDefaultInstance()))
                                .build(),
                        request.toBuilder().setFalseMutations(0, marker).build(),
                        request.toBuilder()
                                .setFalseMutations(
                                        1,
                                        marker.toBuilder()
                                                .setSetCell(
                                                        marker.getSetCell().toBuilder()
                                                                .setTimestampMicros(1000)))
                                .build())) {
            assertThatThrownBy(() -> serializer.deserialize(1, invalid.toByteArray()))
                    .isInstanceOf(IOException.class);
        }
        assertThatThrownBy(() -> serializer.deserialize(0, request.toByteArray()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(1, new byte[] {(byte) 255}))
                .isInstanceOf(IOException.class);
    }

    @Test
    void incompatibleBuilderCombinationsFailBeforeRuntime() {
        assertThatThrownBy(() -> builder().appProfileId("a/b").build())
                .hasMessageContaining("appProfileId");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .createDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                        .tableCreateOptions(
                                                TableCreateOptions.builder()
                                                        .columnFamily("data")
                                                        .build())
                                        .build())
                .hasMessageContaining("CREATE_NEVER");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .writerOptions(BigtableWriterOptions.builder().build())
                                        .build())
                .hasMessageContaining("writerOptions");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .deliveryGuarantee(BigtableDeliveryGuarantee.AT_LEAST_ONCE)
                                        .build())
                .hasMessageContaining("stagedOptions");
        assertThatThrownBy(
                        () ->
                                BigtableSink.<String>builder()
                                        .table(TableDestination.of("p", "i", "t"))
                                        .serializer(
                                                (value, context) -> RowMutationEntry.create(value))
                                        .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                                        .stagedOptions(
                                                BigtableStagedOptions.builder()
                                                        .markerFamily("markers")
                                                        .build())
                                        .build())
                .hasMessageContaining("appProfileId");
    }

    @Test
    void graphRequiresStreamingExactlyOnceCheckpointsAndFinishedTaskCheckpoints() throws Exception {
        for (RuntimeExecutionMode mode : RuntimeExecutionMode.values()) {
            try (var env = StreamExecutionEnvironment.getExecutionEnvironment()) {
                env.setRuntimeMode(mode);
                env.enableCheckpointing(1000);
                env.fromData("row").sinkTo(builder().build());
                if (mode == RuntimeExecutionMode.STREAMING) {
                    assertThat(env.getStreamGraph().getStreamNodes()).isNotEmpty();
                } else {
                    assertThatThrownBy(env::getStreamGraph)
                            .hasStackTraceContaining("execution.runtime-mode");
                }
            }
        }
        for (String invalid : List.of("disabled", "at-least-once", "finished")) {
            Configuration config = new Configuration();
            config.set(
                    CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                    !invalid.equals("finished"));
            try (var env = StreamExecutionEnvironment.getExecutionEnvironment(config)) {
                env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
                if (!invalid.equals("disabled")) {
                    env.enableCheckpointing(1000);
                }
                if (invalid.equals("at-least-once")) {
                    env.getCheckpointConfig()
                            .setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
                }
                env.fromData("row").sinkTo(builder().build());
                assertThatThrownBy(env::getStreamGraph)
                        .hasStackTraceContaining("Bigtable EXACTLY_ONCE requires");
            }
        }
    }

    private static BigtableSinkBuilder<String> builder() {
        return BigtableSink.<String>builder()
                .table(TableDestination.of("p", "i", "t"))
                .serializer(
                        (value, context) ->
                                RowMutationEntry.create(value).setCell("data", "q", 1000, value))
                .appProfileId("profile")
                .emulatorEndpoint("localhost:1")
                .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                .stagedOptions(BigtableStagedOptions.builder().markerFamily("markers").build());
    }
}
