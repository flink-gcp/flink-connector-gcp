/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.storageapi;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Graph-construction tests for the buffered-stream exactly-once topology: the
 * execution-mode/checkpointing validation matrix fires when the pipeline is translated, and valid
 * setups produce a committer at the sink's parallelism with no global exchange and no stamping
 * stage (the pre-commit topology is identity).
 */
class BigQueryBufferedStreamSinkTopologyTest {

    /** A trivial serializable test serializer. */
    private static class TestSerializer extends BigQueryProtoSerializer<Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(Object element) {
            return Empty.getDefaultInstance().toByteString();
        }
    }

    private static Sink<String> sink() {
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                .destination(TableDestination.of("p", "d", "t"))
                .serializer(new TestSerializer())
                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                .build();
    }

    private static void assertTopology(StreamGraph graph) {
        // A committer node exists, and — unlike FILE_LOADS — nothing routes committables through
        // a global exchange: per-stream flushes are independent, the committer runs at the
        // sink's parallelism.
        assertThat(graph.getStreamNodes())
                .anySatisfy(node -> assertThat(node.getOperatorName()).contains("Committer"));
        assertThat(graph.getStreamNodes())
                .allSatisfy(
                        node ->
                                assertThat(node.getInEdges())
                                        .allSatisfy(
                                                edge ->
                                                        assertThat(edge.getPartitioner())
                                                                .isNotInstanceOf(
                                                                        GlobalPartitioner.class)));
        assertThat(
                        graph.getStreamNodes().stream()
                                .anyMatch(
                                        node ->
                                                node.getOperatorName()
                                                        .contains("Stamp checkpoint ids")))
                .isFalse();
    }

    @Test
    void streamingWithExactlyOnceCheckpointingBuildsTheTopology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.fromData("a", "b").sinkTo(sink());

        assertTopology(env.getStreamGraph());
    }

    @Test
    void batchExecutionBuildsTheTopology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);
        env.fromData("a", "b").sinkTo(sink());

        assertTopology(env.getStreamGraph());
    }

    @Test
    void automaticExecutionModeIsRejectedAtGraphConstruction() {
        // AUTOMATIC could resolve to streaming with checkpointing disabled — undetectable here —
        // so an explicit mode is required.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RuntimeExecutionMode.BATCH")
                .hasMessageContaining("STREAMING");
    }

    @Test
    void streamingWithoutCheckpointingIsRejectedAtGraphConstruction() {
        // Checkpoint commits are the flush trigger; without them buffered rows stay invisible.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires checkpointing");
    }

    @Test
    void atLeastOnceCheckpointingIsRejected() {
        // AT_LEAST_ONCE alignment lets post-barrier records land below the barrier's flush
        // offset, and sources replay them after a failure — duplicates.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("EXACTLY_ONCE");
    }

    @Test
    void disabledCheckpointsAfterTasksFinishIsRejected() {
        // Without a checkpoint after the tasks finish, the final batch of a bounded streaming
        // job would buffer but never flush.
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("checkpoints-after-tasks-finish");
    }
}
