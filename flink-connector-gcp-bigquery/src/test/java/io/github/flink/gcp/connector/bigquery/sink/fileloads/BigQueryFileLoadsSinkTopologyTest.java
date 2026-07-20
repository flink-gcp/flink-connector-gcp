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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

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
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Graph-construction tests for the FILE_LOADS topology: the execution-mode/checkpointing/interval
 * validation matrix fires when the pipeline is translated, and valid setups produce the
 * parallelism-1 gather stage feeding a parallelism-1 committer.
 */
class BigQueryFileLoadsSinkTopologyTest {

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
        return sink(FileLoadsOptions.builder().stagingPath("gs://bucket").build());
    }

    private static Sink<String> sink(FileLoadsOptions options) {
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .destination(TableDestination.of("p", "d", "t"))
                .serializer(new TestSerializer())
                .fileLoadsOptions(options)
                .build();
    }

    private static void assertTopology(StreamGraph graph, boolean streaming) {
        // The committer inherits the sink's parallelism; the pre-commit topology's trailing
        // global exchange must route every committable to its subtask 0 — the single-loader
        // invariant the whole per-table load design depends on.
        assertThat(graph.getStreamNodes())
                .anySatisfy(
                        node -> {
                            assertThat(node.getOperatorName()).contains("Committer");
                            assertThat(node.getInEdges())
                                    .anySatisfy(
                                            edge ->
                                                    assertThat(edge.getPartitioner())
                                                            .isInstanceOf(GlobalPartitioner.class));
                        });
        // Only streaming has a checkpoint-id stamping stage.
        assertThat(
                        graph.getStreamNodes().stream()
                                .anyMatch(
                                        node ->
                                                node.getOperatorName()
                                                        .contains("Stamp checkpoint ids")))
                .isEqualTo(streaming);
    }

    @Test
    void batchExecutionBuildsTheCommitterTopology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);
        env.fromData("a", "b").sinkTo(sink());

        assertTopology(env.getStreamGraph(), false);
    }

    @Test
    void streamingWithCheckpointingBuildsTheCommitterTopology() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofMinutes(5).toMillis());
        env.fromData("a", "b").sinkTo(sink());

        assertTopology(env.getStreamGraph(), true);
    }

    @Test
    void streamingWithoutCheckpointingIsRejectedAtGraphConstruction() {
        // Checkpoints are the load trigger; without them staged files would never be loaded.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires checkpointing");
    }

    @Test
    void streamingRejectsNonAppendWriteDisposition() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofMinutes(5).toMillis());
        env.fromData("a", "b")
                .sinkTo(
                        sink(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket")
                                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                                        .build()));

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("WRITE_APPEND");
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
    void checkpointIntervalBelowMinimumIsRejected() {
        // 30 s would mean 2,880 load jobs per table per day — above BigQuery's 1,500 limit.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(30_000);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("checkpoint interval")
                .hasMessageContaining("minCheckpointInterval");
    }

    @Test
    void atLeastOnceCheckpointingIsRejected() {
        // AT_LEAST_ONCE alignment lets post-barrier records land in the barrier's files, which
        // sources replay after a failure — duplicate loads under an exactly-once contract.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofMinutes(5).toMillis());
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("EXACTLY_ONCE");
    }

    @Test
    void disabledCheckpointsAfterTasksFinishIsRejected() {
        // Without a checkpoint after the tasks finish, the final batch of a bounded streaming
        // job would stage but never load.
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(Duration.ofMinutes(5).toMillis());
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("checkpoints-after-tasks-finish");
    }

    @Test
    void shortCheckpointIntervalIsAllowedWithExplicitOverride() {
        // Short-lived jobs whose daily load count stays safe can opt in to fast checkpoints.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(30_000);
        env.fromData("a", "b")
                .sinkTo(
                        sink(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket")
                                        .minCheckpointInterval(Duration.ofSeconds(10))
                                        .build()));

        assertTopology(env.getStreamGraph(), true);
    }
}
