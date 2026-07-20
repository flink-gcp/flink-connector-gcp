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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Graph-construction tests for the FILE_LOADS topology: the batch-only guard fires when the
 * pipeline is translated, and batch translation produces the parallelism-1 load-job operator.
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
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .destination(TableDestination.of("p", "d", "t"))
                .serializer(new TestSerializer())
                .fileLoadsOptions(FileLoadsOptions.builder().stagingPath("gs://bucket").build())
                .build();
    }

    @Test
    void streamingExecutionIsRejectedAtGraphConstruction() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("batch execution only");
    }

    @Test
    void automaticExecutionModeIsRejectedAtGraphConstruction() {
        // AUTOMATIC could resolve to streaming, where end of input — and therefore the load
        // jobs — would never come; explicit BATCH is required.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.fromData("a", "b").sinkTo(sink());

        assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RuntimeExecutionMode.BATCH");
    }

    @Test
    void batchExecutionBuildsLoadJobOperator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);
        env.fromData("a", "b").sinkTo(sink());

        StreamGraph graph = env.getStreamGraph();

        assertThat(graph.getStreamNodes())
                .anySatisfy(
                        node -> {
                            assertThat(node.getOperatorName()).contains("BigQuery load jobs");
                            assertThat(node.getParallelism()).isEqualTo(1);
                        });
    }
}
