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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEvent;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEventProtoSerializer;

final class BigQueryConnectorFileLoads {

    private BigQueryConnectorFileLoads() {}

    static void build(StreamExecutionEnvironment env) {
        // tag::bigquery-connector-file-loads[]
        Sink<MyEvent> sink =
                BigQuerySink.<MyEvent>builder()
                        .writeMethod(WriteMethod.FILE_LOADS)
                        .destinationResolver(
                                (e, ctx) ->
                                        TableDestination.of(
                                                "my-project", "my_dataset", e.tableName()))
                        .serializer(new MyEventProtoSerializer())
                        .fileLoadsOptions(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://my-staging-bucket/flink-loads")
                                        .build())
                        .build();

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // or: env.setRuntimeMode(RuntimeExecutionMode.STREAMING) with checkpointing enabled.
        // end::bigquery-connector-file-loads[]
    }
}
