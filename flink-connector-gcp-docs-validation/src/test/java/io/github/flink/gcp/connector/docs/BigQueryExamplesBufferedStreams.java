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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.BigQueryExamplesTablePerDay.DailyTableResolver;

final class BigQueryExamplesBufferedStreams {

    private BigQueryExamplesBufferedStreams() {}

    static void build(
            Source<OrderEvent, ?, ?> source,
            BigQueryProtoSerializationSchema<OrderEvent> serializer)
            throws Exception {
        // tag::bigquery-examples-buffered-streams[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // The mode must be explicit: AUTOMATIC is rejected at graph construction, because resolving
        // to
        // streaming without checkpointing would leave buffered rows invisible forever.
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(60_000);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders")
                .sinkTo(
                        BigQuerySink.<OrderEvent>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .destinationResolver(
                                        new DailyTableResolver(
                                                "my-project", "my_dataset", "orders"))
                                .serializer(serializer)
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .build());

        env.execute("bigquery-exactly-once");
        // end::bigquery-examples-buffered-streams[]
    }
}
