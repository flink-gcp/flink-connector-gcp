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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderChange;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderChangeDeserializer;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;

final class SpannerConnectorChangeStreamSource {

    private SpannerConnectorChangeStreamSource() {}

    static void build(StreamExecutionEnvironment env) {
        // tag::spanner-connector-change-stream-source[]
        SpannerChangeStreamSource<OrderChange> source =
                SpannerChangeStreamSource.<OrderChange>builder()
                        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                        .changeStreamName("order_changes")
                        .deserializer(new OrderChangeDeserializer())
                        .serviceAccountKeyFile("/var/run/secrets/spanner/key.json")
                        .startPosition(StartPosition.latest())
                        .maxConcurrentQueriesPerSubtask(8)
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "spanner-order-changes")
                .setParallelism(4);
        // end::spanner-connector-change-stream-source[]
    }
}
