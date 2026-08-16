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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderDeserializer;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;

final class SpannerExamplesDataBoost {

    private SpannerExamplesDataBoost() {}

    static void build(StreamExecutionEnvironment env) {
        // tag::spanner-examples-data-boost[]
        SpannerSource.<Order>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                .readOperation(
                        SpannerReadOperation.query(
                                Statement.of(
                                        "SELECT OrderId, Total FROM Orders WHERE Total > 100")))
                .deserializer(new OrderDeserializer())
                .dataBoostEnabled(true)
                // Both are hints. Asking for one partition per subtask is reasonable; getting a
                // different number is normal, and the enumerator warns when the plan is smaller
                // than the parallelism.
                .maxPartitions(env.getParallelism())
                .build();
        // end::spanner-examples-data-boost[]
    }
}
