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

import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderDeserializer;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;

final class SpannerExamplesEmulatorSource {

    private SpannerExamplesEmulatorSource() {}

    static void build() {
        // tag::spanner-examples-emulator-source[]
        SpannerSource.<Order>builder()
                .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                .readOperation(
                        SpannerReadOperation.query(Statement.of("SELECT OrderId FROM Orders")))
                .deserializer(new OrderDeserializer())
                .emulatorEndpoint("localhost:9010")
                .build();
        // end::spanner-examples-emulator-source[]
    }
}
