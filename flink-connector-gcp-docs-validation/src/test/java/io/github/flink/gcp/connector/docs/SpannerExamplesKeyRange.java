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

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderDeserializer;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;

import java.util.Arrays;

final class SpannerExamplesKeyRange {

    private SpannerExamplesKeyRange() {}

    static void build() {
        // tag::spanner-examples-key-range[]
        SpannerSource.<Order>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .readOperation(
                        SpannerReadOperation.read(
                                "Orders",
                                KeySet.range(
                                        KeyRange.closedOpen(
                                                Key.of("order#1000"), Key.of("order#2000"))),
                                Arrays.asList("OrderId", "Total")))
                .deserializer(new OrderDeserializer())
                .build();
        // end::spanner-examples-key-range[]
    }
}
