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

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;

final class SpannerReadmeOverview {

    private SpannerReadmeOverview() {}

    static Sink<OrderEvent> build() {
        // tag::spanner-readme-overview[]
        Sink<OrderEvent> sink =
                SpannerSink.<OrderEvent>builder()
                        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
                        .serializer(
                                (event, context) ->
                                        Mutation.newInsertOrUpdateBuilder("Orders")
                                                .set("OrderId")
                                                .to(event.id())
                                                .set("Total")
                                                .to(event.total())
                                                .build())
                        .build();
        // end::spanner-readme-overview[]
        return sink;
    }

    private static final class OrderEvent {

        String id() {
            return "order-1";
        }

        long total() {
            return 1L;
        }
    }
}
