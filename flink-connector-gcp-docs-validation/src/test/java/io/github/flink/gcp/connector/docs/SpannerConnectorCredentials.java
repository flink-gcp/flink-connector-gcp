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

import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.OrderEventSerializer;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;

final class SpannerConnectorCredentials {

    private SpannerConnectorCredentials() {}

    static void build() {
        SpannerMutationSerializationSchema<OrderEvent> orderSerializer = new OrderEventSerializer();

        // tag::spanner-connector-credentials[]
        SpannerSink.<OrderEvent>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(orderSerializer)
                .serviceAccountKeyFile("/var/run/secrets/spanner/key.json")
                .build();
        // end::spanner-connector-credentials[]
    }
}
