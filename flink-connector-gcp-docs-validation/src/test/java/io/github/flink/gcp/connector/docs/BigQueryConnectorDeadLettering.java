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

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyOrderProtoSerializer;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.Order;
import io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

final class BigQueryConnectorDeadLettering {

    private BigQueryConnectorDeadLettering() {}

    static void build() {
        // tag::bigquery-connector-dead-lettering[]
        BigQuerySink.<Order>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "orders"))
                .serializer(new MyOrderProtoSerializer())
                .failureHandler(
                        FailureHandler.sendToDeadLetterQueue(
                                PubSubDeadLetterQueue.builder()
                                        .topic(TopicDestination.of("my-project", "dead-letters"))
                                        .build()))
                .build();
        // end::bigquery-connector-dead-lettering[]
    }
}
