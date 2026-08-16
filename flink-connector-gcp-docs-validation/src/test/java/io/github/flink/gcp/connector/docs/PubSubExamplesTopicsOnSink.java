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

import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.OrderEventSchema;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

import java.time.Duration;

final class PubSubExamplesTopicsOnSink {

    private PubSubExamplesTopicsOnSink() {}

    static void build() {
        // tag::pubsub-examples-topics-on-sink[]
        PubSubSink.<OrderEvent>builder()
                .topic(TopicDestination.of("my-project", "orders"))
                .topicCreateOptions(
                        TopicCreateOptions.builder()
                                // What makes messages published before a subscription exists
                                // reachable by one created later, or by a backwards seek.
                                .messageRetention(Duration.ofDays(7))
                                .build())
                .serializer(PubSubSerializationSchema.dataOnly(new OrderEventSchema()))
                .build();
        // end::pubsub-examples-topics-on-sink[]
    }
}
