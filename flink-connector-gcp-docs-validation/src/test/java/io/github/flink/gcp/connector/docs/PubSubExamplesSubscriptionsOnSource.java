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
import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.OrderEventDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.time.Duration;

final class PubSubExamplesSubscriptionsOnSource {

    private PubSubExamplesSubscriptionsOnSource() {}

    static void build() {
        // tag::pubsub-examples-subscriptions-on-source[]
        PubSubSource.<OrderEvent>builder()
                .subscription(
                        SubscriptionDestination.of("my-project", "orders-sub"),
                        SubscriptionCreateOptions.builder()
                                .topic(TopicDestination.of("my-project", "orders"))
                                .ackDeadline(Duration.ofSeconds(60))
                                .build())
                // No options: this one must already exist, and the startup check says so if it
                // does not.
                .subscription(SubscriptionDestination.of("my-project", "returns-sub"))
                .deserializer(new OrderEventDeserializationSchema())
                .build();
        // end::pubsub-examples-subscriptions-on-source[]
    }
}
