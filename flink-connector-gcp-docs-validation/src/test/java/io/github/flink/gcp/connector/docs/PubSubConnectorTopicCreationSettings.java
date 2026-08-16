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

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

import java.time.Duration;
import java.util.List;

final class PubSubConnectorTopicCreationSettings {

    private PubSubConnectorTopicCreationSettings() {}

    static void build() {
        // tag::pubsub-connector-topic-creation-settings[]
        PubSubSink.<String>builder()
                .topic(TopicDestination.of("my-project", "orders-topic"))
                .topicCreateOptions(
                        TopicCreateOptions.builder()
                                .messageRetention(Duration.ofDays(7))
                                .kmsKeyName("projects/p/locations/l/keyRings/r/cryptoKeys/k")
                                .allowedPersistenceRegions(List.of("europe-west1", "europe-west4"))
                                .enforceInTransit(true)
                                .build())
                .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .build();
        // end::pubsub-connector-topic-creation-settings[]
    }
}
