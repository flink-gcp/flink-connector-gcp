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

import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.MyEvent;
import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.MyEventSerializationSchema;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

import java.time.Duration;
import java.util.Map;

final class PubSubConnectorOverview {

    private PubSubConnectorOverview() {}

    static Sink<MyEvent> build() {
        // tag::pubsub-connector-overview[]
        Sink<MyEvent> sink =
                PubSubSink.<MyEvent>builder()
                        .destinationResolver(
                                (e, ctx) -> TopicDestination.of("my-project", e.topicName()))
                        .serializer(
                                PubSubSerializationSchema.dataOnly(new MyEventSerializationSchema())
                                        .withAttributes(e -> Map.of("source", e.source()))
                                        .withOrderingKey(MyEvent::deviceId))
                        .publisherOptions(
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .batchDelayThreshold(Duration.ofMillis(10))
                                        .build())
                        .build();
        // end::pubsub-connector-overview[]
        return sink;
    }
}
