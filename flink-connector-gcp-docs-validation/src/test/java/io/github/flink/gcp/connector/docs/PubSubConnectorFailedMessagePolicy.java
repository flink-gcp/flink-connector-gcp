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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

final class PubSubConnectorFailedMessagePolicy {

    private PubSubConnectorFailedMessagePolicy() {}

    static Sink<String> build() {
        // tag::pubsub-connector-failed-message-policy[]
        Sink<String> sink =
                PubSubSink.<String>builder()
                        .topic(TopicDestination.of("my-project", "events"))
                        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                        .failedMessageHandler(FailureHandler.logAndDrop())
                        .build();
        // end::pubsub-connector-failed-message-policy[]
        return sink;
    }
}
