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
import org.apache.flink.api.connector.source.Source;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.MyEvent;
import io.github.flink.gcp.connector.docs.PubSubDocumentationTypes.MyEventSerializationSchema;
import io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

final class JavadocPubSubExamples {

    private JavadocPubSubExamples() {}

    static Sink<MyEvent> sink() {
        // tag::sink[]
        Sink<MyEvent> sink =
                PubSubSink.<MyEvent>builder()
                        .destinationResolver(
                                (e, ctx) -> TopicDestination.of("my-project", e.topicName()))
                        .serializer(
                                PubSubSerializationSchema.dataOnly(
                                        new MyEventSerializationSchema()))
                        .build();
        // end::sink[]
        return sink;
    }

    static Source<String, ?, ?> source() {
        // tag::source[]
        Source<String, ?, ?> source =
                PubSubSource.<String>builder()
                        .subscription(SubscriptionDestination.of("my-project", "my-subscription"))
                        .deserializationSchema(
                                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                        .build();
        // end::source[]
        return source;
    }

    static Sink<Order> deadLetterQueue(BigQueryProtoSerializer<Order> serializer) {
        Sink<Order> sink =
                // tag::dead-letter-queue[]
                BigQuerySink.<Order>builder()
                        .destination(TableDestination.of("my-project", "my_dataset", "orders"))
                        .serializer(serializer)
                        .failureHandler(
                                FailureHandler.sendToDeadLetterQueue(
                                        PubSubDeadLetterQueue.builder()
                                                .topic(
                                                        TopicDestination.of(
                                                                "my-project", "dead-letters"))
                                                .build()))
                        .build();
        // end::dead-letter-queue[]
        return sink;
    }

    private static final class Order {}
}
