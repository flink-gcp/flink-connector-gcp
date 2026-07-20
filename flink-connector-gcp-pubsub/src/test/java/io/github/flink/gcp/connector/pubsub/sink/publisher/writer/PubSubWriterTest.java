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

package io.github.flink.gcp.connector.pubsub.sink.publisher.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.publisher.PubSubPublisherSink;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubWriter} against fake publishers and a fake mailbox. */
class PubSubWriterTest {

    private static final String PROJECT = "test-project";

    private static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return Long.MIN_VALUE;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    private final FakePublisherFactory factory = new FakePublisherFactory();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();

    /** Routes each record to the topic named by the record itself. */
    private PubSubWriter<String> newWriter() {
        return newWriter(PubSubWriter.DEFAULT_MAX_IN_FLIGHT_MESSAGES);
    }

    private PubSubWriter<String> newWriter(int maxInFlightMessages) {
        return newWriter(
                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()), maxInFlightMessages);
    }

    private PubSubWriter<String> newWriter(
            PubSubSerializationSchema<String> serializer, int maxInFlightMessages) {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .destinationResolver(
                                        (element, context) -> TopicDestination.of(PROJECT, element))
                                .serializer(serializer)
                                .build();
        return new PubSubWriter<>(sink.getConfig(), factory, mailbox, maxInFlightMessages);
    }

    private static TopicDestination topic(String topic) {
        return TopicDestination.of(PROJECT, topic);
    }

    @Test
    void fansOutRecordsToPerTopicPublishers() throws Exception {
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        writer.write("topic-c", CONTEXT);
        writer.write("topic-a", CONTEXT);

        assertThat(factory.publishers.keySet())
                .containsExactly(topic("topic-a"), topic("topic-b"), topic("topic-c"));
        assertThat(factory.publishers.get(topic("topic-a")).published)
                .extracting(PubsubMessage::getData)
                .extracting(data -> data.toString(StandardCharsets.UTF_8))
                .containsExactly("topic-a", "topic-a");
        assertThat(factory.publishers.get(topic("topic-b")).published).hasSize(1);
        assertThat(factory.publishers.get(topic("topic-c")).published).hasSize(1);
    }

    @Test
    void reusesOnePublisherPerTopic() throws Exception {
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);

        assertThat(factory.createCalls).isEqualTo(1);
    }

    @Test
    void flushFlushesAllPublishersAndAwaitsInFlightPublishes() throws Exception {
        PubSubWriter<String> writer = newWriter();
        SettableApiFuture<String> pending = SettableApiFuture.create();
        factory.enqueueFuture(pending);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        assertThat(writer.getInFlightMessages()).isEqualTo(2);

        CompletableFuture.runAsync(
                () -> {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    pending.set("message-id");
                });
        writer.flush(false);

        assertThat(writer.getInFlightMessages()).isZero();
        assertThat(factory.publishers.get(topic("topic-a")).flushCalls).isEqualTo(1);
        assertThat(factory.publishers.get(topic("topic-b")).flushCalls).isEqualTo(1);
    }

    @Test
    void flushSurfacesFailedPublishWithTopicContext() throws Exception {
        PubSubWriter<String> writer = newWriter();
        RuntimeException failure = new RuntimeException("publish exploded");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
    }

    @Test
    void asyncPublishFailureFailsSubsequentWrite() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new RuntimeException("publish exploded")));
        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a");
    }

    @Test
    void serializationFailureFailsWriteAndPublishesNothing() {
        IOException failure = new IOException("bad record");
        PubSubWriter<String> writer =
                newWriter(
                        element -> {
                            throw failure;
                        },
                        PubSubWriter.DEFAULT_MAX_IN_FLIGHT_MESSAGES);

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
        assertThat(factory.publishers).isEmpty();
    }

    @Test
    void writeAtInFlightCapWaitsForCompletionsBeforePublishing() throws Exception {
        PubSubWriter<String> writer = newWriter(2);
        SettableApiFuture<String> first = SettableApiFuture.create();
        SettableApiFuture<String> second = SettableApiFuture.create();
        factory.enqueueFuture(first);
        factory.enqueueFuture(second);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        assertThat(writer.getInFlightMessages()).isEqualTo(2);

        // Completing a publish enqueues a completion mail; the capped write processes it while
        // yielding and only then publishes the third record.
        first.set("message-id");
        writer.write("topic-a", CONTEXT);

        assertThat(writer.getInFlightMessages()).isEqualTo(2);
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(3);
    }

    @Test
    void writeAtInFlightCapSurfacesFailedPublishInsteadOfPublishing() throws Exception {
        PubSubWriter<String> writer = newWriter(1);
        RuntimeException failure = new RuntimeException("publish exploded");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(1);
    }

    @Test
    void completionMailsKeepInFlightCountBounded() throws Exception {
        PubSubWriter<String> writer = newWriter();

        for (int i = 0; i < 10; i++) {
            writer.write("topic-a", CONTEXT);
        }
        mailbox.drain();

        assertThat(writer.getInFlightMessages()).isZero();
    }

    @Test
    void closeClosesEveryPublisherWithoutFlushing() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);

        writer.close();

        for (FakePublisherFactory.FakeTopicPublisher publisher : factory.publishers.values()) {
            assertThat(publisher.closeCalls).isEqualTo(1);
            assertThat(publisher.flushCalls).isZero();
        }
    }

    @Test
    void closeClosesRemainingPublishersWhenOneFails() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        RuntimeException failure = new RuntimeException("shutdown exploded");
        factory.publishers.get(topic("topic-a")).closeFailure = failure;

        assertThatThrownBy(writer::close).isSameAs(failure);

        for (FakePublisherFactory.FakeTopicPublisher publisher : factory.publishers.values()) {
            assertThat(publisher.closeCalls).isEqualTo(1);
        }
    }
}
