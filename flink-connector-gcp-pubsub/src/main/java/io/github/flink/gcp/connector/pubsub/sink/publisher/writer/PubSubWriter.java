/*
 * Copyright 2023 Google LLC
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.IOUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * At-least-once writer publishing records to dynamic per-record Pub/Sub topic destinations.
 *
 * <p>Adapted from the {@code PubSubSinkWriter} and {@code PubSubFlushablePublisher} of the Flink
 * connector in <a href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/
 * pubsub</a> (Apache-2.0), extended with per-record topic resolution, a writer-owned per-topic
 * publisher map, mailbox-based backpressure and asynchronous error capture.
 *
 * <h2>Threading model</h2>
 *
 * <p>All mutable state — the publisher map, the in-flight counter and the captured asynchronous
 * error — is touched only on the task thread. Publish completion callbacks do not mutate state
 * directly; they re-dispatch onto the {@link MailboxExecutor}, whose mails run on the task thread
 * inside {@link MailboxExecutor#yield()} calls. This is the backpressure model of the Apache {@code
 * flink-connector-gcp-pubsub} writer (a design reference; no code is copied from it).
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless by design: it stores nothing in Flink state. {@link #flush(boolean)}
 * runs at every checkpoint barrier, sends all messages buffered inside the SDK publishers and
 * blocks until every in-flight publish is acknowledged, so a successful checkpoint means every
 * record up to the barrier is persisted by Pub/Sub — discarding operator state can never lose
 * sink-buffered records. Failed publishes captured by completion callbacks are rethrown on the task
 * thread from the next {@link #write} or {@link #flush}, failing the job (retries within a publish
 * are delegated to the SDK).
 *
 * <p>The number of unacknowledged publishes is capped at {@link #DEFAULT_MAX_IN_FLIGHT_MESSAGES};
 * once the cap is reached, {@link #write} yields to the mailbox until completions bring the count
 * back down, bounding sink memory between checkpoints.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class PubSubWriter<T> implements SinkWriter<T> {

    /**
     * Default cap on unacknowledged publishes per writer. Matches the default of the Apache {@code
     * flink-connector-gcp-pubsub} writer; exposing it on the builder is deferred until issue #20
     * shows which knobs matter.
     */
    static final int DEFAULT_MAX_IN_FLIGHT_MESSAGES = 1000;

    private final PubSubSinkConfig<T> config;
    private final PublisherFactory publisherFactory;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightMessages;

    /** Lazily populated per-topic publishers; touched only on the task thread. */
    private final Map<TopicDestination, TopicPublisher> publishers = new HashMap<>();

    /** Number of publishes not yet acknowledged; touched only on the task thread. */
    private int inFlightMessages;

    /**
     * First terminal publish failure; set and read only on the task thread (failure callbacks
     * re-dispatch through the mailbox).
     */
    private IOException asyncError;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param publisherFactory factory for per-topic publishers
     * @param mailboxExecutor the task mailbox, used to run publish completions on the task thread
     */
    public PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            MailboxExecutor mailboxExecutor) {
        this(config, publisherFactory, mailboxExecutor, DEFAULT_MAX_IN_FLIGHT_MESSAGES);
    }

    @VisibleForTesting
    PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            MailboxExecutor mailboxExecutor,
            int maxInFlightMessages) {
        this.config = config;
        this.publisherFactory = publisherFactory;
        this.mailboxExecutor = mailboxExecutor;
        this.maxInFlightMessages = maxInFlightMessages;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        TopicDestination destination = config.getDestinationResolver().resolve(element, context);
        if (destination == null) {
            throw new IOException("The destination resolver returned null for a record.");
        }
        PubsubMessage message;
        try {
            message = config.getSerializer().serialize(element);
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "Failed to serialize a record for Pub/Sub topic " + destination + ".", e);
        }
        TopicPublisher publisher = publisherFor(destination);
        awaitInFlightBelow(maxInFlightMessages);
        ApiFuture<String> future = publisher.publish(message);
        inFlightMessages++;
        ApiFutures.addCallback(future, new PublishCallback(destination), Runnable::run);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        for (TopicPublisher publisher : publishers.values()) {
            publisher.flushOutstanding();
        }
        awaitInFlightBelow(1);
    }

    @Override
    public void close() throws Exception {
        // No implicit flush: on success Flink calls flush(true) before close, and on the failure
        // path close must not publish further messages.
        try {
            IOUtils.closeAll(publishers.values());
        } finally {
            publishers.clear();
        }
    }

    private TopicPublisher publisherFor(TopicDestination destination) throws IOException {
        TopicPublisher publisher = publishers.get(destination);
        if (publisher == null) {
            try {
                publisher = publisherFactory.create(destination);
            } catch (IOException | RuntimeException e) {
                throw new IOException(
                        "Failed to create a Pub/Sub publisher for topic " + destination + ".", e);
            }
            publishers.put(destination, publisher);
        }
        return publisher;
    }

    /**
     * Runs mailbox mails (publish completions) until fewer than {@code limit} publishes are in
     * flight, surfacing any captured publish failure — including one processed by the final mail —
     * before the caller publishes further messages. With {@code limit == 1} this drains the writer
     * completely.
     */
    private void awaitInFlightBelow(int limit) throws IOException, InterruptedException {
        while (inFlightMessages >= limit) {
            checkAsyncError();
            mailboxExecutor.yield();
        }
        checkAsyncError();
    }

    private void checkAsyncError() throws IOException {
        if (asyncError != null) {
            throw asyncError;
        }
    }

    @VisibleForTesting
    int getInFlightMessages() {
        return inFlightMessages;
    }

    /** Re-dispatches publish completions onto the mailbox so state stays task-thread-only. */
    private final class PublishCallback implements ApiFutureCallback<String> {

        private final TopicDestination destination;

        private PublishCallback(TopicDestination destination) {
            this.destination = destination;
        }

        @Override
        public void onSuccess(String messageId) {
            mailboxExecutor.execute(
                    () -> inFlightMessages--, "Complete a Pub/Sub publish to %s", destination);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> {
                        inFlightMessages--;
                        if (asyncError == null) {
                            asyncError =
                                    new IOException(
                                            "A publish to Pub/Sub topic "
                                                    + destination
                                                    + " failed.",
                                            throwable);
                        }
                    },
                    "Fail a Pub/Sub publish to %s",
                    destination);
        }
    }
}
