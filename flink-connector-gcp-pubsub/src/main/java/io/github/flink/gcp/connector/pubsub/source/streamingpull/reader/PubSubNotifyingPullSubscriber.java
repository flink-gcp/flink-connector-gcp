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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link NotifyingPullSubscriber} backed by a {@code google-cloud-pubsub} {@link Subscriber}.
 *
 * <p>Received messages are appended to a buffer <em>synchronously inside the receiver
 * callback</em>, which is what preserves ordering-key order: for an ordering-enabled subscription
 * the client library only dispatches the next message of a key once the previous callback has
 * returned, so buffer order equals delivery order per key. It also means the callback never blocks
 * on acknowledgement, which happens a whole checkpoint later — the client library's per-key
 * serialization waits for the callback to return, not for the acknowledgement.
 *
 * <p>Backpressure comes from the client library's flow control rather than from a bounded buffer:
 * blocking inside the callback would stall the key's dispatch chain and hold a client-library
 * thread.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * with a caller-supplied data-available signal in place of per-subscriber notification futures.
 */
@Internal
public class PubSubNotifyingPullSubscriber implements NotifyingPullSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubNotifyingPullSubscriber.class);

    private final String splitId;
    private final SubscriptionDestination subscription;
    private final AckTracker ackTracker;
    private final Runnable dataAvailableSignal;
    private final Duration shutdownTimeout;
    private final Subscriber subscriber;

    @GuardedBy("this")
    private final Deque<PubsubMessage> messages = new ArrayDeque<>();

    @GuardedBy("this")
    @Nullable
    private Throwable permanentError;

    @GuardedBy("this")
    private boolean closed;

    /**
     * Creates and starts the subscriber.
     *
     * @param splitId the split this subscriber serves
     * @param subscription the subscription to consume
     * @param subscriberFactory creates the client
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     * @param dataAvailableSignal invoked when messages become available or the subscriber fails, so
     *     a blocked fetch wakes up
     * @param shutdownTimeout how long {@link #close()} waits for the client to release its messages
     * @throws IOException if the subscriber cannot be created or started
     */
    public PubSubNotifyingPullSubscriber(
            String splitId,
            SubscriptionDestination subscription,
            SubscriberFactory subscriberFactory,
            AckTracker ackTracker,
            Runnable dataAvailableSignal,
            Duration shutdownTimeout)
            throws IOException {
        this.splitId = splitId;
        this.subscription = subscription;
        this.ackTracker = ackTracker;
        this.dataAvailableSignal = dataAvailableSignal;
        this.shutdownTimeout = shutdownTimeout;
        this.subscriber = subscriberFactory.create(subscription, this::receiveMessage);
        try {
            this.subscriber.addListener(
                    new ApiService.Listener() {
                        @Override
                        public void failed(ApiService.State from, Throwable failure) {
                            fail(failure);
                        }
                    },
                    // A direct executor: recording the failure and waking the fetcher are both
                    // cheap.
                    Runnable::run);
            this.subscriber.startAsync().awaitRunning();
        } catch (RuntimeException e) {
            // The client opened its gRPC channel and background executors before failing, and the
            // SDK only releases them from its own shutdown path — so a startup failure must stop
            // the subscriber explicitly or every restart attempt of a crash-looping job leaks a
            // channel and its threads.
            stopQuietly();
            throw new IOException(
                    "Failed to start the Pub/Sub subscriber for subscription " + subscription, e);
        }
    }

    /** Receives a message from the client library, on one of its callback threads. */
    private void receiveMessage(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
        synchronized (this) {
            if (closed || permanentError != null) {
                // Nack rather than drop: the message must go back for redelivery immediately.
                ackReplyConsumer.nack();
                return;
            }
            ackTracker.addPendingAck(splitId, message.getMessageId(), ackReplyConsumer);
            messages.addLast(message);
        }
        dataAvailableSignal.run();
    }

    private void fail(Throwable failure) {
        synchronized (this) {
            if (permanentError == null) {
                permanentError = failure;
            }
        }
        dataAvailableSignal.run();
    }

    @Override
    public List<PubsubMessage> pullMessages(int maxMessages) throws IOException {
        synchronized (this) {
            if (permanentError != null) {
                throw new IOException(
                        "The Pub/Sub subscriber for subscription " + subscription + " failed.",
                        permanentError);
            }
            if (messages.isEmpty()) {
                return Collections.emptyList();
            }
            List<PubsubMessage> drained = new ArrayList<>(Math.min(maxMessages, messages.size()));
            while (drained.size() < maxMessages && !messages.isEmpty()) {
                drained.add(messages.pollFirst());
            }
            return drained;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            // Buffered messages were never emitted; nackSplit below returns them for redelivery.
            messages.clear();
        }
        ackTracker.nackSplit(splitId);
        stopQuietly();
    }

    /**
     * Stops the client, tolerating a failure. Shutdown is best-effort: everything this split owned
     * has already been nacked by the time this runs, so a client that lingers costs resources until
     * the JVM exits but loses nothing.
     */
    private void stopQuietly() {
        try {
            subscriber
                    .stopAsync()
                    .awaitTerminated(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | RuntimeException e) {
            LOG.warn(
                    "The Pub/Sub subscriber for subscription {} did not shut down cleanly.",
                    subscription,
                    e);
        }
    }
}
