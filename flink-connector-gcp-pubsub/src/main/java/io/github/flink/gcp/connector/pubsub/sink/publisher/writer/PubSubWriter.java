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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * At-least-once writer publishing records to dynamic per-record Pub/Sub topic destinations.
 *
 * <p>Adapted from the {@code PubSubSinkWriter} and {@code PubSubFlushablePublisher} of the Flink
 * connector in <a href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/
 * pubsub</a> (Apache-2.0), extended with per-record topic resolution, a writer-owned per-topic
 * publisher map, mailbox-based backpressure, asynchronous error capture and reactive topic
 * auto-creation.
 *
 * <h2>Threading model</h2>
 *
 * <p>All mutable state — the publisher map, the in-flight counters, the pending-retry buffers and
 * the captured asynchronous error — is touched only on the task thread. Publish completion
 * callbacks do not mutate state directly; they re-dispatch onto the {@link MailboxExecutor}, whose
 * mails run on the task thread inside {@link MailboxExecutor#yield()} calls. This is the
 * backpressure model of the Apache {@code flink-connector-gcp-pubsub} writer (a design reference;
 * no code is copied from it).
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless by design: it stores nothing in Flink state. {@link #flush(boolean)}
 * runs at every checkpoint barrier, sends all messages buffered inside the SDK publishers and
 * blocks until every in-flight publish is acknowledged — republishing messages whose topic had to
 * be created first — so a successful checkpoint means every record up to the barrier is persisted
 * by Pub/Sub; discarding operator state can never lose sink-buffered records. Terminal publish
 * failures captured by completion callbacks are rethrown on the task thread from the next {@link
 * #write} or {@link #flush}, failing the job (retries within a publish are delegated to the SDK).
 *
 * <p>The number of unacknowledged publishes is capped at {@link #DEFAULT_MAX_IN_FLIGHT_MESSAGES};
 * once the cap is reached, {@link #write} yields to the mailbox until completions bring the count
 * back down, bounding sink memory between checkpoints.
 *
 * <h2>Topic auto-creation</h2>
 *
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED} (the default), publishes failing with {@code
 * NOT_FOUND} are recovered on the task thread: the failed messages are parked per destination, the
 * topic is created through the {@link TopicAdmin} (idempotent across parallel subtasks — {@code
 * ALREADY_EXISTS} is treated as success), and the messages are republished within a bounded backoff
 * budget covering topic-metadata propagation. Failures discovered while a {@link #write} waits on
 * the in-flight cap are repaired on the next {@code write} or {@code flush}; {@code flush} repairs
 * until nothing is pending, so a completed checkpoint never leaves parked messages behind. Under
 * {@link CreateDisposition#CREATE_NEVER}, a {@code NOT_FOUND} publish fails the job immediately.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class PubSubWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubWriter.class);

    /**
     * Default cap on unacknowledged publishes per writer. Matches the default of the Apache {@code
     * flink-connector-gcp-pubsub} writer; exposing it on the builder is deferred until issue #20
     * shows which knobs matter.
     */
    static final int DEFAULT_MAX_IN_FLIGHT_MESSAGES = 1000;

    /**
     * Default backoff budget for republishing after creating a missing topic (~1 minute). Pub/Sub
     * topics are usually publishable immediately after creation; the budget covers propagation edge
     * cases.
     */
    static final RetrySchedule DEFAULT_RECOVERY_SCHEDULE = new RetrySchedule(500, 10_000, 10, 0);

    private final PubSubSinkConfig<T> config;
    private final PublisherFactory publisherFactory;
    private final TopicAdmin topicAdmin;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightMessages;
    private final RetrySchedule recoverySchedule;

    /** Lazily populated per-topic publisher state; touched only on the task thread. */
    private final Map<TopicDestination, DestinationState> states = new HashMap<>();

    /** Number of publishes not yet acknowledged; touched only on the task thread. */
    private int inFlightMessages;

    /**
     * First terminal publish failure; set and read only on the task thread (failure callbacks
     * re-dispatch through the mailbox).
     */
    private IOException asyncError;

    /**
     * Whether some destination has messages parked for republish after topic creation; set and read
     * only on the task thread.
     */
    private boolean repairNeeded;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param publisherFactory factory for per-topic publishers
     * @param topicAdmin admin used to create missing topics; closed with the writer
     * @param mailboxExecutor the task mailbox, used to run publish completions on the task thread
     */
    public PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor) {
        this(
                config,
                publisherFactory,
                topicAdmin,
                mailboxExecutor,
                DEFAULT_MAX_IN_FLIGHT_MESSAGES,
                DEFAULT_RECOVERY_SCHEDULE);
    }

    @VisibleForTesting
    public PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor,
            int maxInFlightMessages,
            RetrySchedule recoverySchedule) {
        this.config = config;
        this.publisherFactory = publisherFactory;
        this.topicAdmin = topicAdmin;
        this.mailboxExecutor = mailboxExecutor;
        this.maxInFlightMessages = maxInFlightMessages;
        this.recoverySchedule = recoverySchedule;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        if (repairNeeded) {
            repairPendingTopics();
        }
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
        DestinationState state = stateFor(destination);
        awaitInFlightBelow(maxInFlightMessages);
        publishTo(state, message);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        // Loop: the final drain can process a NOT_FOUND failure mail that parks messages for
        // repair instead of setting asyncError; returning then would complete a checkpoint with
        // unpublished messages.
        do {
            if (repairNeeded) {
                repairPendingTopics();
            }
            for (DestinationState state : states.values()) {
                state.publisher.flushOutstanding();
            }
            awaitInFlightBelow(1);
        } while (repairNeeded);
    }

    @Override
    public void close() throws Exception {
        // No explicit flush here: on success Flink calls flush(true) before close. On the failure
        // path the writer publishes no further records itself; note the SDK publisher's graceful
        // shutdown still sends messages buffered inside it, which at-least-once tolerates as
        // duplicates after the restart. Messages parked for topic-creation repair are dropped
        // with the writer: they are not covered by a completed checkpoint, so the restart
        // replays them.
        try {
            List<AutoCloseable> closeables =
                    states.values().stream()
                            .map(state -> (AutoCloseable) state.publisher)
                            .collect(Collectors.toList());
            closeables.add(topicAdmin);
            IOUtils.closeAll(closeables);
        } finally {
            states.clear();
        }
    }

    private DestinationState stateFor(TopicDestination destination) throws IOException {
        DestinationState state = states.get(destination);
        if (state == null) {
            TopicPublisher publisher;
            try {
                publisher = publisherFactory.create(destination);
            } catch (IOException | RuntimeException e) {
                throw new IOException(
                        "Failed to create a Pub/Sub publisher for topic " + destination + ".", e);
            }
            state = new DestinationState(destination, publisher);
            states.put(destination, state);
        }
        return state;
    }

    /**
     * Publishes the message to the destination's publisher, counts it in flight and registers its
     * completion callback.
     */
    private void publishTo(DestinationState state, PubsubMessage message) throws IOException {
        ApiFuture<String> future;
        try {
            future = state.publisher.publish(message);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to publish a record to Pub/Sub topic " + state.destination + ".", e);
        }
        inFlightMessages++;
        state.inFlight++;
        ApiFutures.addCallback(future, new PublishCallback(state, message), Runnable::run);
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

    /** Task-thread handler for a failed publish, run as a mailbox mail. */
    private void onPublishFailed(
            DestinationState state, PubsubMessage message, Throwable throwable) {
        inFlightMessages--;
        state.inFlight--;
        if (isRecoverableNotFound(throwable)) {
            state.pendingRetries.add(message);
            state.lastNotFound = throwable;
            repairNeeded = true;
        } else if (asyncError == null) {
            asyncError = wrapPublishFailure(state.destination, throwable);
        }
    }

    /** Repairs every destination with parked messages until none remain (or the repair fails). */
    private void repairPendingTopics() throws IOException, InterruptedException {
        while (repairNeeded) {
            repairNeeded = false;
            for (DestinationState state : states.values()) {
                if (!state.pendingRetries.isEmpty()) {
                    repairDestination(state);
                }
            }
        }
    }

    /**
     * Creates the destination's topic and republishes its parked messages, retrying within the
     * recovery schedule while topic metadata propagates. Failures during a retry re-enter the
     * pending buffer through the normal callback path; non-{@code NOT_FOUND} failures abort the
     * repair through {@link #checkAsyncError()}.
     */
    private void repairDestination(DestinationState state)
            throws IOException, InterruptedException {
        LOG.info(
                "A publish to Pub/Sub topic {} failed because the topic does not exist;"
                        + " creating it (CREATE_IF_NEEDED).",
                state.destination);
        topicAdmin.createTopic(state.destination);
        for (int attempt = 1; ; attempt++) {
            List<PubsubMessage> batch = new ArrayList<>(state.pendingRetries);
            state.pendingRetries.clear();
            for (PubsubMessage message : batch) {
                publishTo(state, message);
            }
            state.publisher.flushOutstanding();
            while (state.inFlight > 0) {
                checkAsyncError();
                mailboxExecutor.yield();
            }
            checkAsyncError();
            if (state.pendingRetries.isEmpty()) {
                return;
            }
            if (attempt >= recoverySchedule.maxAttempts()) {
                throw new IOException(
                        "Republishing to Pub/Sub topic "
                                + state.destination
                                + " kept failing with NOT_FOUND after creating the topic ("
                                + attempt
                                + " attempt(s)).",
                        state.lastNotFound);
            }
            long backoffMs = recoverySchedule.backoffMs(attempt);
            LOG.info(
                    "Republishing to Pub/Sub topic {} still fails with NOT_FOUND; backing off"
                            + " {} ms (attempt {}/{}).",
                    state.destination,
                    backoffMs,
                    attempt,
                    recoverySchedule.maxAttempts());
            Thread.sleep(backoffMs);
        }
    }

    private boolean isRecoverableNotFound(Throwable throwable) {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED
                && isNotFound(throwable);
    }

    private IOException wrapPublishFailure(TopicDestination destination, Throwable throwable) {
        String message = "A publish to Pub/Sub topic " + destination + " failed";
        if (isNotFound(throwable)) {
            message += " because the topic does not exist and createDisposition is CREATE_NEVER";
        }
        return new IOException(message + ".", throwable);
    }

    /**
     * Whether the failure's cause chain carries a {@code NOT_FOUND} status — either as the gax
     * {@link ApiException} the SDK publisher surfaces or as a raw gRPC {@link
     * StatusRuntimeException} (defense in depth).
     */
    private static boolean isNotFound(Throwable throwable) {
        return ExceptionUtils.findThrowable(throwable, PubSubWriter::isNotFoundException)
                .isPresent();
    }

    private static boolean isNotFoundException(Throwable throwable) {
        if (throwable instanceof ApiException) {
            return ((ApiException) throwable).getStatusCode().getCode()
                    == StatusCode.Code.NOT_FOUND;
        }
        if (throwable instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) throwable).getStatus().getCode()
                    == Status.Code.NOT_FOUND;
        }
        return false;
    }

    @VisibleForTesting
    int getInFlightMessages() {
        return inFlightMessages;
    }

    /** Per-topic publisher plus the destination's repair and completion state. */
    private final class DestinationState {

        private final TopicDestination destination;
        private final TopicPublisher publisher;

        /** Messages whose publish failed with a recoverable NOT_FOUND, awaiting republish. */
        private final List<PubsubMessage> pendingRetries = new ArrayList<>();

        /** Retained as the cause of a budget-exhaustion failure. */
        private Throwable lastNotFound;

        /** This destination's unacknowledged publishes; touched only on the task thread. */
        private int inFlight;

        /**
         * Success mail shared by every publish to this destination, so the success path enqueues no
         * per-record mail allocation (the per-publish callback itself is the only one).
         */
        private final ThrowingRunnable<Exception> completionMail =
                () -> {
                    inFlightMessages--;
                    inFlight--;
                };

        private final String completionDescription;
        private final String failureDescription;

        private DestinationState(TopicDestination destination, TopicPublisher publisher) {
            this.destination = destination;
            this.publisher = publisher;
            this.completionDescription = "Complete a Pub/Sub publish to " + destination;
            this.failureDescription = "Fail a Pub/Sub publish to " + destination;
        }
    }

    /**
     * Re-dispatches publish completions onto the mailbox so state stays task-thread-only.
     *
     * <p>One instance per publish: the callback carries its message so a failed publish can be
     * republished after topic auto-creation (the destination's success mail is still shared).
     */
    private final class PublishCallback implements ApiFutureCallback<String> {

        private final DestinationState state;
        private final PubsubMessage message;

        private PublishCallback(DestinationState state, PubsubMessage message) {
            this.state = state;
            this.message = message;
        }

        @Override
        public void onSuccess(String messageId) {
            mailboxExecutor.execute(state.completionMail, state.completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> onPublishFailed(state, message, throwable), state.failureDescription);
        }
    }
}
