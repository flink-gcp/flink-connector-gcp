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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.FailedMessage;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
 * <h2>Per-message failures</h2>
 *
 * <p>Failures {@link PubSubErrorClassifier} calls {@code MESSAGE_LEVEL}, plus records the
 * serializer rejects, are handed to the configured {@link FailureHandler} instead of failing the
 * job outright: they concern one message and republishing the same bytes cannot succeed. The
 * handler drops the message by returning and fails the job by throwing; the default {@code
 * failJob()} throws, which is why the classes it does <em>not</em> cover matter — an outage the SDK
 * gave up on stays a job failure, so no drop policy can quietly discard a backlog. A handler
 * failing inside a completion callback is captured into {@link #asyncError} like any other terminal
 * failure, because a mailbox mail cannot throw a checked exception at its caller.
 *
 * <p>Unacknowledged publishes are capped along both dimensions that bound memory: their number
 * ({@code PubSubPublisherOptions.maxInFlightMessages}, default 1000) and their serialized size
 * ({@code PubSubPublisherOptions.maxInFlightBytes}, default 64 MiB). At either cap {@link #write}
 * yields to the mailbox until completions bring the counters back down. The byte cap exists because
 * the message count alone bounds nothing: Pub/Sub allows 10 MiB per message, and the SDK
 * publisher's flow controller — the only byte bound before — is unusable with message ordering
 * enabled (see {@code PubSubPublisherOptions}).
 *
 * <p>Two documented ways the byte cap is exceeded, both bounded:
 *
 * <ul>
 *   <li>Admission is checked before a publish rather than against the message's own size, so a
 *       message larger than the cap is admitted on an empty writer and overshoots it until it
 *       completes. This is deliberate: a "does it fit" predicate would never admit such a message,
 *       and since {@link MailboxExecutor#yield()} blocks until a mail arrives and no mail can
 *       arrive with nothing in flight, that would be a task hang rather than backpressure.
 *   <li>A topic-creation repair republishes its parked batch without re-checking either cap, so
 *       both counters can transiently exceed it by the batch size. Parked messages were themselves
 *       admitted under the caps, and {@link #repairPendingTopics} drains to empty before
 *       republishing, so the peak is one destination's parked batch.
 * </ul>
 *
 * <p>Messages parked for a repair are released from both counters by their failure mail, so under a
 * {@code NOT_FOUND} storm the writer can hold roughly a cap's worth of parked payload alongside a
 * cap's worth newly admitted: peak retention is ~2× the configured cap, not 1×. It stays bounded
 * because {@link #write} repairs before admitting the next record, so parked messages cannot
 * accumulate across writes.
 *
 * <h2>Topic auto-creation</h2>
 *
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED} (the default), publishes failing with {@code
 * NOT_FOUND} are recovered on the task thread: the failed messages are parked per destination, the
 * topic is created through the {@link TopicAdmin} (idempotent across parallel subtasks — {@code
 * ALREADY_EXISTS} is treated as success), and the messages are republished within a bounded backoff
 * budget covering topic-metadata propagation. Failures discovered while a {@link #write} waits on
 * an in-flight cap are repaired on the next {@code write} or {@code flush}; {@code flush} repairs
 * until nothing is pending, so a completed checkpoint never leaves parked messages behind. Under
 * {@link CreateDisposition#CREATE_NEVER}, a {@code NOT_FOUND} publish fails the job immediately.
 *
 * <h2>Message ordering</h2>
 *
 * <p>Messages may carry ordering keys only when {@code
 * PubSubPublisherOptions.enableMessageOrdering} is set; {@link #write} rejects a keyed message
 * otherwise (the SDK would reject it with a less actionable error). After a failed publish the SDK
 * publisher pauses the message's ordering key and cancels the key's queued publishes with a {@link
 * java.util.concurrent.CancellationException}; under {@code CREATE_IF_NEEDED} those cascade
 * cancellations are parked alongside the {@code NOT_FOUND} that caused them, and the repair resumes
 * each key before republishing the batch. Cross-key and cross-topic ordering are unaffected.
 *
 * <p>Per-key order is preserved by <b>sorting the parked batch on publish sequence</b>, not by the
 * order the failures are observed in: the SDK cancels queued publishes from its own thread, so a
 * cascade's failure mail can reach the mailbox before its root's. Anything deriving the batch from
 * mail order — including deciding whether to park a cascade by whether something is parked already
 * — is a race (see #78).
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class PubSubWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubWriter.class);

    private final PubSubSinkConfig<T> config;
    private final PublisherFactory publisherFactory;
    private final TopicAdmin topicAdmin;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightMessages;
    private final long maxInFlightBytes;
    private final RetrySchedule recoverySchedule;
    private final boolean orderingEnabled;
    private final FailureHandler<? super FailedMessage> failedMessageHandler;

    /** Lazily populated per-topic publisher state; touched only on the task thread. */
    private final Map<TopicDestination, DestinationState> states = new HashMap<>();

    /** Number of publishes not yet acknowledged; touched only on the task thread. */
    private int inFlightMessages;

    /**
     * Serialized size of the publishes not yet acknowledged; touched only on the task thread.
     * Excludes messages parked for a topic-creation repair — their failure mail released them
     * before parking, and the repair republishes those same objects.
     */
    private long inFlightBytes;

    /**
     * Issue order of publishes, which is what a parked batch is sorted by. Assigned in {@link
     * #publishTo} on the task thread, so it needs no synchronization.
     */
    private long nextPublishSequence;

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
                config.getPublisherOptions().toRecoverySchedule());
    }

    /**
     * Creates the writer with an explicit auto-creation recovery schedule, so tests need not sit
     * through the production backoff. Every other knob is read from the config's publisher options
     * — the caps especially, so there is exactly one path by which they reach the writer.
     */
    @VisibleForTesting
    PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor,
            RetrySchedule recoverySchedule) {
        this.config = config;
        this.publisherFactory = publisherFactory;
        this.topicAdmin = topicAdmin;
        this.mailboxExecutor = mailboxExecutor;
        PubSubPublisherOptions options = config.getPublisherOptions();
        // Checked here, not only on the options builder: a non-positive cap holds the
        // awaitCapacity predicate with nothing in flight, and yield() blocks until a mail arrives
        // — so it is a silent permanent park, not a rejected configuration. Fail where the
        // invariant is relied on rather than trusting that every options instance came from the
        // builder, which Java deserialization does not run.
        Preconditions.checkArgument(
                options.getMaxInFlightMessages() > 0, "maxInFlightMessages must be positive");
        Preconditions.checkArgument(
                options.getMaxInFlightBytes() > 0, "maxInFlightBytes must be positive");
        this.maxInFlightMessages = options.getMaxInFlightMessages();
        this.maxInFlightBytes = options.getMaxInFlightBytes();
        this.recoverySchedule = recoverySchedule;
        this.orderingEnabled = options.isEnableMessageOrdering();
        this.failedMessageHandler = config.getFailedMessageHandler();
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
            // The record never became a message, so there is nothing to carry but the destination:
            // FailedMessage.getPayloadBytes() is null, as the shared contract prescribes.
            failedMessageHandler.handle(
                    FailedMessage.of(
                            destination,
                            null,
                            "Failed to serialize a record for Pub/Sub topic " + destination + ".",
                            e));
            return;
        }
        if (!orderingEnabled && !message.getOrderingKey().isEmpty()) {
            throw new IOException(
                    "The serializer produced a message with ordering key '"
                            + message.getOrderingKey()
                            + "' for Pub/Sub topic "
                            + destination
                            + " but message ordering is not enabled; set"
                            + " PubSubPublisherOptions.builder().enableMessageOrdering(true) on"
                            + " the sink.");
        }
        DestinationState state = stateFor(destination);
        awaitCapacity();
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
            drainInFlight();
        } while (repairNeeded);
        // After the drain, never before it: the failures that reach the handler are discovered by
        // the drain, so flushing first would checkpoint past dead letters the drain is about to
        // produce.
        failedMessageHandler.flush();
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
            List<AutoCloseable> closeables = new ArrayList<>(states.size() + 2);
            for (DestinationState state : states.values()) {
                closeables.add(state.publisher);
            }
            closeables.add(topicAdmin);
            // Through closeAll, so the handler is closed even when a publisher's shutdown throws:
            // the lifecycle contract promises close on the failure path too.
            closeables.add(failedMessageHandler::close);
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
        // Memoized by protobuf, so recomputing it in the callback would be equivalent; taking it
        // once keeps a single source of the number the counter is adjusted by.
        int serializedSize = message.getSerializedSize();
        ApiFuture<String> future;
        try {
            future = state.publisher.publish(message);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to publish a record to Pub/Sub topic " + state.destination + ".", e);
        }
        // Counted only once the publish is accepted: a synchronous throw registers no callback, so
        // nothing would ever release it.
        inFlightMessages++;
        inFlightBytes += serializedSize;
        ApiFutures.addCallback(
                future,
                new PublishCallback(state, message, nextPublishSequence++, serializedSize),
                Runnable::run);
    }

    /** Releases one completed publish from both in-flight counters. */
    private void releaseInFlight(int serializedSize) {
        inFlightMessages--;
        inFlightBytes -= serializedSize;
    }

    /**
     * Admission gate for {@link #write}: runs mailbox mails (publish completions) until both
     * in-flight caps have room, surfacing any captured publish failure before the caller publishes.
     *
     * <p>Both predicates are "at or above the cap", never "would this message fit", so an empty
     * writer always admits. That matters beyond overshoot accounting: {@link
     * MailboxExecutor#yield()} blocks until a mail arrives, and with nothing in flight no mail can
     * arrive, so any predicate that can hold at zero is a task hang rather than backpressure. The
     * positive-value preconditions on both options are what rule that out.
     */
    private void awaitCapacity() throws IOException, InterruptedException {
        while (inFlightMessages >= maxInFlightMessages || inFlightBytes >= maxInFlightBytes) {
            checkAsyncError();
            mailboxExecutor.yield();
        }
        checkAsyncError();
    }

    /**
     * Runs mailbox mails until <b>no</b> publish is in flight, surfacing any captured publish
     * failure — including one processed by the final mail — before the caller proceeds.
     *
     * <p>This is a correctness primitive, not backpressure, and reaching exactly zero is what two
     * guarantees rest on (#78, #110): a fatal root failure reaches {@code asyncError} and is
     * rethrown here before any cascade of it can be republished, and a parked batch is never
     * snapshotted while a cascade of it is still in flight. It must stay independent of the
     * in-flight caps — no byte or count limit may weaken it into a low-water mark.
     *
     * <p>Keyed on the message count alone: a {@code PubsubMessage} can serialize to zero bytes, so
     * {@code inFlightBytes == 0} does not imply an empty writer.
     */
    private void drainInFlight() throws IOException, InterruptedException {
        while (inFlightMessages > 0) {
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
            DestinationState state,
            PubsubMessage message,
            long sequence,
            int serializedSize,
            Throwable throwable) {
        releaseInFlight(serializedSize);
        PubSubErrorClassifier.Kind kind = PubSubErrorClassifier.classify(throwable);
        if (kind == PubSubErrorClassifier.Kind.TOPIC_NOT_FOUND && repairsTopics()) {
            state.pendingRetries.put(sequence, message);
            state.repairCause = throwable;
            repairNeeded = true;
        } else if (kind == PubSubErrorClassifier.Kind.CANCELLATION
                && repairsTopics()
                && orderingEnabled) {
            // With ordering enabled the SDK cancels an ordering key's queued publishes after the
            // key's first failure, so a cancellation is never a root cause — it always trails a
            // failure of an earlier publish this writer issued for the same key. Park it for the
            // repair without asking whether the root was recoverable, because that is not knowable
            // here: failure mails do not arrive in publish order (the SDK cancels queued work from
            // its own thread, so a cascade's mail can be enqueued before its root's). It does not
            // need to be knowable — repairPendingTopics drains the writer completely before
            // repairing, and that drain surfaces a fatal root through checkAsyncError, so a
            // cascade is only ever republished once the root is known to be recoverable.
            state.pendingRetries.put(sequence, message);
            // Only as a fallback: a real NOT_FOUND is the better budget-exhaustion cause, and it
            // wins whether it is observed before or after its cascades.
            if (state.repairCause == null) {
                state.repairCause = throwable;
            }
            repairNeeded = true;
        } else if (kind == PubSubErrorClassifier.Kind.MESSAGE_LEVEL) {
            routeFailedMessage(state.destination, message, throwable);
        } else if (asyncError == null) {
            asyncError = wrapPublishFailure(state.destination, kind, throwable);
        }
    }

    /**
     * Hands a message-level publish failure to the configured handler. Runs as a mailbox mail, so a
     * handler that fails the job cannot throw at a caller: its failure is captured into {@link
     * #asyncError} and rethrown from the next {@link #write} or {@link #flush}, exactly as a
     * terminal publish failure is. First failure wins, as everywhere else here.
     */
    private void routeFailedMessage(
            TopicDestination destination, PubsubMessage message, Throwable throwable) {
        try {
            failedMessageHandler.handle(
                    FailedMessage.of(
                            destination,
                            message,
                            "A publish to Pub/Sub topic "
                                    + destination
                                    + " was rejected: the message is invalid"
                                    + " (INVALID_ARGUMENT).",
                            throwable));
        } catch (IOException | RuntimeException e) {
            if (asyncError == null) {
                asyncError =
                        e instanceof IOException
                                ? (IOException) e
                                : new IOException(
                                        "The failed-message handler failed for Pub/Sub topic "
                                                + destination
                                                + ".",
                                        e);
            }
        }
    }

    /** Repairs every destination with parked messages until none remain (or the repair fails). */
    private void repairPendingTopics() throws IOException, InterruptedException {
        while (repairNeeded) {
            repairNeeded = false;
            // Drain before snapshotting the parked batches: a parked root's cascade-cancellation
            // mails may still be queued, or still being enqueued by the SDK thread, and a repair
            // started from a partial batch would republish only part of a key's messages. The
            // drain is also what makes parking a cascade safe in the first place — a fatal root
            // reaches asyncError here, and drainInFlight rethrows it, so no cascade of a
            // fatal root is ever republished.
            drainInFlight();
            // Iterate a snapshot: repairDestination yields to the mailbox, so hardening against
            // a mail ever reaching stateFor() keeps this loop safe from map mutation.
            for (DestinationState state : new ArrayList<>(states.values())) {
                if (!state.pendingRetries.isEmpty()) {
                    repairDestination(state);
                }
            }
        }
    }

    /**
     * Creates the destination's topic and republishes its parked messages, retrying within the
     * recovery schedule while topic metadata propagates. Each attempt drains the writer completely
     * (repair is rare, so waiting on unrelated destinations' publishes is acceptable for the
     * simplicity of reusing {@link #drainInFlight}). Failures during a retry re-enter the pending
     * buffer through the normal callback path; non-{@code NOT_FOUND} failures abort the repair from
     * within the drain.
     */
    private void repairDestination(DestinationState state)
            throws IOException, InterruptedException {
        LOG.info(
                "A publish to Pub/Sub topic {} failed because the topic does not exist;"
                        + " creating it (CREATE_IF_NEEDED).",
                state.destination);
        topicAdmin.createTopic(state.destination, config.getTopicCreateOptions());
        for (int attempt = 1; ; attempt++) {
            // Keyed by publish sequence, so the batch is in the order the messages were originally
            // published however their failure mails interleaved.
            List<PubsubMessage> batch = new ArrayList<>(state.pendingRetries.values());
            state.pendingRetries.clear();
            // Every attempt resumes the batch's ordering keys first: the failure that parked the
            // batch — and every failed republish attempt since — paused them in the publisher.
            resumeOrderingKeys(state, batch);
            for (PubsubMessage message : batch) {
                publishTo(state, message);
            }
            state.publisher.flushOutstanding();
            drainInFlight();
            if (state.pendingRetries.isEmpty()) {
                return;
            }
            if (attempt >= recoverySchedule.maxAttempts()) {
                throw new IOException(
                        "Republishing to Pub/Sub topic "
                                + state.destination
                                + " kept failing after creating the topic ("
                                + attempt
                                + " attempt(s)).",
                        state.repairCause);
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

    /** Resumes the distinct ordering keys of the batch on the destination's publisher. */
    private void resumeOrderingKeys(DestinationState state, List<PubsubMessage> batch) {
        if (!orderingEnabled) {
            return;
        }
        Set<String> orderingKeys = new LinkedHashSet<>();
        for (PubsubMessage message : batch) {
            if (!message.getOrderingKey().isEmpty()) {
                orderingKeys.add(message.getOrderingKey());
            }
        }
        for (String orderingKey : orderingKeys) {
            state.publisher.resumePublish(orderingKey);
        }
    }

    /**
     * Whether a failed publish may be parked for a topic-creation repair at all. Under {@code
     * CREATE_NEVER} nothing is ever parked, so nothing reaches {@link #repairDestination} and no
     * topic is created — a guarantee that has to be checked on every parking branch, not only the
     * {@code NOT_FOUND} one.
     */
    private boolean repairsTopics() {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED;
    }

    private IOException wrapPublishFailure(
            TopicDestination destination, PubSubErrorClassifier.Kind kind, Throwable throwable) {
        String message = "A publish to Pub/Sub topic " + destination + " failed";
        if (kind == PubSubErrorClassifier.Kind.TOPIC_NOT_FOUND) {
            message += " because the topic does not exist and createDisposition is CREATE_NEVER";
        } else if (kind == PubSubErrorClassifier.Kind.CANCELLATION && orderingEnabled) {
            message += " because an earlier publish for its ordering key failed";
        }
        return new IOException(message + ".", throwable);
    }

    @VisibleForTesting
    int getInFlightMessages() {
        return inFlightMessages;
    }

    @VisibleForTesting
    long getInFlightBytes() {
        return inFlightBytes;
    }

    /** Per-topic publisher plus the destination's repair and completion state. */
    private final class DestinationState {

        private final TopicDestination destination;
        private final TopicPublisher publisher;

        /**
         * Messages awaiting republish after topic creation, keyed by publish sequence so the batch
         * is republished in publish order. Sorting matters because the failure mails that park them
         * do not arrive in publish order, and republishing a key's messages out of order would
         * break the very guarantee the repair exists to preserve.
         */
        private final SortedMap<Long, PubsubMessage> pendingRetries = new TreeMap<>();

        /**
         * Retained as the cause of a budget-exhaustion failure: the destination's {@code
         * NOT_FOUND}, or a cascade cancellation when no {@code NOT_FOUND} was ever observed.
         */
        private Throwable repairCause;

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
     * republished after topic auto-creation, its publish sequence so the parked batch can be
     * ordered independently of the order the failures arrive in, and its serialized size so both
     * in-flight counters can be released.
     *
     * <p>It is also its own success mail, which is why the success path still allocates nothing
     * beyond this object. A mail shared per destination — what this replaces — cannot carry a size,
     * and a per-record success lambda would put an allocation back on the hot path.
     */
    private final class PublishCallback
            implements ApiFutureCallback<String>, ThrowingRunnable<Exception> {

        private final DestinationState state;
        private final PubsubMessage message;
        private final long sequence;
        private final int serializedSize;

        private PublishCallback(
                DestinationState state, PubsubMessage message, long sequence, int serializedSize) {
            this.state = state;
            this.message = message;
            this.sequence = sequence;
            this.serializedSize = serializedSize;
        }

        /** The success mail: runs on the task thread. */
        @Override
        public void run() {
            releaseInFlight(serializedSize);
        }

        @Override
        public void onSuccess(String messageId) {
            mailboxExecutor.execute(this, state.completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> onPublishFailed(state, message, sequence, serializedSize, throwable),
                    state.failureDescription);
        }
    }
}
