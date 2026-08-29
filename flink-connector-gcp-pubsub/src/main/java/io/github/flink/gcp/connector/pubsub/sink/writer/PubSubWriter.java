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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.options.OptionChecks;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * At-least-once writer publishing records to dynamic per-record Pub/Sub topic destinations.
 *
 * <p>A destination is resolved per record, so the writer holds one publisher per active topic. The
 * active set is bounded and least-recently-used publishers are released only after their publishes
 * and repair debt are drained. Backpressure is mailbox-based; publish failures arrive on the client
 * library's threads and are surfaced on the task thread; and a topic that turns out not to exist is
 * repaired when a publish reports it, rather than checked for beforehand.
 *
 * <h2>Threading model</h2>
 *
 * <p>Every mutable field this class owns — the publisher map, each destination's repair debt, the
 * parked-message count and the captured asynchronous error — is <em>written</em> only on the task
 * thread. A publish completion callback re-dispatches onto the {@link MailboxExecutor}, whose mails
 * run on the task thread inside {@link MailboxExecutor#yield()} calls, rather than acting on any of
 * it from the SDK's thread; the one thing it does do there is stamp {@link InFlightTracker}'s
 * progress clock, which that class documents as its single off-thread write. Reads are not all on
 * the task thread, and that is what shapes {@code parkedMessages}: the metric reporter runs on a
 * thread of its own, so a gauge over a plain {@code int} is safe where one summing the destination
 * maps would race the task thread mutating them. The in-flight ledger is the tracker's and carries
 * the tracker's own rules. This is the backpressure model of the Apache {@code
 * flink-connector-gcp-pubsub} writer (a design reference; no code is copied from it).
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless by design: it stores nothing in Flink state. {@link #flush(boolean)}
 * runs at every checkpoint barrier, sends all messages buffered inside the SDK publishers and
 * blocks until every in-flight publish is acknowledged — republishing messages whose topic had to
 * be created first — so a successful checkpoint means every record up to the barrier is persisted
 * by Pub/Sub, other than those the serializer skipped by returning {@code null}; discarding
 * operator state can never lose sink-buffered records. Terminal publish failures captured by
 * completion callbacks are rethrown on the task thread from the next {@link #write} or {@link
 * #flush}, failing the job (retries within a publish are delegated to the SDK).
 *
 * <p>That guarantee assumes the default {@code failJob()} policy; what a successful checkpoint
 * means under a dropping policy is stated once on {@link FailureHandler}, and the per-message
 * failures below say which failures reach it.
 *
 * <h2>Per-message failures</h2>
 *
 * <p>Failures {@link PubSubErrorClassifier} calls {@code MESSAGE_LEVEL}, plus records the
 * serializer rejects, are handed to the configured {@link FailureHandler} instead of failing the
 * job outright: they concern one message and republishing the same bytes cannot succeed. A {@code
 * MESSAGE_LEVEL} report from an ordinary publish is only a <em>candidate</em> verdict, though:
 * {@code Publish} is a batch RPC that rejects all-or-nothing, and the SDK sets the one
 * request-level {@code INVALID_ARGUMENT} on every future of the batch with nothing naming the
 * offending message (measured on real Pub/Sub, 2026-08-06, one run, #264 — same {@code Throwable}
 * instance on every future, zero status details). Routing on it directly would hand a whole batch
 * to a dropping handler for one bad message, so the writer parks such a report instead and the
 * repair republishes the parked batch one message per request; only a message rejected
 * <em>solo</em> — a true per-message verdict — reaches the handler, and its co-batched neighbours
 * are published. Drop-versus-throw semantics, the never-routed backlog argument and the
 * asynchronous capture of a handler failing inside a completion callback are stated once on {@link
 * FailureHandler}; here that capture lands in {@code asyncError}. Under a dropping policy the
 * pass's one-request-per-message degradation is bounded by {@code
 * PubSubPublisherOptions.maxConsecutiveRejections} (#361): once that many confirmed rejections
 * arrive with no successfully published message between them, the stream's data is broken rather
 * than anomalous, and the writer fails the job instead of isolating it message by message. The
 * bound is a policy about the stream, accumulated across repairs in {@code consecutiveRejections};
 * the recovery budget is a different bound — it caps one repair's unproductive attempts — and the
 * two failures share no text.
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
 *   <li>A repair republishes its parked batch without re-checking either cap, so both counters can
 *       transiently exceed it by the batch size. Parked messages were themselves admitted under the
 *       caps, and {@link #repairPendingTopics} drains to empty before republishing, so the peak is
 *       one destination's parked batch.
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
 * <p>A message-level failure the handler drops is a second root a cascade can trail, and the
 * publisher pauses an ordering key for it exactly as it does for a missing topic: the SDK marks the
 * key on any non-retryable failure without inspecting it, and never resumes one by itself. So a
 * dropped keyed message hands its key to the next repair, which resumes it and republishes the
 * cascades in publish sequence. The survivors keep their relative order; the dropped message leaves
 * a gap in the key's stream that a consumer cannot distinguish from a loss, which is the price of a
 * dropping policy and is why the dead-letter record carries the whole serialized message.
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
    private final InFlightTracker inFlight;
    private final int maxConsecutiveRejections;
    private final boolean orderingEnabled;
    private final FailureHandler<? super FailedMessage> failedMessageHandler;
    private final PubSubWriterMetrics metrics;
    private final TopicRepairer repairer;
    private final int maxActivePublishers;
    private final long destinationIdleTimeoutNanos;
    private final LongSupplier nanoClock;

    /** Lazily populated publisher state in least-recently-used order; task-thread-only. */
    private final Map<TopicDestination, DestinationState> states =
            new LinkedHashMap<>(16, 0.75f, true);

    /** Mirror of {@code states.size()} that a reporter thread can read without walking the map. */
    private volatile int activePublishers;

    /**
     * Messages held for a destination's next repair, across every destination. A plain counter
     * rather than a sum over {@code states} because the gauge reading it runs on the reporter
     * thread: walking the destination maps from there would race with the task thread mutating
     * them.
     */
    private int parkedMessages;

    /**
     * Confirmed rejections routed since the last successfully published message; touched only on
     * the task thread. Every success mail zeroes it, and {@code
     * PubSubPublisherOptions.maxConsecutiveRejections} — whose javadoc carries the reasoning — is
     * what it is compared against (#361).
     */
    private int consecutiveRejections;

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
     * @param metricGroup the writer's metric group
     */
    public PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        this(
                config,
                publisherFactory,
                topicAdmin,
                mailboxExecutor,
                metricGroup,
                config.getPublisherOptions().toRecoverySchedule(),
                System::nanoTime);
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
            SinkWriterMetricGroup metricGroup,
            RetrySchedule recoverySchedule) {
        this(
                config,
                publisherFactory,
                topicAdmin,
                mailboxExecutor,
                metricGroup,
                recoverySchedule,
                System::nanoTime);
    }

    /** Creates the writer with explicit recovery timing and monotonic clock for tests. */
    @VisibleForTesting
    PubSubWriter(
            PubSubSinkConfig<T> config,
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup,
            RetrySchedule recoverySchedule,
            LongSupplier nanoClock) {
        this.config = config;
        this.publisherFactory = publisherFactory;
        this.topicAdmin = topicAdmin;
        this.mailboxExecutor = mailboxExecutor;
        this.nanoClock = nanoClock;
        PubSubPublisherOptions options = config.getPublisherOptions();
        // Re-checked for the deserialization reason InFlightTracker's own preconditions give,
        // though the failure mode is milder: a zero — which an options instance serialized before
        // the field existed carries — would fail the job on the first confirmed rejection,
        // silently overriding the handler the user configured, rather than hanging anything.
        //
        // Kept ahead of the tracker's construction, which is where it stood relative to the budget
        // check the tracker now owns. The two caps, which used to precede it, are the tracker's
        // too — so an options instance invalid in both a cap and this field now reports this one
        // where it reported the cap. Both are true of such an instance and both fail construction.
        Preconditions.checkArgument(
                options.getMaxConsecutiveRejections() > 0
                        || options.getMaxConsecutiveRejections()
                                == PubSubPublisherOptions.UNBOUNDED,
                "maxConsecutiveRejections must be positive or -1 (unbounded)");
        this.maxConsecutiveRejections = options.getMaxConsecutiveRejections();
        Preconditions.checkArgument(
                options.getMaxActivePublishers() > 0, "maxActivePublishers must be positive");
        this.maxActivePublishers = options.getMaxActivePublishers();
        OptionChecks.checkPositive(options.getDestinationIdleTimeout(), "destinationIdleTimeout");
        this.destinationIdleTimeoutNanos =
                OptionChecks.checkExpressibleInNanos(
                                options.getDestinationIdleTimeout(), "destinationIdleTimeout")
                        .toNanos();
        // Eagerly, not lazily: the tracker carries the preconditions on both caps and on the
        // progress budget, and those have to fail where the writer is built rather than at the
        // first record.
        this.inFlight =
                new InFlightTracker(
                        mailboxExecutor,
                        options,
                        LOG,
                        this::checkAsyncError,
                        this::sendWhatIsStillBatched);
        this.orderingEnabled = options.isEnableMessageOrdering();
        this.failedMessageHandler = config.getFailedMessageHandler();
        this.metrics = new PubSubWriterMetrics(metricGroup, options.isPerDestinationMetrics());
        this.metrics.bindWriterState(
                (Gauge<Integer>) this::getInFlightMessages,
                (Gauge<Long>) this::getInFlightBytes,
                (Gauge<Integer>) this::getParkedMessages,
                (Gauge<Integer>) this::getActivePublishers);
        this.repairer =
                new TopicRepairer(
                        topicAdmin,
                        config.getTopicCreateOptions(),
                        recoverySchedule,
                        metrics,
                        orderingEnabled,
                        LOG,
                        new RepairContextImpl());
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
            // FailedMessage.getPayloadBytes() is null, as the shared contract prescribes. The
            // description leaves the topic to describeDestination(), as routeFailedMessage does.
            metrics.messageFailed(metrics.forTopic(destination));
            failedMessageHandler.handle(
                    FailedMessage.of(destination, null, "The record could not be serialized.", e));
            return;
        }
        if (message == null) {
            // Skip by contract, not a failure. Ahead of stateFor(...), so a record written nowhere
            // opens no publisher. Counted, because nothing else reports it: a serializer skipping
            // every record leaves an empty topic under a green job.
            metrics.recordSkipped();
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
        inFlight.awaitCapacity();
        publishTo(state, message, true, false);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        drainPublishersAndRepair();
        // After the drain, never before it: the failures that reach the handler are discovered by
        // the drain, so flushing first would checkpoint past dead letters the drain is about to
        // produce.
        failedMessageHandler.flush();
        if (!endOfInput) {
            evictIdlePublishers();
        }
    }

    @Override
    public void close() throws Exception {
        // No explicit flush here: on success Flink calls flush(true) before close. On the failure
        // path the writer publishes no further records itself; note the SDK publisher's graceful
        // shutdown still sends messages buffered inside it, which at-least-once tolerates as
        // duplicates after the restart. Parked messages are dropped with the writer: they are
        // not covered by a completed checkpoint, so the restart replays them.
        try {
            // Every publisher is asked to shut down before any is waited on, so the waits overlap
            // and a close costs one shutdown timeout rather than one per topic — which matters
            // here because a writer with dynamic destinations owns a publisher per topic, and
            // seven sequential 30 s waits exceed Flink's task.cancellation.timeout, making a
            // cancelling task a fatal TaskManager error.
            //
            // One list rather than a loop and then a call (#297): closeAll runs every entry before
            // reporting anything, so the later shutdowns, the closes and the handler's close all
            // still run when a shutdown throws — which the lifecycle contract promises on the
            // failure path too.
            List<AutoCloseable> closeables = new ArrayList<>(states.size() * 2 + 2);
            for (DestinationState state : states.values()) {
                closeables.add(state.publisher::shutdown);
            }
            for (DestinationState state : states.values()) {
                closeables.add(state.publisher);
            }
            closeables.add(topicAdmin);
            closeables.add(failedMessageHandler::close);
            Closers.closeAll(closeables);
        } finally {
            states.clear();
            activePublishers = 0;
            // Dropped with the writer, so the gauge must not keep reporting them.
            parkedMessages = 0;
        }
    }

    private DestinationState stateFor(TopicDestination destination)
            throws IOException, InterruptedException {
        DestinationState state = states.get(destination);
        if (state == null) {
            if (states.size() >= maxActivePublishers) {
                evictForCapacity();
            }
            TopicPublisher publisher;
            try {
                publisher = publisherFactory.create(destination);
            } catch (IOException | RuntimeException e) {
                throw new IOException(
                        "Failed to create a Pub/Sub publisher for topic " + destination + ".", e);
            }
            state =
                    new DestinationState(
                            destination,
                            publisher,
                            metrics.forTopic(destination),
                            nanoClock.getAsLong());
            states.put(destination, state);
            activePublishers++;
        }
        return state;
    }

    private void evictForCapacity() throws IOException, InterruptedException {
        DestinationState evicted = removeOldestEvictable();
        if (evicted == null) {
            drainPublishersAndRepair();
            evicted = removeOldestEvictable();
        }
        if (evicted == null) {
            throw new IOException(
                    "The Pub/Sub writer reached maxActivePublishers("
                            + maxActivePublishers
                            + ") but no publisher was clean after draining; this is an internal"
                            + " lifecycle invariant failure.");
        }
        releasePublishers(List.of(evicted), "capacity eviction");
        metrics.capacityEviction();
    }

    private DestinationState removeOldestEvictable() {
        Iterator<Map.Entry<TopicDestination, DestinationState>> entries =
                states.entrySet().iterator();
        while (entries.hasNext()) {
            DestinationState candidate = entries.next().getValue();
            if (isEvictable(candidate)) {
                entries.remove();
                activePublishers--;
                return candidate;
            }
        }
        return null;
    }

    private boolean isEvictable(DestinationState state) {
        return state.inFlightPublishes == 0
                && state.pendingRetries.isEmpty()
                && state.keysToResume.isEmpty()
                && !state.topicMissing
                && !state.isolationNeeded
                && state.repairCause == null;
    }

    private void evictIdlePublishers() throws IOException, InterruptedException {
        long now = nanoClock.getAsLong();
        List<DestinationState> evicted = new ArrayList<>();
        Iterator<Map.Entry<TopicDestination, DestinationState>> entries =
                states.entrySet().iterator();
        while (entries.hasNext()) {
            DestinationState candidate = entries.next().getValue();
            if (isEvictable(candidate)
                    && now - candidate.lastAccessNanos > destinationIdleTimeoutNanos) {
                entries.remove();
                activePublishers--;
                evicted.add(candidate);
            }
        }
        releasePublishers(evicted, "idle eviction");
        for (int i = 0; i < evicted.size(); i++) {
            metrics.idleEviction();
        }
    }

    /** Releases a batch in two phases so every shutdown interval overlaps. */
    private void releasePublishers(List<DestinationState> released, String reason)
            throws IOException, InterruptedException {
        if (released.isEmpty()) {
            return;
        }
        List<AutoCloseable> closeables = new ArrayList<>(released.size() * 2);
        for (DestinationState state : released) {
            closeables.add(state.publisher::shutdown);
        }
        for (DestinationState state : released) {
            closeables.add(state.publisher);
        }
        Exception releaseFailure = null;
        try {
            Closers.closeAll(closeables);
        } catch (Exception e) {
            Optional<InterruptedException> interrupted = findInterrupted(e);
            if (interrupted.isPresent()) {
                Thread.currentThread().interrupt();
                if (interrupted.get() == e) {
                    throw interrupted.get();
                }
                // Closers keeps later failures suppressed on the first. Re-throwing only that
                // nested interrupt would erase the primary failure and its other suppressed
                // entries, while suppressing the primary back onto the same interrupt would make
                // a cycle. A fresh interruption preserves the complete collected failure tree.
                InterruptedException propagated =
                        new InterruptedException(interrupted.get().getMessage());
                propagated.addSuppressed(e);
                throw propagated;
            }
            releaseFailure = e;
        }
        List<TopicDestination> abandoned =
                released.stream()
                        .filter(state -> state.publisher.wasShutdownIncomplete())
                        .map(state -> state.destination)
                        .collect(java.util.stream.Collectors.toList());
        if (!abandoned.isEmpty()) {
            IOException failure =
                    new IOException(
                            "Pub/Sub publisher shutdown did not finish during "
                                    + reason
                                    + " for "
                                    + abandoned
                                    + ". The writer will not open replacement publishers and"
                                    + " accumulate resources; raise"
                                    + " PubSubPublisherOptions.builder().shutdownTimeout(...)"
                                    + " (sink.shutdown-timeout in Table API) or correct the"
                                    + " destination failures.");
            if (releaseFailure != null) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
        if (releaseFailure != null) {
            throw new IOException(
                    "Failed to release Pub/Sub publishers during "
                            + reason
                            + ". The writer will not open replacement publishers because the"
                            + " released resources may still be live.",
                    releaseFailure);
        }
    }

    /**
     * Finds a task-thread interrupt reported first or directly suppressed behind another failure.
     */
    private static Optional<InterruptedException> findInterrupted(Exception failure) {
        if (failure instanceof InterruptedException) {
            return Optional.of((InterruptedException) failure);
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            if (suppressed instanceof InterruptedException) {
                return Optional.of((InterruptedException) suppressed);
            }
        }
        return Optional.empty();
    }

    /**
     * Publishes the message to the destination's publisher, counts it in flight and registers its
     * completion callback.
     *
     * <p>{@code firstAttempt} is what keeps {@code numRecordsSend} a count of <em>records</em>: a
     * repair re-enters this method for every parked message, and a record must be counted once
     * however many publishes it took. The in-flight counters are the opposite — they track
     * publishes, so they are adjusted on every call.
     *
     * <p>{@code soloVerdict} says the message travels as its own single-message {@code Publish}
     * request — true only inside {@link TopicRepairer}'s isolation pass, which flushes and drains
     * around each publish — so an {@code INVALID_ARGUMENT} answering it concerns this message alone
     * and may be routed to the failure handler. From any other publish that status is a
     * request-level report the SDK fans out across the whole batch, so it must be isolated first,
     * not routed.
     */
    private void publishTo(
            DestinationState state,
            PubsubMessage message,
            boolean firstAttempt,
            boolean soloVerdict)
            throws IOException {
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
        state.inFlightPublishes++;
        inFlight.admit(serializedSize);
        if (firstAttempt) {
            state.lastAccessNanos = nanoClock.getAsLong();
            metrics.messagePublished(state.metrics, serializedSize);
        }
        ApiFutures.addCallback(
                future,
                new PublishCallback(
                        state, message, inFlight.nextSequence(), serializedSize, soloVerdict),
                Runnable::run);
    }

    /**
     * Holds a failed publish for the destination's next repair, keyed by its publish sequence so
     * the batch keeps publish order. Sole entry point, so the {@code parkedMessages} gauge cannot
     * drift from the pending buffers it reports.
     *
     * <p>Parking does not decide that a topic will be created: only {@code topicMissing} does,
     * which is why a cascade may be parked under {@code CREATE_NEVER}.
     */
    private void park(DestinationState state, long sequence, PubsubMessage message) {
        if (state.pendingRetries.put(sequence, message) == null) {
            parkedMessages++;
        }
        repairNeeded = true;
    }

    /**
     * Asks every publisher to send what it is still batching — the writer's answer to a wait that
     * has nothing left to run.
     *
     * <p>A message counts against the in-flight caps from the moment the publisher accepts it,
     * which is before it goes anywhere, so a wait can be waiting on messages that are merely
     * batched. That would put {@code batchDelayThreshold} <em>inside</em> {@code
     * publishProgressTimeout}: a batch delay configured longer than the budget would expire a wait
     * on a perfectly reachable topic, for messages this writer never sent. The task thread is
     * blocked in the wait, so it cannot add the message that would trip the size threshold instead.
     *
     * <p>{@link InFlightTracker#awaitCapacity()} calls this only once the mailbox has been found
     * empty, and at most once per wait. Both halves matter. A wait whose completions are arriving
     * is a batcher that is working, and flushing it per record — which is what a cap-bound writer
     * does — would collapse every batch to one message exactly while the job is under load. And
     * once is enough, because nothing can join a batch while the task thread is parked there.
     *
     * <p>Every wait needs it, not only the admission gate — but the drain does not call it, its
     * callers do, which is why the tracker is handed this as a hook rather than doing it for both
     * waits. {@link #repairPendingTopics} calls this and then drains, because it is reached from a
     * {@link #write} whose predecessor was parked by a failure mail during a capacity wait, so its
     * in-flight message can be sitting unflushed in exactly the same way; {@link #flush} calls it
     * before its own drain, and the isolation pass flushes the destination it is publishing to.
     * Doing it inside the drain instead would repeat a flush those callers have just made.
     */
    private void sendWhatIsStillBatched() {
        for (DestinationState state : states.values()) {
            state.publisher.flushOutstanding();
        }
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
            boolean soloVerdict,
            Throwable throwable) {
        state.inFlightPublishes--;
        inFlight.release(serializedSize);
        PubSubErrorClassifier.Kind kind = PubSubErrorClassifier.classify(throwable);
        boolean batchedRejection = kind == PubSubErrorClassifier.Kind.MESSAGE_LEVEL && !soloVerdict;
        if (kind != PubSubErrorClassifier.Kind.CANCELLATION && !batchedRejection) {
            // Root failures with a confirmed identity only. A cascade cancellation is not one — it
            // always trails a failure of an earlier publish for the same ordering key (#78), and
            // that root is counted here itself, so counting the cascade too would multiply one
            // incident by the length of the key's queue. It carries no status of its own either,
            // so it would land under UNCLASSIFIED and hide genuinely unclassifiable failures. A
            // batched INVALID_ARGUMENT is excluded for the same multiplication: the SDK reports
            // the one request-level status against every co-batched message, and the isolation
            // pass counts the true rejections when it finds them.
            metrics.publishFailure(PubSubErrorClassifier.statusCode(throwable));
        }
        if (kind == PubSubErrorClassifier.Kind.TOPIC_NOT_FOUND && repairsTopics()) {
            park(state, sequence, message);
            // The only thing that makes the repair create a topic. Everything else it does —
            // resuming ordering keys, republishing the batch in order — is needed whatever parked
            // the batch, and a repair that has not seen a NOT_FOUND must not issue a createTopic.
            state.topicMissing = true;
            state.repairCause = throwable;
        } else if (kind == PubSubErrorClassifier.Kind.CANCELLATION && orderingEnabled) {
            // With ordering enabled the SDK cancels an ordering key's queued publishes after the
            // key's first failure, so a cancellation is never a root cause — it always trails a
            // failure of an earlier publish this writer issued for the same key. Park it for the
            // repair without asking whether the root was recoverable, because that is not knowable
            // here: failure mails do not arrive in publish order (the SDK cancels queued work from
            // its own thread, so a cascade's mail can be enqueued before its root's). It does not
            // need to be knowable — repairPendingTopics drains the writer completely before
            // repairing, and that drain surfaces a fatal root through checkAsyncError, so a
            // cascade is only ever republished once the root is known to be recoverable.
            //
            // Deliberately not conditioned on the create disposition, unlike the NOT_FOUND branch
            // above. A cascade's root may be a message the failure handler dropped, which is a
            // repair CREATE_NEVER needs too. What keeps CREATE_NEVER from creating a topic is
            // topicMissing, which only a parked NOT_FOUND sets, so the guard is not needed here.
            // Asking instead whether this key has a recorded drop would be the #78 bug again:
            // the drop mail and the cascade mail arrive in either order, so a cascade observed
            // first would find nothing recorded and be misread as a root cause.
            park(state, sequence, message);
            // Only as a fallback: a real NOT_FOUND is the better budget-exhaustion cause, and it
            // wins whether it is observed before or after its cascades.
            if (state.repairCause == null) {
                state.repairCause = throwable;
            }
        } else if (kind == PubSubErrorClassifier.Kind.MESSAGE_LEVEL) {
            if (batchedRejection) {
                // A request-level report: the service rejected the batch all-or-nothing, so this
                // message is not known to be the invalid one. Park it for the isolation pass,
                // which republishes the batch one message per request so each gets its own
                // verdict — routing here would drop a whole batch for one bad message (#264).
                park(state, sequence, message);
                state.isolationNeeded = true;
                // Only as a fallback, like the cascade branch above: a NOT_FOUND stays the better
                // budget-exhaustion cause when both are in play.
                if (state.repairCause == null) {
                    state.repairCause = throwable;
                }
            } else {
                routeFailedMessage(state, message, throwable);
            }
        } else if (asyncError == null) {
            asyncError = wrapPublishFailure(state.destination, kind, throwable);
        }
    }

    /**
     * Hands a message-level publish failure to the configured handler. Reached only with a solo
     * verdict — an {@code INVALID_ARGUMENT} answering a single-message request of the isolation
     * pass — so the message really is the one the service rejected. Runs as a mailbox mail, so a
     * handler that fails the job cannot throw at a caller: its failure is captured into {@code
     * asyncError} and rethrown from the next {@link #write} or {@link #flush}, exactly as a
     * terminal publish failure is. First failure wins, as everywhere else here.
     *
     * <p>Routing is <em>not</em> skipped once {@code asyncError} is set. The writer is about to
     * fail either way, but this message really did fail terminally, and a dead-letter destination
     * that is missing it is worse than one holding a message a replay will produce again — the
     * guarantee is at-least-once.
     *
     * <p>The description does not name the topic: every reader of it reaches the element's {@code
     * describeDestination()} too — the built-in handlers compose the two — so naming it here would
     * put the topic in the sentence twice, in two spellings.
     *
     * <p>Dropping a message the SDK rejected leaves work behind when it carried an ordering key:
     * the publisher paused that key and cancelled its queued publishes, and it never resumes a key
     * on its own. So a drop registers the key for the repair to resume — see {@code
     * TopicRepairer#resumeOrderingKeys}, which is also where the reason it cannot be resumed here
     * is recorded.
     */
    private void routeFailedMessage(
            DestinationState state, PubsubMessage message, Throwable throwable) {
        TopicDestination destination = state.destination;
        metrics.messageFailed(state.metrics);
        consecutiveRejections++;
        try {
            failedMessageHandler.handle(
                    FailedMessage.of(
                            destination,
                            message,
                            "The publish was rejected because "
                                    + PubSubErrorClassifier.MESSAGE_LEVEL_REASON
                                    + ".",
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
            // The handler refused to drop, so the job is failing and its paused key will never be
            // published to again. Belt and braces rather than load-bearing: asyncError is set by
            // the block above, and every path into a repair opens with checkAsyncError, so falling
            // through would register resume work nothing would ever run. Returning keeps the
            // method's own reading straight — a resume registered below means a drop happened.
            return;
        }
        // Returning from handle() is the SPI's only way of saying "dropped", so this is the point
        // the message stops being the writer's business — and the point its key has to be handed
        // back. An unkeyed message never entered the SDK's sequential executor, so it pauses
        // nothing.
        state.routedDuringRepair++;
        if (orderingEnabled && !message.getOrderingKey().isEmpty()) {
            state.keysToResume.add(message.getOrderingKey());
            repairNeeded = true;
        }
        // After the routing, not instead of it: the message that tripped the bound really was
        // refused, and a dead-letter destination missing it would be worse than one holding it —
        // the same argument as routing beside an existing asyncError. First failure still wins.
        if (maxConsecutiveRejections != PubSubPublisherOptions.UNBOUNDED
                && consecutiveRejections >= maxConsecutiveRejections
                && asyncError == null) {
            String run =
                    consecutiveRejections == 1
                            ? "Pub/Sub topic "
                                    + destination
                                    + " refused a message (status "
                                    + PubSubErrorClassifier.statusCode(throwable)
                                    + ")"
                            : "Pub/Sub refused "
                                    + consecutiveRejections
                                    + " messages in a row (the last by topic "
                                    + destination
                                    + ", with status "
                                    + PubSubErrorClassifier.statusCode(throwable)
                                    + ") with none successfully published between them";
            asyncError =
                    new IOException(
                            run
                                    + ", reaching maxConsecutiveRejections("
                                    + maxConsecutiveRejections
                                    + "): the stream's data looks broken rather than anomalous, so"
                                    + " the job fails instead of isolating it message by message."
                                    + " Every rejected message, this one included, was routed to"
                                    + " the configured handler first;"
                                    + " PubSubPublisherOptions.builder().maxConsecutiveRejections(-1)"
                                    + " removes this bound.",
                            throwable);
        }
    }

    /**
     * Repairs every destination that owes one — messages parked for republish, an ordering key a
     * dropped message left paused, or both — until none remain (or the repair fails).
     */
    private void repairPendingTopics() throws IOException, InterruptedException {
        // Ahead of the drain below, which is the one wait in this class with no flush in front of
        // it; see sendWhatIsStillBatched for the window that opens without this.
        sendWhatIsStillBatched();
        while (repairNeeded) {
            repairNeeded = false;
            // Drain before snapshotting the parked batches: a parked root's cascade-cancellation
            // mails may still be queued, or still being enqueued by the SDK thread, and a repair
            // started from a partial batch would republish only part of a key's messages. The
            // drain is also what makes parking a cascade safe in the first place — a fatal root
            // reaches asyncError here, and the drain rethrows it, so no cascade of a
            // fatal root is ever republished.
            inFlight.drainToEmpty();
            // Iterate a snapshot: the repairer yields to the mailbox, so hardening against
            // a mail ever reaching stateFor() keeps this loop safe from map mutation.
            for (DestinationState state : new ArrayList<>(states.values())) {
                // A destination with nothing parked can still owe a repair: a dropped message's
                // ordering key is paused in the publisher with no message left to republish.
                if (!state.pendingRetries.isEmpty() || !state.keysToResume.isEmpty()) {
                    repairer.repair(state);
                }
            }
        }
    }

    /**
     * Drains all accepted publishes and resolves every repair debt without flushing the handler.
     * The final drain can process a failure mail that owes a repair rather than setting {@code
     * asyncError}, so the loop is what keeps a checkpoint from leaving unpublished messages or a
     * paused ordering key behind.
     */
    private void drainPublishersAndRepair() throws IOException, InterruptedException {
        do {
            if (repairNeeded) {
                repairPendingTopics();
            }
            sendWhatIsStillBatched();
            inFlight.drainToEmpty();
        } while (repairNeeded);
    }

    /**
     * Whether a missing topic may be repaired by creating it. Gates the {@code NOT_FOUND} parking
     * branch and that one only: since #215 the disposition does not gate parking at all — a cascade
     * is parked under {@code CREATE_NEVER} too, including one behind a message the handler dropped,
     * because repairing an ordering key creates nothing. What decides a creation is {@code
     * topicMissing}, which only this branch sets.
     */
    private boolean repairsTopics() {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED;
    }

    /**
     * Wraps a failure the writer is not repairing. A {@code CANCELLATION} reaching here always has
     * ordering disabled — with it enabled every cancellation is parked — so there is no ordering
     * wording to add: a cancellation without ordering is not the SDK's per-key cascade and saying
     * so would misdescribe it.
     */
    private IOException wrapPublishFailure(
            TopicDestination destination, PubSubErrorClassifier.Kind kind, Throwable throwable) {
        String message = "A publish to Pub/Sub topic " + destination + " failed";
        if (kind == PubSubErrorClassifier.Kind.TOPIC_NOT_FOUND) {
            message += " because the topic does not exist and createDisposition is CREATE_NEVER";
        }
        return new IOException(message + ".", throwable);
    }

    @VisibleForTesting
    int getInFlightMessages() {
        return inFlight.getInFlightMessages();
    }

    @VisibleForTesting
    long getInFlightBytes() {
        return inFlight.getInFlightBytes();
    }

    @VisibleForTesting
    int getParkedMessages() {
        return parkedMessages;
    }

    @VisibleForTesting
    int getActivePublishers() {
        return activePublishers;
    }

    @VisibleForTesting
    DestinationState getDestinationState(TopicDestination destination) {
        // states is access ordered. A test observation must not change the next capacity victim.
        for (Map.Entry<TopicDestination, DestinationState> entry : states.entrySet()) {
            if (entry.getKey().equals(destination)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * The writer-owned operations {@link TopicRepairer} needs — publishing, draining and the
     * parked-message gauge — implemented against the writer's own state. An inner class rather than
     * a lambda because the seam has three methods.
     */
    private final class RepairContextImpl implements TopicRepairer.RepairContext {

        @Override
        public void republish(DestinationState state, PubsubMessage message, boolean soloVerdict)
                throws IOException {
            publishTo(state, message, false, soloVerdict);
        }

        @Override
        public void drainInFlight() throws IOException, InterruptedException {
            inFlight.drainToEmpty();
        }

        @Override
        public void releaseParked(int count) {
            parkedMessages -= count;
        }
    }

    /**
     * Re-dispatches publish completions onto the mailbox so state stays task-thread-only.
     *
     * <p>One instance per publish: the callback carries its message so a failed publish can be
     * republished after topic auto-creation, its publish sequence so the parked batch can be
     * ordered independently of the order the failures arrive in, its serialized size so both
     * in-flight counters can be released, and whether the publish was solo — which is what upgrades
     * an {@code INVALID_ARGUMENT} answering it to a per-message verdict.
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
        private final boolean soloVerdict;

        private PublishCallback(
                DestinationState state,
                PubsubMessage message,
                long sequence,
                int serializedSize,
                boolean soloVerdict) {
            this.state = state;
            this.message = message;
            this.sequence = sequence;
            this.serializedSize = serializedSize;
            this.soloVerdict = soloVerdict;
        }

        /** The success mail: runs on the task thread. */
        @Override
        public void run() {
            state.inFlightPublishes--;
            inFlight.release(serializedSize);
            // A published message is evidence the stream is not wholly broken, whichever request
            // shape or destination carried it — a solo republish included — so the
            // consecutive-rejection bound resets on every success.
            consecutiveRejections = 0;
        }

        @Override
        public void onSuccess(String messageId) {
            inFlight.recordCompletion();
            mailboxExecutor.execute(this, state.completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            inFlight.recordCompletion();
            mailboxExecutor.execute(
                    () ->
                            onPublishFailed(
                                    state,
                                    message,
                                    sequence,
                                    serializedSize,
                                    soloVerdict,
                                    throwable),
                    state.failureDescription);
        }
    }
}
