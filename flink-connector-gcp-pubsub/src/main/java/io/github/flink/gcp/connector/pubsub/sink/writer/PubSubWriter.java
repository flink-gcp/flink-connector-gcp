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
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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
 * FailureHandler}; here that capture lands in {@link #asyncError}.
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

    /**
     * How long {@link #awaitPublishProgress} parks when the mailbox is empty. Short enough that a
     * completion arriving mid-park is picked up promptly, and reached only while nothing is
     * arriving — under load {@code tryYield} keeps returning {@code true} and nothing parks.
     */
    private static final long PROGRESS_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * The fraction of {@code publishProgressTimeout} a wait may spend without progress before it
     * says so. Spending the whole budget in silence is what makes a stall hard to operate: the
     * error counters cannot move — no publish is resolving, which is the definition of the state —
     * so nothing else reports it until the job dies, and at the shipped default that is ten minutes
     * later, by which time Flink's own checkpoint timeout may have failed the job first with a
     * message that names nothing about Pub/Sub.
     */
    private static final int PROGRESS_WARN_FRACTION = 10;

    /**
     * What {@link #awaitPublishProgress} returns when it ran a mail rather than measuring a gap.
     */
    private static final long RAN_A_MAIL = -1L;

    private final PubSubSinkConfig<T> config;
    private final PublisherFactory publisherFactory;
    private final TopicAdmin topicAdmin;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightMessages;
    private final long maxInFlightBytes;
    private final long publishProgressTimeoutNanos;
    private final long publishProgressWarnAfterNanos;
    private final RetrySchedule recoverySchedule;
    private final boolean orderingEnabled;
    private final FailureHandler<? super FailedMessage> failedMessageHandler;
    private final PubSubSinkWriterMetrics metrics;

    /** Lazily populated per-topic publisher state; touched only on the task thread. */
    private final Map<TopicDestination, DestinationState> states = new HashMap<>();

    /** Number of publishes not yet acknowledged; touched only on the task thread. */
    private int inFlightMessages;

    /**
     * Serialized size of the publishes not yet acknowledged; touched only on the task thread.
     * Excludes parked messages — their failure mail released them before parking, and the repair
     * republishes those same objects.
     */
    private long inFlightBytes;

    /**
     * Issue order of publishes, which is what a parked batch is sorted by. Assigned in {@link
     * #publishTo} on the task thread, so it needs no synchronization.
     */
    private long nextPublishSequence;

    /**
     * Messages held for a destination's next repair, across every destination. A plain counter
     * rather than a sum over {@code states} because the gauge reading it runs on the reporter
     * thread: walking the destination maps from there would race with the task thread mutating
     * them.
     */
    private int parkedMessages;

    /**
     * When the publisher last answered a publish, successfully or not — the clock {@link
     * #awaitPublishProgress} measures its budget against.
     *
     * <p>Stamped in the completion callback, on whichever SDK thread runs it, and <b>not</b> when
     * the resulting mailbox mail runs: what the budget is asking is whether the publisher is still
     * answering, and a mail that has been enqueued but not yet dequeued already answers that. Were
     * it stamped on the task thread instead, a mailbox busy with unrelated work for longer than the
     * budget would fail a sink whose every publish was completing on time.
     *
     * <p>The one field of this writer not confined to the task thread, hence {@code volatile}: it
     * is a monotonic timestamp rather than logical state, so a reader wants the freshest value and
     * nothing is derived from reading it together with anything else. Initialised at construction
     * so it is always a real {@code nanoTime} reading — the zero default is not one, and comparing
     * it against a reading is only meaningful as a difference.
     */
    private volatile long lastCompletionNanos;

    /**
     * When {@link #warnIfStalled} last spoke; touched only on the task thread. A field rather than
     * a per-wait flag because a wait is not an incident: {@link #repairDestination}'s isolation
     * pass drains once per parked message, and a parked batch runs to about twice {@code
     * maxInFlightMessages}, so one {@code flush} can make a thousand waits and a per-wait flag
     * would put a line in the log for each of them.
     */
    private long lastStallWarnNanos;

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
            SinkWriterMetricGroup metricGroup,
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
        // Checked here for the reason the two caps above are: Java deserialization does not run
        // the builder, and a non-positive budget would make every wait expire on its first pass.
        // Null-tolerant on purpose: this field was added under an unchanged serialVersionUID, so
        // the stream the guard exists for — an older one, which the builder never ran against —
        // carries no value for it at all. Without the null check that case is a bare NPE from
        // isZero() rather than the named failure the two caps above give.
        Preconditions.checkArgument(
                options.getPublishProgressTimeout() != null
                        && !options.getPublishProgressTimeout().isZero()
                        && !options.getPublishProgressTimeout().isNegative(),
                "publishProgressTimeout must be positive");
        // The ceiling as well as the floor: toNanos() below is the call the builder's own ceiling
        // exists to protect, and this re-check is here precisely for the instance the builder never
        // saw.
        OptionChecks.checkExpressibleInNanos(
                options.getPublishProgressTimeout(), "publishProgressTimeout");
        this.publishProgressTimeoutNanos = options.getPublishProgressTimeout().toNanos();
        // A tenth of the budget: long enough that ordinary backpressure never reaches it, early
        // enough that the line beats both clocks that can end the job.
        this.publishProgressWarnAfterNanos = publishProgressTimeoutNanos / PROGRESS_WARN_FRACTION;
        this.lastCompletionNanos = System.nanoTime();
        this.lastStallWarnNanos = this.lastCompletionNanos - publishProgressWarnAfterNanos;
        this.recoverySchedule = recoverySchedule;
        this.orderingEnabled = options.isEnableMessageOrdering();
        this.failedMessageHandler = config.getFailedMessageHandler();
        this.metrics = new PubSubSinkWriterMetrics(metricGroup, options.isPerDestinationMetrics());
        this.metrics.bindWriterState(
                (Gauge<Integer>) this::getInFlightMessages,
                (Gauge<Long>) this::getInFlightBytes,
                (Gauge<Integer>) this::getParkedMessages);
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
        awaitCapacity();
        publishTo(state, message, true, false);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        // Loop: the final drain can process a failure mail that owes a repair rather than setting
        // asyncError — a NOT_FOUND that parks messages, or a message-level rejection the handler
        // drops, leaving its ordering key paused with nothing parked at all. Returning then would
        // complete a checkpoint with unpublished messages, or with a key no later publish for it
        // could get past. This loop is what makes "a completed checkpoint leaves neither" true.
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
            // Dropped with the writer, so the gauge must not keep reporting them.
            parkedMessages = 0;
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
     *
     * <p>{@code firstAttempt} is what keeps {@code numRecordsSend} a count of <em>records</em>: a
     * repair re-enters this method for every parked message, and a record must be counted once
     * however many publishes it took. The in-flight counters are the opposite — they track
     * publishes, so they are adjusted on every call.
     *
     * <p>{@code soloVerdict} says the message travels as its own single-message {@code Publish}
     * request — true only inside {@link #repairDestination}'s isolation pass, which flushes and
     * drains around each publish — so an {@code INVALID_ARGUMENT} answering it concerns this
     * message alone and may be routed to the failure handler. From any other publish that status is
     * a request-level report the SDK fans out across the whole batch, so it must be isolated first,
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
        inFlightMessages++;
        inFlightBytes += serializedSize;
        if (firstAttempt) {
            metrics.messagePublished(state.metrics, serializedSize);
        }
        ApiFutures.addCallback(
                future,
                new PublishCallback(
                        state, message, nextPublishSequence++, serializedSize, soloVerdict),
                Runnable::run);
    }

    /** Releases one completed publish from both in-flight counters. */
    private void releaseInFlight(int serializedSize) {
        inFlightMessages--;
        inFlightBytes -= serializedSize;
    }

    /**
     * Holds a failed publish for the destination's next repair, keyed by its publish sequence so
     * the batch keeps publish order. Sole entry point, so the {@link #parkedMessages} gauge cannot
     * drift from the pending buffers it reports.
     *
     * <p>Parking does not decide that a topic will be created: only {@link
     * DestinationState#topicMissing} does, which is why a cascade may be parked under {@code
     * CREATE_NEVER}.
     */
    private void park(DestinationState state, long sequence, PubsubMessage message) {
        if (state.pendingRetries.put(sequence, message) == null) {
            parkedMessages++;
        }
        repairNeeded = true;
    }

    /**
     * Admission gate for {@link #write}: runs mailbox mails (publish completions) until both
     * in-flight caps have room, surfacing any captured publish failure before the caller publishes.
     *
     * <p>Both predicates are "at or above the cap", never "would this message fit", so an empty
     * writer always admits. That matters beyond overshoot accounting: a wait here ends only when a
     * publish completes, and with nothing in flight none can, so any predicate that can hold at
     * zero waits for something that cannot happen — before {@code publishProgressTimeout} a task
     * hang, and since it a job failure blaming the topic. The positive-value preconditions on both
     * options are what rule that out.
     *
     * <p>A wait that finds the mailbox empty asks the publishers to send what they are still
     * batching — see {@link #sendWhatIsStillBatched}, which every wait here does, not just this
     * one.
     */
    private void awaitCapacity() throws IOException, InterruptedException {
        boolean flushed = false;
        long start = System.nanoTime();
        while (inFlightMessages >= maxInFlightMessages || inFlightBytes >= maxInFlightBytes) {
            checkAsyncError();
            long idleNanos = awaitPublishProgress(start, "admitting a record");
            if (idleNanos != RAN_A_MAIL && !flushed) {
                flushed = true;
                sendWhatIsStillBatched();
            }
            warnIfStalled(idleNanos, "admitting a record");
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
        long start = System.nanoTime();
        while (inFlightMessages > 0) {
            checkAsyncError();
            warnIfStalled(
                    awaitPublishProgress(start, "draining the in-flight publishes"),
                    "draining the in-flight publishes");
        }
        checkAsyncError();
    }

    /**
     * Says once, per wait, that this wait has stopped making progress — long before the budget that
     * ends it.
     *
     * <p>The counters an operator watches cannot report this state: no publish is resolving, which
     * is what the state <em>is</em>, so {@code errorClass.*.errors} and {@code
     * numRecordsSendErrors} stay where they were for its whole duration. Without this line the
     * first thing anyone sees is the job dying — at the shipped default ten minutes later, and
     * possibly of Flink's checkpoint timeout instead, which names nothing about Pub/Sub.
     *
     * @return whether the line has now been said, to be passed back on the next pass
     */
    private void warnIfStalled(long idleNanos, String what) {
        if (idleNanos == RAN_A_MAIL || idleNanos < publishProgressWarnAfterNanos) {
            return;
        }
        long now = System.nanoTime();
        if (now - lastStallWarnNanos < publishProgressWarnAfterNanos) {
            return;
        }
        lastStallWarnNanos = now;
        LOG.warn(
                "No publish to Pub/Sub has completed for {} while {} ({} publish(es) in flight)."
                        + " The sink is waiting, not failing: it fails if nothing completes within"
                        + " its publishProgressTimeout of {}. Watch numRecordsSend, which stays"
                        + " flat for as long as this lasts; the error counters need not move at"
                        + " all, since a publish that never answers is never counted as a failure.",
                Duration.ofNanos(idleNanos),
                what,
                inFlightMessages,
                Duration.ofNanos(publishProgressTimeoutNanos));
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
     * <p>Called only once the mailbox has been found empty, and at most once per wait. Both halves
     * matter. A wait whose completions are arriving is a batcher that is working, and flushing it
     * per record — which is what a cap-bound writer does — would collapse every batch to one
     * message exactly while the job is under load. And once is enough, because nothing can join a
     * batch while the task thread is parked here.
     *
     * <p>Every wait needs it, not just the admission gate: {@link #repairPendingTopics} opens with
     * a drain of its own, reached from a {@link #write} whose predecessor was parked by a failure
     * mail during a capacity wait — so its in-flight message can be sitting unflushed in exactly
     * the same way.
     */
    private void sendWhatIsStillBatched() {
        for (DestinationState state : states.values()) {
            state.publisher.flushOutstanding();
        }
    }

    /**
     * Runs one mailbox mail, failing if nothing has completed a publish for {@code
     * publishProgressTimeout}.
     *
     * <p>What is bounded is a <b>stall, not a slow topic</b>: {@link #lastCompletionNanos} is
     * restamped by every completion the publisher reports, so one that keeps answering never spends
     * the budget however long the wait lasts in total, while one that has stopped answering
     * entirely fails the job once. That distinction is the whole design — a plain deadline on the
     * call would fail a job the SDK's retries were about to rescue, which is the objection the
     * issue raised against bounding the sink's own writes at all (#333).
     *
     * <p>Both callers pass the moment their wait began, which is what stops an idle writer from
     * expiring immediately: with no publish in the last hour, {@code lastCompletionNanos} is an
     * hour old and only the later of the two is the honest start of <em>this</em> wait.
     *
     * <p>{@link MailboxExecutor#yield()} cannot be used here — it blocks until a mail arrives, and
     * a stalled publisher sends none, so the deadline would never be read. {@link
     * MailboxExecutor#tryYield()} plus a short park is the only shape the interface offers, and it
     * costs nothing in the case that matters: while completions are arriving {@code tryYield}
     * returns {@code true} and nothing parks at all.
     *
     * <p>The budget is read only once {@code tryYield} has come back empty; the body says what that
     * costs and why the alternative costs more. Returns the time this wait has gone without
     * progress, or {@link #RAN_A_MAIL} if it ran one instead — which is what tells the caller the
     * mailbox is empty, so that {@link #sendWhatIsStillBatched} and the warning are worth doing.
     * The caller owns the once-per-wait flags for those, because {@link #awaitCapacity} is on the
     * record path and a state object here would be an allocation per record.
     */
    private long awaitPublishProgress(long waitStartNanos, String what)
            throws IOException, InterruptedException {
        // Read first, and on every pass. The blocking yield() this replaced took the mailbox lock
        // interruptibly and so threw of its own accord; tryYield() does not look at the flag at
        // all, so without this a cancellation arriving while mails keep coming would not be
        // observed until the budget ran out — and would then surface as the wrong exception.
        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted while " + what + " for Pub/Sub.");
        }
        // The budget is read only once the mailbox has nothing left to run, and that ordering is
        // the conservative half of a genuine trade rather than an accident. Reading it first lets
        // the wait expire while work it has not yet done would have ended it — a completion mail
        // queued behind other work is a publish that already succeeded, and failing the job for it
        // would blame an unreachable topic for a busy task thread. Reading it here instead means a
        // mailbox saturated for the whole budget defers the check, so a stall behind continuous
        // unrelated mail traffic is noticed late rather than never. Failing a healthy job is the
        // worse of the two, so this ordering takes the late notice.
        if (mailboxExecutor.tryYield()) {
            return RAN_A_MAIL;
        }
        // Subtraction rather than Math.max: both are System.nanoTime() readings — the constructor
        // stamps lastCompletionNanos so it is never the zero default — and their ordering is only
        // meaningful as a difference.
        long idleSinceNanos =
                lastCompletionNanos - waitStartNanos > 0 ? lastCompletionNanos : waitStartNanos;
        long idleNanos = System.nanoTime() - idleSinceNanos;
        if (idleNanos >= publishProgressTimeoutNanos) {
            throw new IOException(
                    "No publish to Pub/Sub completed for "
                            + Duration.ofNanos(idleNanos)
                            + " while "
                            + what
                            + " ("
                            + inFlightMessages
                            + " publish(es) still in flight), so the sink gave up its"
                            + " publishProgressTimeout of "
                            + Duration.ofNanos(publishProgressTimeoutNanos)
                            + ". Nothing is dropped: the job fails and the records behind those"
                            + " publishes are replayed from the last completed checkpoint, as"
                            + " duplicates if the client delivers them after all. This bounds a"
                            + " publisher that has stopped answering, not a slow one — any"
                            + " completion restarts the budget. Every retryable status the client"
                            + " keeps retrying looks the same from here, so read the cause and the"
                            + " errorClass counters before assuming the topic is unreachable:"
                            + " RESOURCE_EXHAUSTED is a quota to raise, not a budget."
                            + " PubSubPublisherOptions.builder().publishProgressTimeout(...) is the"
                            + " budget to raise when the publisher is merely slower than it.");
        }
        // Parked rather than spun, and in slices so the deadline is still read promptly. The
        // remainder is what keeps a budget shorter than the slice honest.
        long parkNanos =
                Math.min(PROGRESS_POLL_INTERVAL_NANOS, publishProgressTimeoutNanos - idleNanos);
        if (parkNanos > 0) {
            // Returns on interrupt without throwing and without clearing the flag, so the next
            // pass's read above is what turns it into an InterruptedException.
            LockSupport.parkNanos(parkNanos);
        }
        return idleNanos;
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
        releaseInFlight(serializedSize);
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
     * handler that fails the job cannot throw at a caller: its failure is captured into {@link
     * #asyncError} and rethrown from the next {@link #write} or {@link #flush}, exactly as a
     * terminal publish failure is. First failure wins, as everywhere else here.
     *
     * <p>Routing is <em>not</em> skipped once {@link #asyncError} is set. The writer is about to
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
     * on its own. So a drop registers the key for the repair to resume — see {@link
     * #resumeOrderingKeys}, which is also where the reason it cannot be resumed here is recorded.
     */
    private void routeFailedMessage(
            DestinationState state, PubsubMessage message, Throwable throwable) {
        TopicDestination destination = state.destination;
        metrics.messageFailed(state.metrics);
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
            // reaches asyncError here, and drainInFlight rethrows it, so no cascade of a
            // fatal root is ever republished.
            drainInFlight();
            // Iterate a snapshot: repairDestination yields to the mailbox, so hardening against
            // a mail ever reaching stateFor() keeps this loop safe from map mutation.
            for (DestinationState state : new ArrayList<>(states.values())) {
                // A destination with nothing parked can still owe a repair: a dropped message's
                // ordering key is paused in the publisher with no message left to republish.
                if (!state.pendingRetries.isEmpty() || !state.keysToResume.isEmpty()) {
                    repairDestination(state);
                }
            }
        }
    }

    /**
     * Republishes the destination's parked messages — creating its topic first when that is what
     * they are parked for — retrying within the recovery schedule while topic metadata propagates.
     * Each attempt drains the writer completely (repair is rare, so waiting on unrelated
     * destinations' publishes is acceptable for the simplicity of reusing {@link #drainInFlight});
     * an isolating attempt drains once per message, so its pass publishes strictly one message at a
     * time. Failures during a retry re-enter the pending buffer through the normal callback path;
     * non-{@code NOT_FOUND} failures abort the repair from within the drain.
     *
     * <p>Only a parked {@code NOT_FOUND} creates a topic. Every other reason a batch is here — a
     * cascade of a message the failure handler dropped, a request-level {@code INVALID_ARGUMENT}
     * awaiting isolation, or a publish that reached an ordering key still paused from one — needs
     * the resume and the republish and nothing else, and issuing a {@code createTopic} for them
     * would both misreport {@code topicsCreated} and create a topic under {@code CREATE_NEVER}.
     *
     * <p>An attempt whose batch was parked for a request-level {@code INVALID_ARGUMENT} runs as an
     * <b>isolation pass</b>: each message goes out as its own single-message request, flushed and
     * drained individually, so the service answers per message. A message rejected solo is routed
     * to the failure handler by its own drain, and the ordering key that rejection paused is
     * resumed before the key's next message is republished — so one pass drains an arbitrarily long
     * run of invalid messages in a single attempt, and the budget keeps bounding
     * <em>unproductive</em> retrying rather than the length of a poisoned key (#269). Per-key order
     * holds because the batch is in publish-sequence order and nothing else publishes during a
     * repair.
     *
     * <p>A fatal solo failure surfaces from the pass's drain and aborts the repair with the
     * not-yet-republished remainder abandoned — in neither the pending buffer nor in flight. That
     * is safe for the same reason {@link #close}'s parked-message drop is: the checkpoint does not
     * complete, so the restart replays those records.
     */
    private void repairDestination(DestinationState state)
            throws IOException, InterruptedException {
        // Creation is checked per attempt, not once up front: a batch parked for another reason
        // can turn out to need it, when its republish is the publish that first meets the missing
        // topic. At most once per repair all the same — the retry loop exists for topic metadata
        // propagating to the publisher, where the topic already exists and creating it again would
        // answer nothing.
        boolean topicCreated = false;
        state.routedDuringRepair = 0;
        for (int attempt = 1; ; attempt++) {
            if (state.topicMissing) {
                state.topicMissing = false;
                if (!topicCreated) {
                    topicCreated = true;
                    LOG.info(
                            "A publish to Pub/Sub topic {} failed because the topic does not exist;"
                                    + " creating it (CREATE_IF_NEEDED).",
                            state.destination);
                    topicAdmin.createTopic(state.destination, config.getTopicCreateOptions());
                    metrics.topicCreated();
                }
            }
            // Isolation is decided per attempt for the same reason creation is: a batch parked
            // for a NOT_FOUND can meet a request-level INVALID_ARGUMENT on its republish, and
            // only the attempt after that report can know to isolate.
            boolean isolating = state.isolationNeeded;
            state.isolationNeeded = false;
            // Keyed by publish sequence, so the batch is in the order the messages were originally
            // published however their failure mails interleaved.
            List<PubsubMessage> batch = new ArrayList<>(state.pendingRetries.values());
            state.pendingRetries.clear();
            // Every attempt resumes the batch's ordering keys first: the failure that parked the
            // batch — and every failed republish attempt since — paused them in the publisher.
            resumeOrderingKeys(state, batch);
            // Republishes are not a first attempt in either shape: these records were counted by
            // the write that admitted them.
            if (isolating) {
                for (PubsubMessage message : batch) {
                    // Released one at a time, not up front: the pass holds the rest of the batch
                    // through a drain per message, and the gauge must keep reporting what the
                    // writer still holds — a pass over a long batch is exactly when a reader
                    // watches it.
                    parkedMessages--;
                    publishTo(state, message, false, true);
                    state.publisher.flushOutstanding();
                    drainInFlight();
                    // A rejection routed by that drain paused its ordering key and registered it;
                    // hand the key back before its next message, or the rest of the pass comes
                    // back cancelled and every drop costs one budget attempt.
                    resumeRegisteredKeys(state);
                }
            } else {
                parkedMessages -= batch.size();
                for (PubsubMessage message : batch) {
                    publishTo(state, message, false, false);
                }
                state.publisher.flushOutstanding();
                drainInFlight();
            }
            if (state.pendingRetries.isEmpty()) {
                // The incident is over, so its cause must not outlive it: a cascade only fills
                // repairCause in when it is still null, so a value left behind here would be
                // reported as the cause of some later destination-level failure it had nothing to
                // do with. A dropped message provokes a repair of its own, so incidents on one
                // destination are not rare enough to leave that to chance.
                state.repairCause = null;
                return;
            }
            if (attempt >= recoverySchedule.maxAttempts()) {
                if (state.routedDuringRepair > 0) {
                    // Distinguished from the topic-shaped exhaustion below: this repair was
                    // draining a key whose messages the handler is dropping, and the budget ran
                    // out with messages still parked — the reader needs to know drops happened
                    // and roughly how many, not only to go looking for a topic problem. The two
                    // facts are not exclusive, so a creation is still reported here.
                    throw new IOException(
                            "Republishing to Pub/Sub topic "
                                    + state.destination
                                    + " could not drain its parked messages within the recovery"
                                    + " budget ("
                                    + attempt
                                    + " attempt(s)"
                                    + (topicCreated ? ", after creating the topic" : "")
                                    + "); "
                                    + state.routedDuringRepair
                                    + " message(s) were handed to the failure handler during the"
                                    + " repair.",
                            state.repairCause);
                }
                throw new IOException(
                        "Republishing to Pub/Sub topic "
                                + state.destination
                                + (topicCreated
                                        ? " kept failing after creating the topic ("
                                        : " kept failing (")
                                + attempt
                                + " attempt(s)).",
                        state.repairCause);
            }
            long backoffMs = recoverySchedule.backoffMs(attempt);
            LOG.info(
                    "Republishing to Pub/Sub topic {} still fails; backing off {} ms"
                            + " (attempt {}/{}).",
                    state.destination,
                    backoffMs,
                    attempt,
                    recoverySchedule.maxAttempts());
            Thread.sleep(backoffMs);
        }
    }

    /**
     * Resumes the distinct ordering keys of the batch, plus those of the messages the failure
     * handler dropped, on the destination's publisher.
     *
     * <p>Keys are resumed only from within a repair — here at an attempt's start, and through
     * {@link #resumeRegisteredKeys} between an isolation pass's publishes — and deliberately never
     * from {@link #routeFailedMessage}. {@link #write} tests {@code repairNeeded} <em>before</em>
     * {@link #awaitCapacity()}, and mailbox mails — the drop among them — run inside it, so a key
     * resumed from the failure mail could be published to by the rest of that same {@code write}
     * while the key's cascades were still parked: a newer message ahead of older ones, the one
     * thing the repair exists to prevent. Left paused, that racing publish comes back cancelled, is
     * parked, and is republished in publish-sequence order with the rest. The isolation pass can
     * resume mid-batch without opening that race, because the key's remaining messages are held by
     * the pass itself in sequence order and the mails its drains run only complete publishes, never
     * issue one.
     *
     * <p>Dropped keys are drained here rather than re-resumed on every attempt: a later attempt
     * only re-pauses keys the batch republished, which the batch itself covers, and a dropped key
     * with nothing left to republish cannot be paused again by this repair.
     *
     * <p>{@code resumePublish} is a no-op for a key that is not paused, and rejects a shut-down
     * publisher — unreachable here, since a repair runs only from {@link #write} or {@link #flush}.
     */
    private void resumeOrderingKeys(DestinationState state, List<PubsubMessage> batch) {
        if (!orderingEnabled) {
            return;
        }
        for (PubsubMessage message : batch) {
            if (!message.getOrderingKey().isEmpty()) {
                state.keysToResume.add(message.getOrderingKey());
            }
        }
        resumeRegisteredKeys(state);
    }

    /**
     * Resumes and drains the registered keys, without adding a batch's — the mid-pass complement of
     * {@link #resumeOrderingKeys} (which delegates here), run by the isolation pass after each solo
     * verdict so a key a drop just paused is handed back before the key's next republish.
     */
    private void resumeRegisteredKeys(DestinationState state) {
        if (!orderingEnabled) {
            return;
        }
        for (String orderingKey : state.keysToResume) {
            state.publisher.resumePublish(orderingKey);
        }
        state.keysToResume.clear();
    }

    /**
     * Whether a missing topic may be repaired by creating it. Gates the {@code NOT_FOUND} parking
     * branch and that one only: since #215 the disposition does not gate parking at all — a cascade
     * is parked under {@code CREATE_NEVER} too, including one behind a message the handler dropped,
     * because repairing an ordering key creates nothing. What decides a creation is {@link
     * DestinationState#topicMissing}, which only this branch sets.
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
        return inFlightMessages;
    }

    @VisibleForTesting
    long getInFlightBytes() {
        return inFlightBytes;
    }

    @VisibleForTesting
    int getParkedMessages() {
        return parkedMessages;
    }

    /** Per-topic publisher plus the destination's repair and completion state. */
    private final class DestinationState {

        private final TopicDestination destination;
        private final TopicPublisher publisher;

        /**
         * Messages awaiting republish — parked for a missing topic, a cascade cancellation, or a
         * request-level rejection awaiting isolation — keyed by publish sequence so the batch is
         * republished in publish order. Sorting matters because the failure mails that park them do
         * not arrive in publish order, and republishing a key's messages out of order would break
         * the very guarantee the repair exists to preserve.
         */
        private final SortedMap<Long, PubsubMessage> pendingRetries = new TreeMap<>();

        /**
         * Ordering keys of messages the failure handler dropped, which the publisher paused and
         * will never resume on its own. Drained by the next repair attempt, and mid-pass by the
         * isolation pass after each drop. Separate from {@link #pendingRetries} because the dropped
         * message itself is gone: the key needs handing back even when there is nothing left to
         * republish for it.
         */
        private final Set<String> keysToResume = new LinkedHashSet<>();

        /**
         * Whether a {@code NOT_FOUND} is among the reasons this destination owes a repair — the
         * only one that calls for creating the topic. Cleared by the repair that answers it.
         */
        private boolean topicMissing;

        /**
         * Whether a request-level {@code INVALID_ARGUMENT} is among the reasons this destination
         * owes a repair — the only one that calls for republishing the batch one message per
         * request. Cleared by the attempt that answers it, like {@link #topicMissing}, and re-set
         * by any later batched rejection.
         */
        private boolean isolationNeeded;

        /**
         * Messages the current repair handed to the failure handler; zeroed when a repair starts,
         * read at budget exhaustion to choose between its two messages. Per destination rather than
         * per writer because that is the delta the exhaustion reports — and routing can only happen
         * from the isolation pass of the destination being repaired, since a solo verdict exists
         * nowhere else.
         */
        private long routedDuringRepair;

        /**
         * Retained as the cause of a budget-exhaustion failure: the destination's {@code
         * NOT_FOUND}, or — when none was ever observed — the batched {@code INVALID_ARGUMENT} or
         * cascade cancellation that parked the batch, whichever came first.
         */
        private Throwable repairCause;

        private final String completionDescription;
        private final String failureDescription;

        /**
         * The destination's optional per-destination counters, resolved once here rather than per
         * record — the topic's resource name is composed by the lookup.
         */
        private final DestinationMetrics.Counters metrics;

        private DestinationState(TopicDestination destination, TopicPublisher publisher) {
            this.destination = destination;
            this.publisher = publisher;
            this.completionDescription = "Complete a Pub/Sub publish to " + destination;
            this.failureDescription = "Fail a Pub/Sub publish to " + destination;
            this.metrics = PubSubWriter.this.metrics.forTopic(destination);
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
            releaseInFlight(serializedSize);
        }

        @Override
        public void onSuccess(String messageId) {
            lastCompletionNanos = System.nanoTime();
            mailboxExecutor.execute(this, state.completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            lastCompletionNanos = System.nanoTime();
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
