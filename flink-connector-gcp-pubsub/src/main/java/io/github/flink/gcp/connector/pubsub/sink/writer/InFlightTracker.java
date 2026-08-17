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
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * The writer's in-flight ledger, its admission gate, and the progress budget both of its waits are
 * bounded on (ADR-0052).
 *
 * <p>One class rather than three because the waits re-read the counters on every pass: a ledger
 * that did not know how to wait, and a wait that had to ask a ledger, would be the same loop with a
 * getter between its halves.
 *
 * <p>Everything here <em>mutates</em> on the task thread except {@link #recordCompletion()}, which
 * the client library calls on whichever of its threads answered a publish. The two counters are
 * also <em>read</em> off it — the writer binds them as gauges, and a metric reporter runs on a
 * thread of its own (ADR-0009) — which is a torn `long` at worst and is why nothing derives a
 * decision from reading them together.
 *
 * <p>The two waits differ in one way that is deliberate and load-bearing: {@link #awaitCapacity()}
 * asks the caller, once per wait, to send what the publishers are still batching, and {@link
 * #drainToEmpty} never does — its callers flush before they call it. Moving that hook to the drain
 * as well would duplicate flushes the callers already made, and the counts are pinned by tests.
 */
@Internal
final class InFlightTracker {

    /**
     * How long {@link #awaitProgress} parks when the mailbox is empty. Short enough that a
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

    /** What {@link #awaitProgress} returns when it ran a mail rather than measuring a gap. */
    private static final long RAN_A_MAIL = -1L;

    private final MailboxExecutor mailboxExecutor;
    private final Logger log;
    private final ThrowingRunnable<IOException> failureCheck;
    private final Runnable sendWhatIsStillBatched;
    private final int maxInFlightMessages;
    private final long maxInFlightBytes;
    private final long publishProgressTimeoutNanos;
    private final long publishProgressWarnAfterNanos;

    /** Number of publishes not yet acknowledged; touched only on the task thread. */
    private int inFlightMessages;

    /**
     * Serialized size of the publishes not yet acknowledged; touched only on the task thread.
     * Excludes parked messages — their failure mail released them before parking, and the repair
     * republishes those same objects.
     */
    private long inFlightBytes;

    /**
     * Issue order of publishes, which is what a parked batch is sorted by. Handed out on the task
     * thread, so it needs no synchronization.
     */
    private long nextPublishSequence;

    /**
     * When the publisher last answered a publish, successfully or not — the clock {@link
     * #awaitProgress} measures its budget against.
     *
     * <p>Stamped in the completion callback, on whichever SDK thread runs it, and <b>not</b> when
     * the resulting mailbox mail runs: what the budget is asking is whether the publisher is still
     * answering, and a mail that has been enqueued but not yet dequeued already answers that. Were
     * it stamped on the task thread instead, a mailbox busy with unrelated work for longer than the
     * budget would fail a sink whose every publish was completing on time.
     *
     * <p>The one field here <em>written</em> off the task thread, hence {@code volatile}: it is a
     * monotonic timestamp rather than logical state, so a reader wants the freshest value and
     * nothing is derived from reading it together with anything else. Initialised at construction
     * so it is always a real {@code nanoTime} reading — the zero default is not one, and comparing
     * it against a reading is only meaningful as a difference.
     */
    private volatile long lastCompletionNanos;

    /**
     * When {@link #warnIfStalled} last spoke; touched only on the task thread. A field rather than
     * a per-wait flag because a wait is not an incident: {@link TopicRepairer}'s isolation pass
     * drains once per parked message, and a parked batch runs to about twice {@code
     * maxInFlightMessages}, so one {@code flush} can make a thousand waits and a per-wait flag
     * would put a line in the log for each of them.
     */
    private long lastStallWarnNanos;

    /**
     * Creates the tracker.
     *
     * @param mailboxExecutor the writer's mailbox, whose mails are the publish completions
     * @param options the publisher options carrying both caps and the progress budget
     * @param log the writer's logger, so a stalled wait keeps reporting under the category an
     *     operator's filters and the tests already use
     * @param failureCheck rethrows a publish failure captured on a callback thread; run at the top
     *     of every wait pass and once more after each wait
     * @param sendWhatIsStillBatched asks the publishers for what they are still batching, run at
     *     most once per {@link #awaitCapacity()} and never by {@link #drainToEmpty}
     */
    InFlightTracker(
            MailboxExecutor mailboxExecutor,
            PubSubPublisherOptions options,
            Logger log,
            ThrowingRunnable<IOException> failureCheck,
            Runnable sendWhatIsStillBatched) {
        this.mailboxExecutor = mailboxExecutor;
        this.log = log;
        this.failureCheck = failureCheck;
        this.sendWhatIsStillBatched = sendWhatIsStillBatched;
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
        // Checked here for the reason the two caps are: Java deserialization does not run the
        // builder, and a non-positive budget would make every wait expire on its first pass.
        // Null-tolerant on purpose: this field was added under an unchanged serialVersionUID, so
        // the stream the guard exists for — an older one, which the builder never ran against —
        // carries no value for it at all. Without the null check that case is a bare NPE from
        // isZero() rather than the named failure the two caps give.
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
    }

    /**
     * Hands out the issue order of the next publish, which is what a parked batch is sorted by.
     *
     * @return the sequence number to stamp on this publish
     */
    long nextSequence() {
        return nextPublishSequence++;
    }

    /**
     * Counts one accepted publish against both caps.
     *
     * @param serializedSize the message's serialized size
     */
    void admit(int serializedSize) {
        inFlightMessages++;
        inFlightBytes += serializedSize;
    }

    /**
     * Releases one completed publish from both counters.
     *
     * @param serializedSize the size {@link #admit} counted
     */
    void release(int serializedSize) {
        inFlightMessages--;
        inFlightBytes -= serializedSize;
    }

    /**
     * Records that the publisher answered a publish, restarting the progress budget.
     *
     * <p>The one method here that is not task-thread-confined: the client library calls it from
     * whichever of its threads completed the publish.
     */
    void recordCompletion() {
        lastCompletionNanos = System.nanoTime();
    }

    /**
     * Admission gate for the write path: runs mailbox mails (publish completions) until both
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
     * batching, once per wait. {@link #drainToEmpty} does not: its callers flush first.
     *
     * @throws IOException if a captured publish failure surfaces, or the budget expires
     * @throws InterruptedException if the task thread is interrupted
     */
    void awaitCapacity() throws IOException, InterruptedException {
        boolean flushed = false;
        long start = System.nanoTime();
        while (inFlightMessages >= maxInFlightMessages || inFlightBytes >= maxInFlightBytes) {
            failureCheck.run();
            long idleNanos = awaitProgress(start, "admitting a record");
            if (idleNanos != RAN_A_MAIL && !flushed) {
                flushed = true;
                sendWhatIsStillBatched.run();
            }
            warnIfStalled(idleNanos, "admitting a record");
        }
        failureCheck.run();
    }

    /**
     * Runs mailbox mails until <b>no</b> publish is in flight, surfacing any captured publish
     * failure — including one processed by the final mail — before the caller proceeds.
     *
     * <p>This is a correctness primitive, not backpressure, and reaching exactly zero is what two
     * guarantees rest on (#78, #110): a fatal root failure reaches the writer's captured error and
     * is rethrown here before any cascade of it can be republished, and a parked batch is never
     * snapshotted while a cascade of it is still in flight. It must stay independent of the
     * in-flight caps — no byte or count limit may weaken it into a low-water mark.
     *
     * <p>Keyed on the message count alone: a {@code PubsubMessage} can serialize to zero bytes, so
     * {@code inFlightBytes == 0} does not imply an empty writer.
     *
     * @throws IOException if a captured publish failure surfaces, or the budget expires
     * @throws InterruptedException if the task thread is interrupted
     */
    void drainToEmpty() throws IOException, InterruptedException {
        long start = System.nanoTime();
        while (inFlightMessages > 0) {
            failureCheck.run();
            warnIfStalled(
                    awaitProgress(start, "draining the in-flight publishes"),
                    "draining the in-flight publishes");
        }
        failureCheck.run();
    }

    /**
     * Returns how many publishes are not yet acknowledged.
     *
     * @return the in-flight message count
     */
    int getInFlightMessages() {
        return inFlightMessages;
    }

    /**
     * Returns the serialized size of the publishes not yet acknowledged.
     *
     * @return the in-flight byte total
     */
    long getInFlightBytes() {
        return inFlightBytes;
    }

    /**
     * Says once, per warn interval, that a wait has stopped making progress — long before the
     * budget that ends it.
     *
     * <p>The counters an operator watches cannot report this state: no publish is resolving, which
     * is what the state <em>is</em>, so {@code errorClass.*.errors} and {@code
     * numRecordsSendErrors} stay where they were for its whole duration. Without this line the
     * first thing anyone sees is the job dying — at the shipped default ten minutes later, and
     * possibly of Flink's checkpoint timeout instead, which names nothing about Pub/Sub.
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
        log.warn(
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
     * Runs one mailbox mail, failing if nothing has completed a publish for {@code
     * publishProgressTimeout}.
     *
     * <p>What is bounded is a <b>stall, not a slow topic</b>: {@code lastCompletionNanos} is
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
     * progress, or {@code RAN_A_MAIL} if it ran one instead — which is what tells {@link
     * #awaitCapacity()} the mailbox is empty, so that the flush and the warning are worth doing.
     * The caller owns the once-per-wait flag for the flush, because {@link #awaitCapacity()} is on
     * the record path and a state object here would be an allocation per record.
     */
    private long awaitProgress(long waitStartNanos, String what)
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
}
