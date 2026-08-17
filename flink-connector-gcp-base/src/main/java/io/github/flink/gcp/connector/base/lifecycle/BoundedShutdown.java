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

package io.github.flink.gcp.connector.base.lifecycle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * The teardown of one client whose own shutdown cannot be trusted to return: both of its steps on a
 * separate thread, and one deadline that the calling thread's single {@code join} is the whole of.
 *
 * <p>The bound is the point. Written for the {@code google-cloud-pubsub} {@code Publisher}, whose
 * {@code shutdown()} waits on a counter of accepted publishes, uninterruptibly and with no timeout,
 * until it is exactly zero — and which both the Pub/Sub sink's per-topic publishers and {@code
 * PubSubDeadLetterQueue} own.
 *
 * <p>That counter can be left permanently above zero: the failure callback cancels the messages
 * still accumulating in a failed ordering key's un-flushed batch and removes the batch, but
 * decrements only by the size of the batch that was in flight, so those increments are never
 * returned (measured on {@code google-cloud-pubsub} 1.152.0; issue #265).
 *
 * <p>And it can simply take arbitrarily long to reach zero, with nothing defective involved: with
 * {@code enableMessageOrdering} the SDK overrides the publisher's retry settings to {@code
 * maxAttempts = Integer.MAX_VALUE} and an effectively infinite total timeout, so during an outage
 * the in-flight publishes retry forever and the counter never drains. An ordered sink therefore
 * needs this bound whatever the SDK version, which is why it is not written as a workaround.
 *
 * <p>A separate thread is therefore the only lever available: the wait cannot be interrupted, and
 * {@code Publisher} offers no forcible variant. It is a daemon thread so one that never returns
 * cannot keep a JVM from exiting, and a plain thread rather than an executor because {@code
 * shutdownNow()} could not interrupt that wait either — the thread would leak just the same, and
 * the executor would then need a bounded teardown of its own.
 *
 * <p><b>The termination wait runs on that thread too</b>, rather than on the calling thread after a
 * successful join, and that placement is load-bearing: gax's {@code
 * BackgroundResourceAggregation.awaitTermination} passes the <em>full</em> duration to every
 * resource in turn (its own source carries the {@code TODO subtract time already used up from
 * previous resources}), and a publisher nests several — its executor, then the stub's transport
 * channel and watchdog. Awaiting on the calling thread would therefore cost a multiple of the
 * timeout, not the timeout. Here it costs the daemon thread's time and nothing else.
 *
 * <p>Anything either step throws is captured and rethrown by {@link #close()} with its own type,
 * because a thread's uncaught exception would otherwise reach only Flink's JVM-wide handler —
 * losing a teardown failure the caller used to report, and, under {@code
 * cluster.uncaught-exception-handling: FAIL}, turning it into a TaskManager exit.
 *
 * <p>The two steps are held as functional values rather than as a client, because the client this
 * was written for cannot be subclassed — {@code Publisher} is non-final, but its only constructor
 * is private, which forbids a subclass just as effectively (#324) — so this is the only seam a test
 * can drive.
 *
 * <p><b>{@link #start()} and {@link #close()} must be called from one thread</b> — the task thread,
 * for both users today. That precondition is what makes {@link #thread} safe as a plain field, and
 * it is enforced by nothing: two threads racing {@code start()} would each see a null {@code
 * thread} and run {@code shutdown} twice, on two daemon threads. A guard was weighed and left out —
 * the callers are writer teardowns, which Flink runs on the task thread by construction — so a
 * third consumer has to honour it deliberately.
 *
 * <p><b>The budget must be expressible in nanoseconds</b> — at most {@code
 * Duration.ofNanos(Long.MAX_VALUE)}, about 292 years — because nanoseconds are the arithmetic the
 * clock does. The constructor rejects a longer one, rather than leaving {@link Duration#toNanos()}
 * to throw {@link ArithmeticException} from {@link #start()}: that would land on a TaskManager
 * during a teardown, where it reaches Flink's teardown path and not a caller's {@code try}. Every
 * option setter that feeds a budget here rejects the same value, so a user meets it on the client;
 * this is the backstop for a consumer whose budget is built in code and passes no setter (#334). A
 * budget at that ceiling is a real budget — see {@link #remainingNanos()}, whose arithmetic looks
 * wrong there and is not.
 *
 * <p>The threading of the remaining mutable state, stated precisely because the class is shared:
 *
 * <ul>
 *   <li>{@link #deadlineNanos} is read by <em>both</em> threads ({@link #close()} and {@link
 *       #shutdownAndAwait()}), so it is not confined. It is safe as a plain {@code long} because
 *       its single write happens before {@code Thread.start()} and the idempotence guard means it
 *       never happens again — publication, not confinement. A write added after the thread starts
 *       would be a data race.
 *   <li>{@link #abandoned} is written by the calling thread and read by the shutdown thread with no
 *       synchronisation edge between them, so it genuinely needs {@code volatile}.
 *   <li>{@link #failure} is written by the shutdown thread and read by {@link #close()} only after
 *       {@code thread.isAlive()} has returned false, which is itself a happens-before edge (JLS
 *       17.4.5), so a plain field would suffice. It is {@code volatile} as belt and braces, since
 *       the read is one early-return away from being unordered.
 * </ul>
 */
@Internal
public final class BoundedShutdown implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedShutdown.class);

    /** The client's own bounded wait, satisfied by e.g. {@code Publisher::awaitTermination}. */
    @FunctionalInterface
    public interface TerminationWait {
        boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    }

    private final Runnable shutdown;
    private final TerminationWait awaitTermination;
    private final String description;
    @Nullable private final Runnable release;
    private final Duration timeout;
    private final LongAdder abandonedCount;

    @Nullable private Thread thread;
    private long deadlineNanos;
    @Nullable private volatile Throwable failure;

    /** Set once {@link #close()} has stopped waiting, so a later failure knows to report itself. */
    private volatile boolean abandoned;

    /**
     * Set by the first {@link #close()}, so a second one does nothing. {@code AutoCloseable}
     * strongly encourages idempotence and neither caller relies on the alternative: without this, a
     * second close re-runs {@link #release}, rethrows the same captured failure, and re-emits the
     * give-up warning. Calling-thread only, like {@link #thread}.
     */
    private boolean closed;

    /**
     * Creates the teardown. Nothing runs until {@link #start()} or {@link #close()}.
     *
     * @param shutdown the client's own shutdown call, which may never return
     * @param awaitTermination its bounded wait for the resources behind it
     * @param description what is being shut down, for the thread name and the give-up warnings.
     *     Name the kind as well as the resource, so one client is distinguishable from another in a
     *     log line the class itself cannot qualify — the two callers pass {@code "topic
     *     my-project/events"} and {@code "dead-letter topic my-project/dead-letters"}
     * @param release a resource released in {@code close()}'s {@code finally} whatever happened,
     *     including on the give-up path, or {@code null}; the caller's owned transport channel is
     *     what this is for. A {@link Runnable} rather than an {@link AutoCloseable} deliberately:
     *     it runs in a {@code finally}, where anything it threw would replace the failure being
     *     propagated, so this is for a release that does not fail — {@code
     *     ManagedChannel.shutdownNow()} is the one it was written for. A resource whose release can
     *     fail belongs in the caller's own {@link Closers#closeAll(AutoCloseable...)} list beside
     *     this one instead — the shape {@code PubSubDeadLetterQueue.close()} takes.
     * @param timeout the whole budget, measured from {@link #start()}; at most {@code
     *     Duration.ofNanos(Long.MAX_VALUE)}, which is checked — a non-positive one is not, and
     *     gives up at once, the callers' own setters being where positivity is refused
     * @param abandonedCount incremented once whenever {@link #close()} gives up, so the owner can
     *     report the residue. <b>Supplied by the caller rather than held here, and that is the
     *     design</b>: the count has to outlive the task to be observable at all (measured — a
     *     reporter at 10 ms never sees a metric a writer only touches in {@code close()}), so it is
     *     process-wide wherever it lives. Keeping it here would make one number out of every client
     *     this class ever serves, and a metric named for one of them — {@code
     *     publisherShutdownsAbandoned} — would silently include the rest. The nearest such client
     *     is not another connector but the Pub/Sub <em>source</em>, whose subscriber teardown has
     *     the same shape. Each owner passing its own keeps the names true by construction
     */
    public BoundedShutdown(
            Runnable shutdown,
            TerminationWait awaitTermination,
            String description,
            @Nullable Runnable release,
            Duration timeout,
            LongAdder abandonedCount) {
        this.shutdown = shutdown;
        this.awaitTermination = awaitTermination;
        this.description = description;
        this.release = release;
        // Checked here rather than left to start(), where toNanos() would throw
        // ArithmeticException instead — on a TaskManager, out of a teardown, where it reaches
        // Flink's teardown path and not a caller's try. Every setter feeding a budget here
        // rejects the same value, so a user meets it on the client; this covers the consumer
        // whose budget is built in code and passes no setter (#334; ADR-0068).
        this.timeout = OptionChecks.checkExpressibleInNanos(timeout, "timeout");
        // Checked here, not where it is used: the one increment sits on the give-up path, ahead of
        // the warning that explains it, so a null would turn a bounded and logged give-up into an
        // NPE out of close() with the diagnostic swallowed — during exactly the outage this class
        // exists to survive, and only then. A consumer that does not want to count passes a
        // throwaway LongAdder; there is no null shorthand.
        this.abandonedCount =
                Preconditions.checkNotNull(abandonedCount, "abandonedCount must not be null");
    }

    /** The budget, for a test that checks which one its caller handed over. */
    @VisibleForTesting
    public Duration timeout() {
        return timeout;
    }

    /**
     * The counter this teardown was handed, so a caller's wiring can be asserted by identity rather
     * than by driving a give-up and observing an increment — which is a footrace against the
     * freshly started thread, not a deterministic test. Widened for that reason specifically, per
     * the rule beside {@link #timeout()}'s justification in the module's detailed guidance.
     */
    @VisibleForTesting
    public LongAdder abandonedCounter() {
        return abandonedCount;
    }

    /**
     * Returns the budget the timeout has not yet used, in nanoseconds; never negative.
     *
     * <p>Correct at the largest budget this class accepts, where {@link #deadlineNanos} has
     * overflowed: this subtraction wraps a second time and the two cancel, leaving the true
     * remainder — measured rather than argued, and pinned by {@code
     * theLargestExpressibleBudgetIsNotSpentTheInstantItStarts}. Do not "harden" the stamp in {@link
     * #start()} with {@code Math.addExact} or a saturating add: the first turns a legal budget into
     * an exception, the second changes nothing, and both are what that test guards against.
     */
    private long remainingNanos() {
        return Math.max(deadlineNanos - System.nanoTime(), 0);
    }

    /**
     * Starts the client's teardown and the clock, without waiting for either. Idempotent, and
     * deliberately does not restart the clock: a caller owning several clients starts every
     * teardown before it closes any, and a second call resetting the deadline would turn its one
     * timeout back into one per client.
     */
    public void start() {
        if (thread != null) {
            return;
        }
        deadlineNanos = System.nanoTime() + timeout.toNanos();
        // Named after the caller as well as the resource, because the caller is the task thread
        // and Flink names it "Sink: Writer (2/4)#1" — without that, every subtask on a
        // TaskManager writing the same topic leaves identically-named threads behind, and a
        // thread dump cannot say which subtask leaked or on which attempt. Flink's own
        // SplitFetcherManager names fetcher threads the same way.
        thread =
                new Thread(
                        this::shutdownAndAwait,
                        "bounded-shutdown-"
                                + description
                                + " for "
                                + Thread.currentThread().getName());
        thread.setDaemon(true);
        thread.start();
    }

    private void shutdownAndAwait() {
        try {
            shutdown.run();
            long remainingNanos = remainingNanos();
            if (!awaitTermination.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                LOG.warn(
                        "The client for {} shut down but its resources did not terminate within the"
                                + " {} of its {} shutdown budget that were left; they may leak"
                                + " until the JVM exits.",
                        description,
                        Duration.ofNanos(remainingNanos),
                        timeout);
            }
        } catch (Throwable t) {
            failure = t;
            if (abandoned) {
                // Nothing will read the field: close() already gave up and returned, so this
                // is the only report this failure will ever get. Reachable in practice — a
                // thread outliving its job meets a closed user classloader.
                LOG.warn(
                        "The client for {} failed to shut down, after its close had already given"
                                + " up waiting for it.",
                        description,
                        t);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try {
            start();
            long waitedNanos = remainingNanos();
            if (waitedNanos > 0) {
                // Milliseconds rounded up, so a sub-millisecond remainder never reaches
                // join(0) — which waits forever, the very thing this class exists to bound.
                thread.join(1 + TimeUnit.NANOSECONDS.toMillis(waitedNanos));
            }
            if (thread.isAlive()) {
                abandoned = true;
                abandonedCount.increment();
                // The waited time, not the configured budget: it is shared across every
                // client the caller owns, so one after another that hung gets none of it and
                // would otherwise report "did not finish within 30s" having waited nothing —
                // which reads as "raise the timeout" when the answer is elsewhere.
                //
                // No issue link here any more. This class serves more than one client, and the
                // SDK defect the sink's publishers can hit (#265) is unreachable for the
                // dead-letter queue's — it sets no ordering key and never enables ordering, so
                // pointing its operator at that issue sends them after a cause they cannot have.
                // The mechanism belongs on the connector's own page, which the description names
                // well enough to find.
                LOG.warn(
                        "The client for {} did not finish shutting down within the {} of its {}"
                                + " shutdown budget that were left; it is left to a background"
                                + " thread, and its resources leak until the JVM exits. Raising"
                                + " that budget is the knob; the connector's documentation page"
                                + " explains what an unfinished shutdown means for it.",
                        description,
                        Duration.ofNanos(waitedNanos),
                        timeout);
                return;
            }
            Throwable captured = failure;
            if (captured != null) {
                // Rethrown with its own type: Closers.closeAll relies on that, and a wrapped
                // Error is a different thing to Flink's Task.preProcessException.
                ExceptionUtils.rethrowException(captured);
            }
        } catch (InterruptedException e) {
            // Restored before it propagates: Closers.closeAll collects and carries on, and the
            // join cleared the flag, so without this the rest of the caller's teardown — the
            // other clients, the admin, the failure handler — would stop honouring the
            // cancellation that interrupted us.
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            if (release != null) {
                release.run();
            }
        }
    }
}
