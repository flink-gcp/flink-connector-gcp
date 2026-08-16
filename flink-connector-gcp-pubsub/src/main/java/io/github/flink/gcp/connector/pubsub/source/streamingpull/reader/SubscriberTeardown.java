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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * The bounded, absorbing tail of a subscriber's life: waits out the client's shutdown, classifies
 * whatever the wait raises, and reports it to the log alone (ADR-0012).
 *
 * <p>Best-effort by design: everything the split owned has been nacked before either wait runs, so
 * a client that lingers costs resources until the JVM exits but loses nothing. What the
 * classification protects is the reader's failure report — the client re-raises at shutdown the
 * failure the subscriber already delivered, and propagating it would report one failure twice,
 * which {@link PullSubscriber#close()} forbids (#325).
 *
 * <p>The already-reported question is asked <b>live</b>, through a predicate that reads the
 * subscriber's latch under the subscriber's own monitor at classification time. ADR-0012 records
 * why a snapshot taken before the shutdown looks equivalent and is wrong twice over: on the
 * reader's close path every subscriber's {@code shutdown()} runs before any {@code close()}, so the
 * snapshot would be taken after {@code stopAsync()} — and a failure the teardown itself produced in
 * that window would be in it, reported as a repeat of something nobody had read.
 */
@Internal
final class SubscriberTeardown {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriberTeardown.class);

    private final SubscriptionDestination subscription;
    private final Duration shutdownTimeout;
    private final Runnable subscriberStopAsync;
    private final StreamingPullSubscriber.TerminationWait subscriberAwaitTerminated;
    private final Predicate<Throwable> isAlreadyReported;

    /**
     * Creates the teardown.
     *
     * @param subscription the subscription, named in every warning
     * @param shutdownTimeout how long either wait gives the client
     * @param subscriberStopAsync the client's {@code stopAsync}
     * @param subscriberAwaitTerminated the client's own bounded wait for termination
     * @param isAlreadyReported whether a cause is the failure the subscriber already handed to a
     *     caller — a live read under the subscriber's monitor, never a snapshot (ADR-0012)
     */
    SubscriberTeardown(
            SubscriptionDestination subscription,
            Duration shutdownTimeout,
            Runnable subscriberStopAsync,
            StreamingPullSubscriber.TerminationWait subscriberAwaitTerminated,
            Predicate<Throwable> isAlreadyReported) {
        this.subscription = subscription;
        this.shutdownTimeout = shutdownTimeout;
        this.subscriberStopAsync = subscriberStopAsync;
        this.subscriberAwaitTerminated = subscriberAwaitTerminated;
        this.isAlreadyReported = isAlreadyReported;
    }

    /**
     * Stops the client, tolerating a failure. Used only beneath a failed start (#349): everything
     * the client raises here is part of a failure already going to the caller.
     *
     * <p>Deliberately <em>not</em> a {@code Closers.closeAll} list, unlike the subscriber's {@code
     * shutdown()} and {@code close()}: there, a step that throws skips one that is still needed,
     * while here a stop that threw has started no shutdown for the wait to wait out. Skipping it is
     * the right outcome, not a defect in the same shape.
     *
     * <p>It has its own absorb rather than {@link #awaitTerminated()}'s, because both of that
     * method's messages are <em>false</em> here. There is no repeat to identify — the start failure
     * is propagating as this runs, so everything the client raises is part of a failure already
     * going to the caller, and asking {@code isAlreadyReported} would be a race besides: Guava
     * dispatches the failure listener from the SDK's own thread <em>after</em> leaving the monitor
     * {@code awaitRunning()} is blocked on, so this can run before the latch is written. And
     * nothing has been nacked, because {@code shutdown()} never ran; that message's "nothing is
     * lost" holds anyway, for the different reason that a client which never started received
     * nothing.
     *
     * <p><b>Counted by nothing either</b> (#358), which is the same argument once more: the start
     * failure this runs beneath is an {@code IOException} on its way to failing the job, so an
     * expiry here is a footnote to something already reported, not a signal of its own. What it
     * would otherwise report — resources stranded by a client that never ran — the SDK releases
     * itself on three of this path's four routes (#349).
     */
    void stopQuietly() {
        subscriberStopAsync.run();
        try {
            subscriberAwaitTerminated.await(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | RuntimeException e) {
            LOG.warn(
                    "The Pub/Sub subscriber for subscription {} did not shut down cleanly after"
                            + " failing to start. Nothing is lost — it received no messages — and"
                            + " this is only the release of a client that never ran. The start"
                            + " failure itself follows this line rather than preceding it, and is"
                            + " the one to read.",
                    subscription,
                    e);
        }
    }

    /**
     * Waits out the client's shutdown, reporting anything it raises to the log alone.
     *
     * <p>Best-effort as {@link #stopQuietly()} describes, and — the reason this absorb is required
     * rather than merely tolerable — <b>the client reports here a failure the subscriber has
     * already delivered</b>. {@code Subscriber} extends gax's {@code AbstractApiService}, which
     * holds a Guava {@code AbstractService} as a private inner field (redeclared so Guava can be
     * shaded, so no Guava type is catchable here); {@code awaitTerminated} ends in that class's
     * {@code checkCurrentState(TERMINATED)}, which on a {@code FAILED} service throws {@code
     * IllegalStateException} carrying {@code failureCause()} — the very {@link Throwable} the
     * failure listener already recorded and {@code pullMessages} already reported, wrapped in an
     * {@code IOException}. Propagating it would report one failure twice — the contract {@link
     * PullSubscriber#close()} states, and the repository-wide rule the detailed repository guidance
     * carries (#325). Measured on {@code google-cloud-pubsub} 1.152.0, api-common 2.65.0 and Guava
     * 33.5.0.
     *
     * <p><b>The catch is wider than that one case, and the other two are not repeats of
     * anything</b> (#351). The wait has exactly three exits: the {@code FAILED} rethrow above, a
     * {@code TimeoutException}, and a {@code checkCurrentState} mismatch that cannot happen (the
     * wait returns only on {@code TERMINATED} or {@code FAILED}, and both are handled). A timeout
     * is absorbed on the older best-effort ground, that this split's messages are already nacked. A
     * failure the SDK raises <em>during</em> this teardown — from the thread {@code doStop()}
     * spawns, whose {@code catch (Exception e) { notifyFailed(e); }} moves the service to {@code
     * FAILED} carrying a brand-new cause — arrives as that same {@code IllegalStateException},
     * latches in the subscriber with nothing left to read it, and is absorbed too. All three are
     * absorbed and each says which it is, because they mean different things to whoever reads the
     * log: one is expected on a failing teardown, one means no job failure is coming.
     *
     * <p><b>Telling the first from the third is an identity comparison, and it is sound because the
     * SDK keeps the first cause.</b> Guava's {@code notifyFailed} does nothing on an already-{@code
     * FAILED} service, and {@code stopAsync()} on one enters nothing — its {@code isStoppable}
     * guard is {@code state().compareTo(RUNNING) <= 0} and {@code FAILED} sorts last — so {@code
     * doStop()} runs only for a service that was healthy when {@code shutdown()} did, and the cause
     * reaching here is the one recorded first either way. A cause the subscriber has handed to a
     * caller is therefore the re-report; anything else was produced after the reader stopped
     * pulling and has been consumed by nothing. Measured on {@code google-cloud-pubsub} 1.152.0 and
     * Guava 33.5.0.
     *
     * <p>A genuine streaming failure that lands between the report and the wait is classified as
     * the third case, not the first, and that is correct by the property being tested: {@code
     * pullMessages} will not be called again, so nothing consumes it either.
     *
     * <p>What the third case costs is <em>promptness</em>, not messages. {@code runShutdown()}
     * begins with {@code stopAllStreamingConnections}, which is what flushes to the service the
     * nacks {@link AckTracker#nackSplit} has just enqueued; a failure before that flush leaves
     * those messages to wait out their acknowledgement deadline rather than being redelivered at
     * once — the property #118 settled as one this connector asserts.
     *
     * <p><b>Two of the three are counted, into {@link PubSubShutdownResidue}</b> (#358), because a
     * {@code WARN} is invisible to a log pipeline filtering below {@code ERROR} and to every
     * dashboard. Not the re-report: the reader is failing the job on that failure, so the count
     * would be a second series for an incident already reported by the loudest means there is.
     */
    void awaitTerminated() {
        try {
            subscriberAwaitTerminated.await(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.increment();
            LOG.warn(
                    "The Pub/Sub subscriber for subscription {} did not finish shutting down"
                            + " within {}. This split's messages were nacked before the wait, so"
                            + " nothing is lost; the shutdown it was asked for is still running"
                            + " and may yet finish, so the client may or may not keep its channel"
                            + " and threads until the JVM exits. Raise"
                            + " PubSubSubscriberOptions.shutdownTimeout(...) if this recurs,"
                            + " keeping it under source.reader.close.timeout.",
                    subscription,
                    shutdownTimeout,
                    e);
        } catch (RuntimeException e) {
            if (isAlreadyReported.test(e.getCause())) {
                LOG.warn(
                        "The Pub/Sub subscriber for subscription {} reported at shutdown the"
                                + " failure it had already reported to the reader. The reader is"
                                + " failing the job on that one, so this is not raised again.",
                        subscription,
                        e);
            } else {
                PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.increment();
                LOG.warn(
                        "The Pub/Sub subscriber for subscription {} failed while shutting down,"
                                + " and this is the only report of it: the failure arrived after"
                                + " the reader stopped pulling. This split's messages were nacked"
                                + " before the wait and are not lost, but the shutdown is what"
                                + " returns them to Pub/Sub, so redelivery may wait out their"
                                + " acknowledgement deadline instead of being immediate.",
                        subscription,
                        e);
            }
        }
    }
}
