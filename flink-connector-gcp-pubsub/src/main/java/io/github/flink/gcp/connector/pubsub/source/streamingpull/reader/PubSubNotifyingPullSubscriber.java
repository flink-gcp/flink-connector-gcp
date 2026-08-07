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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
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
import java.util.function.Consumer;

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
 * <p>The client's three lifecycle operations are held as functional values rather than as a {@link
 * Subscriber}, <b>because that is the only seam a test can drive</b> (#325). {@code Subscriber} is
 * a non-final class whose only constructor is private, so it cannot be subclassed to misbehave —
 * the same mechanism that makes {@code Publisher} unfakeable for {@code BoundedShutdown}, and the
 * same effect, by package-private access rather than private, for the {@code BigtableDataClient}
 * behind Bigtable's batcher adapter. This class's failure paths all need a client that misbehaves:
 * one that fails to start, one that never terminates, and one that reports at teardown a failure
 * this subscriber has already delivered. None of them was reachable before, since the emulator and
 * gated ITCases exercise only a client that works.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * with a caller-supplied data-available signal in place of per-subscriber notification futures.
 */
@Internal
public class PubSubNotifyingPullSubscriber implements NotifyingPullSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubNotifyingPullSubscriber.class);

    /**
     * Registers a permanent-failure sink with the client and starts it, returning once it is
     * running.
     *
     * <p>Takes the sink rather than an SDK {@code ApiService.Listener} so no vendor type reaches
     * this seam: what this class needs from the listener is the {@link Throwable}, and a test
     * driving a failure should not have to build a listener to deliver one.
     */
    @FunctionalInterface
    interface SubscriberStart {
        void start(Consumer<Throwable> onPermanentFailure);
    }

    /**
     * The client's own bounded wait for termination, satisfied by {@code
     * Subscriber::awaitTerminated}.
     *
     * <p>Not {@code BoundedShutdown.TerminationWait}, whose shape is {@code boolean await(...)
     * throws InterruptedException} against this one's {@code void ... throws TimeoutException}.
     */
    @FunctionalInterface
    interface TerminationWait {
        void await(long timeout, TimeUnit unit) throws TimeoutException;
    }

    private final String splitId;
    private final SubscriptionDestination subscription;
    private final AckTracker ackTracker;
    private final Runnable dataAvailableSignal;
    private final Duration shutdownTimeout;
    private final Runnable subscriberStopAsync;
    private final TerminationWait subscriberAwaitTerminated;

    @GuardedBy("this")
    private final Deque<PubsubMessage> messages = new ArrayDeque<>();

    @GuardedBy("this")
    @Nullable
    private Throwable permanentError;

    /**
     * Whether {@link #permanentError} has been handed to a caller, which is what {@link
     * #awaitTerminated} needs to know and is not the same question as whether it has been recorded.
     */
    @GuardedBy("this")
    private boolean permanentErrorReported;

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
        Subscriber subscriber = subscriberFactory.create(subscription, this::receiveMessage);
        this.subscriberStopAsync = subscriber::stopAsync;
        this.subscriberAwaitTerminated = subscriber::awaitTerminated;
        // The client is created here rather than in a this(...) delegation because the receiver it
        // takes is this::receiveMessage, which does not exist until the instance does. So the two
        // constructors assign the same fields instead of one calling the other, and share only what
        // has to be shared: the start, and what it does when it fails.
        startOrRelease(
                onPermanentFailure ->
                        registerFailureListenerAndStart(subscriber, onPermanentFailure));
    }

    /**
     * The seam. Takes the client's three lifecycle operations directly, so a test can supply ones
     * that fail; see the class javadoc for why a fake {@link Subscriber} is not an option.
     */
    @VisibleForTesting
    PubSubNotifyingPullSubscriber(
            String splitId,
            SubscriptionDestination subscription,
            AckTracker ackTracker,
            Runnable dataAvailableSignal,
            Duration shutdownTimeout,
            SubscriberStart subscriberStart,
            Runnable subscriberStopAsync,
            TerminationWait subscriberAwaitTerminated)
            throws IOException {
        this.splitId = splitId;
        this.subscription = subscription;
        this.ackTracker = ackTracker;
        this.dataAvailableSignal = dataAvailableSignal;
        this.shutdownTimeout = shutdownTimeout;
        this.subscriberStopAsync = subscriberStopAsync;
        this.subscriberAwaitTerminated = subscriberAwaitTerminated;
        startOrRelease(subscriberStart);
    }

    /**
     * The production wiring, and the one thing here no unit test reaches — stated so it is not
     * mistaken for pinned. Every test drives the seam constructor, so nothing checks that the
     * listener is registered <em>before</em> the start (a failure raised in between would be lost,
     * and the split would then stall silently rather than fail), that the executor is a direct one,
     * or that the two teardown operations bind the client this starts. A fake {@link Subscriber} is
     * what it would take, which is the thing that cannot be built.
     */
    private static void registerFailureListenerAndStart(
            Subscriber subscriber, Consumer<Throwable> onPermanentFailure) {
        subscriber.addListener(
                new ApiService.Listener() {
                    @Override
                    public void failed(ApiService.State from, Throwable failure) {
                        onPermanentFailure.accept(failure);
                    }
                },
                // A direct executor: recording the failure and waking the fetcher are both cheap.
                Runnable::run);
        subscriber.startAsync().awaitRunning();
    }

    /**
     * Starts the client, asking it to stop again if that fails.
     *
     * <p><b>This call is a no-op in the case it was written for, and the resources it was written
     * to release are released anyway — by the SDK, not by us.</b> Both halves measured on {@code
     * google-cloud-pubsub} 1.152.0 and Guava 33.5.0 (#325 found the first, #349 the second).
     *
     * <p>The no-op half: Guava's {@code stopAsync()} is guarded by {@code
     * state().compareTo(RUNNING) <= 0} and {@code FAILED} sorts last, so once the service has
     * failed, {@code stopAsync()} enters nothing and {@code doStop()} never runs. A failed start is
     * precisely what leaves it {@code FAILED}.
     *
     * <p>The half that stops it being a leak: {@code Subscriber.startStreamingConnections()} adds
     * to every connection a listener whose {@code failed(...)} runs {@code runShutdown()} — {@code
     * stopAllStreamingConnections}, {@code shutdownBackgroundResources}, {@code
     * subscriberStub.shutdownNow()} — <em>before</em> it calls {@code notifyFailed}. A connection
     * that fails to start, or a stream that dies on a missing IAM grant, therefore releases the
     * stub, the executors and the background resources on the SDK's own path, whichever of the two
     * {@code notifyFailed} calls wins the race. {@code doStart()}'s other failure, {@code
     * GrpcSubscriberStub.create} throwing {@code IOException}, strands nothing either: it happens
     * before the stub exists.
     *
     * <p>What is left uncovered is narrow, and is not what the guard covers: a throw in the
     * <em>synchronous</em> part of {@code startStreamingConnections} — {@code
     * executorProvider.getExecutor()}, or building a connection — which happens before any
     * connection listener exists, so nothing runs {@code runShutdown()} and the stub plus any
     * executor already added stay open. Owning the channel and executor ourselves ({@code
     * Subscriber.Builder.setChannelProvider} / {@code setExecutorProvider}) would close it, and was
     * declined: it means taking over channel sizing, which the SDK derives from {@code
     * parallelPullCount}, to cover the one path out of four that the SDK does not cover itself.
     *
     * <p>So the call is kept for the states where {@code stopAsync()} is <em>not</em> a no-op — a
     * failure registering the listener leaves the service {@code NEW}, one racing a start in
     * progress leaves it {@code STARTING} — and because it costs a no-op otherwise.
     */
    private void startOrRelease(SubscriberStart subscriberStart) throws IOException {
        try {
            subscriberStart.start(this::fail);
        } catch (RuntimeException e) {
            releaseAfterFailedStart(e);
            throw new IOException(
                    "Failed to start the Pub/Sub subscriber for subscription " + subscription, e);
        } catch (Error e) {
            // An Error takes the same release, as in the sibling guard in
            // DefaultMutationBatcherFactory.create (#324): a client's first classload can fail
            // with a NoClassDefFoundError, which repeats on every restart attempt and would
            // otherwise walk past this. Rethrown unchanged rather than wrapped — an Error is not
            // an IOException, and Flink's escalation reads the type it is handed.
            releaseAfterFailedStart(e);
            throw e;
        }
    }

    /**
     * Through {@link Closers#closeAllSuppressing} rather than a bare call, as at the sibling site
     * (#324): a release that throws must be suppressed onto the failure being propagated, never
     * replace it. The SDK's own {@code stopAsync()} cannot throw — Guava catches {@code Throwable}
     * inside it — but this method's operations are injected, so what holds for the production
     * client is not what holds for the seam.
     */
    private void releaseAfterFailedStart(Throwable failure) {
        Closers.closeAllSuppressing(failure, this::stopQuietly);
    }

    /** Receives a message from the client library, on one of its callback threads. */
    private void receiveMessage(PubsubMessage message, AckHandle ackHandle) {
        synchronized (this) {
            if (closed || permanentError != null) {
                // Nack rather than drop: the message must go back for redelivery immediately.
                ackHandle.nack();
                return;
            }
            ackTracker.addPendingAck(splitId, message.getMessageId(), ackHandle);
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
            throwIfFailed();
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
    public void checkFailure() throws IOException {
        synchronized (this) {
            throwIfFailed();
        }
    }

    /**
     * The single place a permanent failure is turned into the exception the reader sees, so {@link
     * #pullMessages} and {@link #checkFailure} cannot come to report it differently.
     */
    @GuardedBy("this")
    private void throwIfFailed() throws IOException {
        if (permanentError != null) {
            // Recording that it was handed over is what lets awaitTerminated tell the client's
            // repeat of this failure from one nothing has consumed.
            permanentErrorReported = true;
            throw new IOException(
                    "The Pub/Sub subscriber for subscription " + subscription + " failed.",
                    permanentError);
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            // Buffered messages were never emitted; nackSplit below returns them for redelivery.
            messages.clear();
        }
        // One list rather than two calls, for the reason #297 gave the same shape one level up in
        // PubSubSplitReader.close(): closed is already true by the time these run, so the stop must
        // not be skipped when the nack throws. It would leave this method's idempotence guard
        // claiming a client had been asked to stop that had not, close() returning at that guard,
        // and awaitTerminated spending its whole budget on it — reported by nothing but a WARN
        // about an unclean shutdown. closeAll finishes the list before reporting, so the order the
        // SPI's javadoc argues for — the nack is what must not be skipped — survives a failure in
        // either step.
        try {
            Closers.closeAll(() -> ackTracker.nackSplit(splitId), subscriberStopAsync::run);
        } catch (Exception e) {
            // Neither step declares a checked exception, so this is the unchecked one closeAll
            // collected; an Error leaves closeAll as itself and never reaches here.
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void close() throws Exception {
        // A list here for the same reason shutdown() has one, one step further out: shutdown()
        // guarantees the client was asked to stop whatever else it did, so a shutdown that throws
        // leaves a stop in progress that still has to be waited out. The reader's own closeAll
        // covers that on its path, calling shutdown before close so close meets the idempotence
        // guard; removeSplit calls close() directly and has only this.
        Closers.closeAll(this::shutdown, this::awaitTerminated);
    }

    /**
     * Whether the given cause is the failure this subscriber has already handed to a caller.
     *
     * <p>Asks the question directly rather than through a proxy, which a first draft of this got
     * wrong twice over. Snapshotting {@link #permanentError} before the shutdown looks equivalent
     * and is not: on the reader's own close path every subscriber's {@link #shutdown()} runs before
     * any {@link #close()}, so the snapshot would be taken after {@code stopAsync()} — and a
     * failure the teardown itself produced in that window would be in it, and reported as a repeat
     * of something nobody had read. The narrower version of the same mistake is that "recorded"
     * never meant "consumed" anyway: a stream dying after the last {@link #pullMessages} but before
     * the shutdown is recorded and read by nothing.
     */
    private synchronized boolean isAlreadyReported(@Nullable Throwable cause) {
        return permanentErrorReported && cause == permanentError;
    }

    /**
     * Stops the client, tolerating a failure. Shutdown is best-effort: everything this split owned
     * has already been nacked by the time this runs, so a client that lingers costs resources until
     * the JVM exits but loses nothing.
     *
     * <p>Deliberately <em>not</em> a {@link Closers#closeAll} list, unlike {@link #shutdown()} and
     * {@link #close()}: there, a step that throws skips one that is still needed, while here a stop
     * that threw has started no shutdown for the wait to wait out. Skipping it is the right
     * outcome, not a defect in the same shape.
     *
     * <p>It has its own absorb rather than {@link #awaitTerminated()}'s, because both of that
     * method's messages are <em>false</em> here. There is no repeat to identify — {@link
     * #startOrRelease} is propagating the start failure as this runs, so everything the client
     * raises is part of a failure already going to the caller, and asking {@link
     * #isAlreadyReported} would be a race besides: Guava dispatches the failure listener from the
     * SDK's own thread <em>after</em> leaving the monitor {@code awaitRunning()} is blocked on, so
     * this can run before {@link #permanentError} is written. And nothing has been nacked, because
     * {@link #shutdown()} never ran; that message's "nothing is lost" holds anyway, for the
     * different reason that a client which never started received nothing.
     */
    private void stopQuietly() {
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
     * rather than merely tolerable — <b>the client reports here a failure this subscriber has
     * already delivered</b>. {@code Subscriber} extends gax's {@code AbstractApiService}, which
     * holds a Guava {@code AbstractService} as a private inner field (redeclared so Guava can be
     * shaded, so no Guava type is catchable here); {@code awaitTerminated} ends in that class's
     * {@code checkCurrentState(TERMINATED)}, which on a {@code FAILED} service throws {@code
     * IllegalStateException} carrying {@code failureCause()} — the very {@link Throwable} the
     * failure listener already recorded as {@link #permanentError} and {@link #pullMessages}
     * already reported, wrapped in an {@link IOException}. Propagating it would report one failure
     * twice — the contract {@link NotifyingPullSubscriber#close()} states, and the repository-wide
     * rule the root {@code CLAUDE.md} carries (#325). Measured on {@code google-cloud-pubsub}
     * 1.152.0, api-common 2.65.0 and Guava 33.5.0.
     *
     * <p><b>The catch is wider than that one case, and the other two are not repeats of
     * anything</b> (#351). The wait has exactly three exits: the {@code FAILED} rethrow above, a
     * {@code TimeoutException}, and a {@code checkCurrentState} mismatch that cannot happen (the
     * wait returns only on {@code TERMINATED} or {@code FAILED}, and both are handled). A timeout
     * is absorbed on the older best-effort ground, that this split's messages are already nacked. A
     * failure the SDK raises <em>during</em> this teardown — from the thread {@code doStop()}
     * spawns, whose {@code catch (Exception e) { notifyFailed(e); }} moves the service to {@code
     * FAILED} carrying a brand-new cause — arrives as that same {@code IllegalStateException}, sets
     * {@link #permanentError} with nothing left to read it, and is absorbed too. All three are
     * absorbed and each says which it is, because they mean different things to whoever reads the
     * log: one is expected on a failing teardown, one means no job failure is coming.
     *
     * <p><b>Telling the first from the third is an identity comparison, and it is sound because the
     * SDK keeps the first cause.</b> Guava's {@code notifyFailed} does nothing on an already-{@code
     * FAILED} service, and {@code stopAsync()} on one enters nothing — its {@code isStoppable}
     * guard is {@code state().compareTo(RUNNING) <= 0} and {@code FAILED} sorts last — so {@code
     * doStop()} runs only for a service that was healthy when {@link #shutdown()} did, and the
     * cause reaching here is the one recorded first either way. A cause that is the {@code
     * alreadyReported} snapshot is therefore the re-report; anything else was produced after the
     * reader stopped pulling and has been consumed by nothing. Measured on {@code
     * google-cloud-pubsub} 1.152.0 and Guava 33.5.0.
     *
     * <p>A genuine streaming failure that lands between the snapshot and the wait is classified as
     * the third case, not the first, and that is correct by the property being tested: {@link
     * #pullMessages} will not be called again, so nothing consumes it either.
     *
     * <p>What the third case costs is <em>promptness</em>, not messages. {@code runShutdown()}
     * begins with {@code stopAllStreamingConnections}, which is what flushes to the service the
     * nacks {@link AckTracker#nackSplit} has just enqueued; a failure before that flush leaves
     * those messages to wait out their acknowledgement deadline rather than being redelivered at
     * once — the property #118 settled as one this connector asserts.
     */
    private void awaitTerminated() {
        try {
            subscriberAwaitTerminated.await(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
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
            if (isAlreadyReported(e.getCause())) {
                LOG.warn(
                        "The Pub/Sub subscriber for subscription {} reported at shutdown the"
                                + " failure it had already reported to the reader. The reader is"
                                + " failing the job on that one, so this is not raised again.",
                        subscription,
                        e);
            } else {
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
