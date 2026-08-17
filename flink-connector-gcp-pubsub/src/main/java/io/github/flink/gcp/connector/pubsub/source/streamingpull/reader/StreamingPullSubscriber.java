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
 * {@link PullSubscriber} backed by a {@code google-cloud-pubsub} {@link Subscriber}.
 *
 * <p>Received messages are appended to a buffer <em>synchronously inside the receiver
 * callback</em>, which is what preserves ordering-key order: for an ordering-enabled subscription
 * the client library only dispatches the next message of a key once the previous callback has
 * returned, so buffer order equals delivery order per key. It also means the callback never blocks
 * on acknowledgement, which happens a whole checkpoint later — the client library's per-key
 * serialization waits for the callback to return, not for the acknowledgement.
 *
 * <p><b>The buffer is deliberately unbounded, and its bound is elsewhere.</b> The two things a
 * bounded buffer could do here are both ruled out by the paragraph above: blocking inside the
 * callback would stall the key's dispatch chain and hold a client-library thread, and refusing a
 * message means nacking one already leased. So backpressure comes from the client library's flow
 * control, which stops pulling once its outstanding limit fills — <em>for one {@code
 * maxAckExtensionPeriod}</em>. Past it the client releases a message's flow-control permit while
 * this subscriber is still holding the message, and pulling resumes with nothing capping what
 * accumulates (#357, measured by {@code PubSubPausedSplitBufferITCase}). What bounds it after that
 * is {@link SubscriberRoster}, which reads {@link #bufferUsage()} and stops the client of a paused
 * split that has outgrown it.
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
 * <p>The teardown itself — the bounded waits, the three-exit classification and the four {@code
 * WARN} messages whose fragments the documentation quotes verbatim (#359) — is {@link
 * SubscriberTeardown}'s, handed a live already-reported predicate that reads this class's latch
 * under this class's monitor (ADR-0012).
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * with a caller-supplied data-available signal in place of per-subscriber notification futures.
 */
@Internal
public class StreamingPullSubscriber implements PullSubscriber {

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
    private final Runnable subscriberStopAsync;
    private final SubscriberTeardown teardown;

    @GuardedBy("this")
    private final Deque<PubsubMessage> messages = new ArrayDeque<>();

    /**
     * The serialized size of everything in {@code messages}, maintained alongside it because
     * summing on demand is O(n) on a buffer whose whole problem is growing large.
     */
    @GuardedBy("this")
    private long bufferedBytes;

    @GuardedBy("this")
    @Nullable
    private Throwable permanentError;

    /**
     * Whether {@code permanentError} has been handed to a caller, which is what {@link
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
    public StreamingPullSubscriber(
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
        Subscriber subscriber = subscriberFactory.create(subscription, this::receiveMessage);
        this.subscriberStopAsync = subscriber::stopAsync;
        // this::isAlreadyReported leaks a half-built instance, harmlessly: the teardown stores the
        // predicate and calls it only from stopQuietly/awaitTerminated, never during construction.
        this.teardown =
                new SubscriberTeardown(
                        subscription,
                        shutdownTimeout,
                        subscriber::stopAsync,
                        subscriber::awaitTerminated,
                        this::isAlreadyReported);
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
    StreamingPullSubscriber(
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
        this.subscriberStopAsync = subscriberStopAsync;
        this.teardown =
                new SubscriberTeardown(
                        subscription,
                        shutdownTimeout,
                        subscriberStopAsync,
                        subscriberAwaitTerminated,
                        this::isAlreadyReported);
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
        Closers.closeAllSuppressing(failure, teardown::stopQuietly);
    }

    /**
     * Receives a message from the client library, on one of its callback threads.
     *
     * <p>Package-private rather than private so the buffer accounting can be driven without a
     * client: this is the client library's callback, and the seam constructor takes the client's
     * lifecycle operations only, never its receiver.
     */
    @VisibleForTesting
    void receiveMessage(PubsubMessage message, AckHandle ackHandle) {
        synchronized (this) {
            if (closed || permanentError != null) {
                // Nack rather than drop: the message must go back for redelivery immediately.
                ackHandle.nack();
                return;
            }
            ackTracker.addPendingAck(splitId, message.getMessageId(), ackHandle);
            messages.addLast(message);
            bufferedBytes += message.getSerializedSize();
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
                PubsubMessage message = messages.pollFirst();
                bufferedBytes -= message.getSerializedSize();
                drained.add(message);
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

    @Override
    public synchronized BufferUsage bufferUsage() {
        return BufferUsage.of(messages.size(), bufferedBytes);
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
            bufferedBytes = 0L;
        }
        // One list rather than two calls, for the reason #297 gave the same shape one level up in
        // SubscriberRoster.closeAll(): closed is already true by the time these run, so the stop
        // must not be skipped when the nack throws. It would leave this method's idempotence guard
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
        Closers.closeAll(this::shutdown, teardown::awaitTerminated);
    }

    /**
     * Whether the given cause is the failure this subscriber has already handed to a caller.
     *
     * <p>Asks the question directly rather than through a proxy, which a first draft of this got
     * wrong twice over. Snapshotting {@code permanentError} before the shutdown looks equivalent
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
}
