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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * At-least-once writer sending one single-row request per record to the Bigtable tables a {@link
 * DestinationResolver} names, and discarding the answers.
 *
 * <h2>Threading model</h2>
 *
 * <p>The model is {@code BigtableWriter}'s: all logical state changes — the destination pool, the
 * in-flight ledger and the captured asynchronous error — happen on the task thread. A request's
 * completion callback runs on a client thread and does not mutate logical state; it stamps {@code
 * lastCompletionNanos} and re-dispatches a mail onto the {@link MailboxExecutor}, whose mails run
 * on the task thread inside this writer's own waits. The waits run {@link
 * MailboxExecutor#tryYield()} and park rather than {@link MailboxExecutor#yield()}, which is what
 * lets them notice a stalled client and why they read the interrupt flag themselves ({@code
 * docs/adr/0078}).
 *
 * <h2>Destinations</h2>
 *
 * <p>A request names its table when it starts, so the writer holds per table only a lease on the
 * instance's client, taken on the table's first record and returned once the table has been idle
 * for {@code BigtableRequestOptions.destinationIdleTimeout}. The {@link SingleRowClientFactory}
 * owns one client per (project, instance) and shares it across that instance's tables; one writer
 * holds at most {@code BigtableRequestOptions.maxActiveInstances}, and opening another drains the
 * outstanding requests and evicts the least recently used instance.
 *
 * <p>The destination is resolved <em>before</em> the record is serialized, so a record the
 * serializer rejects is reported against the table it was headed for; the null-skip check still
 * sits ahead of the pool, so a skipped record opens no client. A resolver returning {@code null}
 * fails the write rather than reaching the failure handler: it is a configuration failure, not a
 * bad record, and routing it would let a dropping policy write nothing at all under a green job.
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless: it stores nothing in Flink state. {@link #flush(boolean)} runs at
 * every checkpoint barrier and blocks until every outstanding request is answered, so a successful
 * checkpoint means the service answered every request up to the barrier — applied, or refused at
 * the row level and routed. Checkpointing must be enabled in streaming jobs; without it {@code
 * flush()} never runs mid-stream. A record replayed after a restart is sent again, and neither RPC
 * is idempotent: a replayed {@code CheckAndMutateRow} re-evaluates its condition against whatever
 * state the first attempt left, and a replayed {@code ReadModifyWriteRow} applies its increment or
 * append again. That is the at-least-once contract of this surface, stated once here and in the
 * failure that fails a job for an ambiguous answer.
 *
 * <h2>Retries and failures</h2>
 *
 * <p>There are none, on either side: the client ships both RPCs with an empty retryable-code set
 * and this writer adds no loop, because a retry of an ambiguous failure could apply a
 * non-idempotent request twice. Each request has one attempt bounded by {@code
 * BigtableRequestOptions.requestTimeout}. What a failure means is {@link RequestFailures}'
 * decision: a row-level rejection is handed to the configured {@link FailureHandler} — the record
 * is malformed and sending it again cannot succeed — while an ambiguous or fatal failure is
 * captured into {@code asyncError} and fails the job from the next {@link #write} or {@link
 * #flush}. The handler's own contract (drop-versus-throw semantics, what a checkpoint means under a
 * dropping policy, the capture of a handler failing inside a completion) is stated once on {@link
 * FailureHandler}.
 *
 * <p>The in-flight cap is {@code BigtableRequestOptions.maxInFlightRequests}, counted in requests
 * accepted by the client and not yet answered: at the cap {@link #write} yields to the mailbox
 * until completions bring the count down. Admission is checked before a request, never "would this
 * one fit", so an empty writer always admits — a wait ends only when a completion arrives, and none
 * can with nothing in flight.
 *
 * <p>A failure that first surfaces during {@link #close()} reaches neither the handler nor {@code
 * asyncError}: the close sets {@code closed} first, and a completion's mail is a no-op from then
 * on. On Flink's success path the mailbox is quiesced before the operators close and the mail is
 * rejected outright; on its failure path the mailbox is still open but no longer drained, so the
 * mail is enqueued and dropped when the mailbox closes after the operators. The close cancels what
 * is outstanding and lets the restart replay it.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class SingleRowRequestWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleRowRequestWriter.class);

    /**
     * How long a wait may go without the client answering anything before it says so, and the
     * default of the injectable one below.
     *
     * <p>The same tenth-of-a-budget the batching writer warns at ({@code docs/adr/0078}), against
     * the same shape of wait. Here every request carries its own deadline, {@code
     * BigtableRequestOptions.requestTimeout}, and a wait past that deadline ends in a job failure
     * rather than a stall — so with the default 20-second deadline this warning never fires, and it
     * speaks only for a job whose deadline was raised past a minute.
     */
    private static final long STALL_WARN_AFTER_NANOS = Duration.ofSeconds(60).toNanos();

    /**
     * How long a wait parks when the mailbox has nothing to run: the throughput cost of not using
     * the blocking {@link MailboxExecutor#yield()}, sized by mail latency rather than by the
     * warning.
     */
    private static final long POLL_INTERVAL_NANOS = Duration.ofMillis(1).toNanos();

    /** {@link #awaitRequestProgress} ran a mail rather than finding the mailbox empty. */
    private static final long RAN_A_MAIL = -1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final RowRequestSerializer<? super T> serializer;
    private final SingleRowClientFactory clientFactory;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightRequests;
    private final int maxActiveInstances;
    private final long destinationIdleTimeoutNanos;
    private final long stallWarnAfterNanos;
    private final LongSupplier nanoClock;
    private final FailureHandler<? super FailedRequest> failedRequestHandler;
    private final SingleRowRequestMetrics metrics;
    private final DestinationClients clients;

    /** Number of requests accepted and not yet answered; touched only on the task thread. */
    private int inFlight;

    /**
     * The accepted requests not yet answered, so {@link #close()} can cancel them; touched only on
     * the task thread. An identity set: a handle is its own key.
     */
    private final Set<RequestHandle> outstanding =
            Collections.newSetFromMap(new IdentityHashMap<>());

    /**
     * When the client last answered a request, successfully or not — the clock a wait measures its
     * idleness against. Stamped in the completion callback, on the client thread, and not when the
     * resulting mail runs: a mail enqueued but not yet dequeued already answers the question a wait
     * asks, which is whether the client is still answering. Volatile as a monotonic timestamp read
     * only as a difference; initialised at construction so it is always a real clock reading.
     */
    private volatile long lastCompletionNanos;

    /** When {@link #warnIfStalled} last spoke; touched only on the task thread. */
    private long lastStallWarnNanos;

    /**
     * First terminal failure; set and read only on the task thread (failure callbacks re-dispatch
     * through the mailbox).
     */
    private IOException asyncError;

    /**
     * Set by {@link #close()} before it cancels what is outstanding, so a completion mail that
     * still runs — a cancellation's callback re-dispatches one when the mailbox is not yet
     * quiesced, as a test's is not and Flink's failure path's is not — finds nothing to release and
     * nothing to report.
     */
    private boolean closed;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param clientFactory the factory leasing one client per instance; closed with the writer
     * @param mailboxExecutor the task mailbox, used to run request completions on the task thread
     * @param metricGroup the writer's metric group, which {@link SingleRowRequestMetrics} registers
     *     this sink's counters and gauges on
     */
    public SingleRowRequestWriter(
            SingleRowRequestConfig<T> config,
            SingleRowClientFactory clientFactory,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        this(
                config,
                clientFactory,
                mailboxExecutor,
                metricGroup,
                System::nanoTime,
                STALL_WARN_AFTER_NANOS);
    }

    /**
     * Creates the writer with an explicit clock and stall-warning threshold, so tests can
     * fast-forward the idle timeout and reach the warning without waiting a minute for it.
     */
    @VisibleForTesting
    SingleRowRequestWriter(
            SingleRowRequestConfig<T> config,
            SingleRowClientFactory clientFactory,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup,
            LongSupplier nanoClock,
            long stallWarnAfterNanos) {
        this.destinationResolver = config.getDestinationResolver();
        this.serializer = config.getSerializer();
        this.clientFactory = clientFactory;
        this.mailboxExecutor = mailboxExecutor;
        this.nanoClock = nanoClock;
        BigtableRequestOptions options = config.getRequestOptions();
        // Re-checked here, not only on the options builder, for the reason BigtableWriter re-checks
        // its caps: a non-positive in-flight cap holds the admission predicate with nothing in
        // flight and no completion can arrive to end it — a silent permanent park — and Java
        // deserialization does not run the builder. The idle timeout gets no re-check, as there:
        // its failure is an immediate throw at construction, not a silent one (ADR-0068).
        Preconditions.checkArgument(
                options.getMaxInFlightRequests() > 0, "maxInFlightRequests must be positive");
        Preconditions.checkArgument(
                options.getMaxActiveInstances() > 0, "maxActiveInstances must be positive");
        this.maxInFlightRequests = options.getMaxInFlightRequests();
        this.maxActiveInstances = options.getMaxActiveInstances();
        this.destinationIdleTimeoutNanos = options.getDestinationIdleTimeout().toNanos();
        this.stallWarnAfterNanos = stallWarnAfterNanos;
        // Both stamped from a real clock reading rather than left at zero, which is not one; the
        // warning's is back-dated by its own threshold so the first stall warns as promptly as the
        // tenth.
        this.lastCompletionNanos = nanoClock.getAsLong();
        this.lastStallWarnNanos = this.lastCompletionNanos - stallWarnAfterNanos;
        this.failedRequestHandler = config.getFailedRequestHandler();
        this.metrics =
                SingleRowRequestMetrics.forSink(metricGroup, options.isPerDestinationMetrics());
        this.clients = new DestinationClients(clientFactory, metrics, nanoClock);
        this.metrics.bindState(this::getInFlight, clients::activeClients);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        TableDestination destination = destinationResolver.resolve(element, context);
        if (destination == null) {
            throw new IOException("The destination resolver returned null for a record.");
        }
        RowRequest<?> request;
        try {
            request = serializer.serialize(element, context);
        } catch (IOException | RuntimeException e) {
            // The record never became a request, so there is nothing to carry but the destination
            // — which is why the resolve above runs first. Handled on the task thread, so a handler
            // that fails the job throws at the caller directly. Counted before the handler runs,
            // because the counter says "routed", not "dropped"; not under errorClass, since a
            // serialization failure carries no status. The per-table counter is looked up rather
            // than read off a DestinationState: there is none, and there must not be — a poison
            // record must reach the handler without leasing a client for a table that may never
            // receive a request.
            metrics.requestFailed(null);
            metrics.requestRouted(metrics.forTable(destination));
            failedRequestHandler.handle(
                    FailedRequest.of(
                            destination, null, null, "The record could not be serialized.", e));
            return;
        }
        if (request == null) {
            // Skip by contract, not a failure. Counted, because nothing else reports it. Ahead of
            // stateFor(...), so a record sent nowhere leases no client.
            metrics.recordSkipped();
            return;
        }
        DestinationState state = stateFor(destination);
        state.lastAccessNanos = nanoClock.getAsLong();
        awaitCapacity();
        submit(state, request);
    }

    /** Returns the destination's state, leasing its instance's client on first use. */
    private DestinationState stateFor(TableDestination destination)
            throws IOException, InterruptedException {
        DestinationState state = clients.find(destination);
        if (state != null) {
            return state;
        }
        if (!clients.holdsInstance(destination)) {
            ensureInstanceCapacity();
        }
        return clients.open(destination);
    }

    private void ensureInstanceCapacity() throws IOException, InterruptedException {
        if (clients.instanceCount() < maxActiveInstances) {
            return;
        }
        drainInFlight();
        String eldest = clients.leastRecentlyUsedInstance();
        List<DestinationState> evicted = clients.removeInstance(eldest);
        metrics.capacityEviction();
        closeEvicted(evicted, "least-recently-used Bigtable instance " + eldest);
    }

    /**
     * Starts a request on its table's client, counts it in flight and registers its completion.
     *
     * <p>Counted only once the client accepted it: a synchronous throw registers no callback, so
     * nothing would ever release it. Which throw it is decides whose failure it is. The client's
     * request builders check the request's own content before the call leaves the process, with
     * {@code IllegalArgumentException} or {@code IllegalStateException} — a {@code
     * CheckAndMutateRow} whose {@code then} and {@code otherwise} are both empty is refused that
     * way, inside the call — and a request that fails for its content fails the same way on every
     * replay, so it is a row-level failure routed to the handler like a rejected one. Any other
     * throw is the client refusing work — a rejected executor, a closed channel — and fails the
     * write rather than the record; a refused call never reports through those two types.
     */
    private void submit(DestinationState state, RowRequest<?> request) throws IOException {
        ApiFuture<?> future;
        try {
            future = request.start(state.client, state.destination);
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Task thread, like a serialization failure: a handler that fails the job throws at
            // the caller directly, and the count says "routed".
            metrics.requestFailed(e);
            metrics.requestRouted(state.counters);
            failedRequestHandler.handle(
                    FailedRequest.of(
                            state.destination,
                            request.operation(),
                            request.rowKey(),
                            "The request was refused by the client's validation before it was"
                                    + " sent.",
                            e));
            return;
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to start a "
                            + request.operation().getRpcName()
                            + " request to Bigtable table "
                            + state.destination
                            + ".",
                    e);
        }
        inFlight++;
        state.inFlight.incrementAndGet();
        metrics.requestAccepted(state.counters);
        RequestHandle handle =
                new RequestHandle(state, request.operation(), request.rowKey(), future);
        outstanding.add(handle);
        ApiFutures.addCallback(future, new Completion(handle, request), Runnable::run);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        drainInFlight();
        // The handler's flush comes after the drain, which is what discovers this checkpoint's
        // row-level failures; flushing earlier would checkpoint past a dead letter the drain was
        // about to produce.
        failedRequestHandler.flush();
        if (!endOfInput) {
            evictIdleDestinations();
        }
    }

    /**
     * Returns the leases of tables idle beyond the configured timeout — memory hygiene for
     * long-lived jobs with per-record destinations. Runs at the end of a successful flush, where
     * nothing is in flight, so a lease returned here has no request over it.
     */
    private void evictIdleDestinations() throws InterruptedException {
        List<DestinationState> evicted =
                clients.removeIdle(nanoClock.getAsLong(), destinationIdleTimeoutNanos);
        closeEvicted(evicted, "idle Bigtable destinations");
    }

    /**
     * Returns dropped leases to the factory. A failure to do so is logged rather than thrown: the
     * logical state was removed and will be rebuilt on the next matching record, and a job need not
     * fail because a client did not close cleanly. An interrupt is the exception — it is the task
     * being cancelled, and is re-raised with the flag set.
     */
    private void closeEvicted(List<DestinationState> evicted, String reason)
            throws InterruptedException {
        if (evicted.isEmpty()) {
            return;
        }
        try {
            clients.release(evicted);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to release one or more Bigtable clients while evicting {}; the logical"
                            + " state was removed and will be recreated on the next matching"
                            + " record.",
                    reason,
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        // No flush here: on success Flink calls flush(true) before close, and on the failure path
        // what is outstanding is cancelled and replayed by the restart — no checkpoint completed
        // with it in flight. The gauge is zeroed before anything below can throw, since nothing
        // decrements it afterwards: the mails that would are rejected by a quiesced mailbox or
        // never run (see the class comment).
        inFlight = 0;
        closed = true;
        // A cancellation's callback runs synchronously inside cancel(), on this thread; it finds
        // the mailbox quiesced (rejection swallowed at debug) or enqueues a mail that the closed
        // flag turns into a no-op. Snapshot first, as neither path may mutate the set.
        List<RequestHandle> cancelled = new ArrayList<>(outstanding);
        outstanding.clear();
        for (RequestHandle handle : cancelled) {
            handle.cancel();
        }
        // The factory after every cancel: it closes the clients the requests were started on.
        try {
            Closers.closeAll(clientFactory::close, failedRequestHandler::close);
        } finally {
            clients.clear();
        }
    }

    /**
     * Admission gate for {@link #write}: runs mailbox mails until the in-flight count is under the
     * cap, surfacing any captured failure before the caller starts another request.
     */
    private void awaitCapacity() throws IOException, InterruptedException {
        long start = nanoClock.getAsLong();
        while (inFlight >= maxInFlightRequests) {
            checkAsyncError();
            warnIfStalled(awaitRequestProgress(start, "admitting a record"), "admitting a record");
        }
        checkAsyncError();
    }

    /**
     * Runs mailbox mails until <b>no</b> request is in flight, surfacing any captured failure —
     * including one processed by the final mail — before the caller proceeds. A correctness
     * primitive, not backpressure: a completed checkpoint claims every request up to the barrier
     * was answered.
     */
    private void drainInFlight() throws IOException, InterruptedException {
        long start = nanoClock.getAsLong();
        while (inFlight > 0) {
            checkAsyncError();
            warnIfStalled(
                    awaitRequestProgress(start, "draining the outstanding requests"),
                    "draining the outstanding requests");
        }
        checkAsyncError();
    }

    /**
     * Runs one mailbox mail, or reports how long this wait has gone without the client answering
     * anything. The shape and both of its load-bearing details — the interrupt flag read first and
     * on every pass, the idle time read only once {@code tryYield} came back empty — are {@code
     * BigtableWriter}'s, which explains them.
     *
     * @return the time this wait has gone without progress, or {@code RAN_A_MAIL} if it ran a mail
     */
    private long awaitRequestProgress(long waitStartNanos, String what)
            throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted while " + what + " for Bigtable.");
        }
        if (mailboxExecutor.tryYield()) {
            return RAN_A_MAIL;
        }
        long idleSinceNanos =
                lastCompletionNanos - waitStartNanos > 0 ? lastCompletionNanos : waitStartNanos;
        long idleNanos = nanoClock.getAsLong() - idleSinceNanos;
        LockSupport.parkNanos(POLL_INTERVAL_NANOS);
        return idleNanos;
    }

    /**
     * Says that a wait has stopped making progress. Unlike the batching writer's stall, this one
     * ends on its own: every request carries {@code requestTimeout}, after which the client fails
     * it and the job fails — so the line names the deadline the reader is waiting on.
     */
    private void warnIfStalled(long idleNanos, String what) {
        if (idleNanos == RAN_A_MAIL || idleNanos < stallWarnAfterNanos) {
            return;
        }
        long now = nanoClock.getAsLong();
        if (now - lastStallWarnNanos < stallWarnAfterNanos) {
            return;
        }
        lastStallWarnNanos = now;
        LOG.warn(
                "No Bigtable single-row request has been answered for {} while {} ({} requests in"
                        + " flight over {} table(s)). The sink is waiting: each request fails with"
                        + " DEADLINE_EXCEEDED at its own BigtableRequestOptions.requestTimeout, and"
                        + " that failure is ambiguous — the service may have applied it — so it"
                        + " fails the job. Watch requestsCompleted, which stays flat for as long"
                        + " as this lasts.",
                Duration.ofNanos(idleNanos),
                what,
                inFlight,
                clients.tableCount());
    }

    private void checkAsyncError() throws IOException {
        if (asyncError != null) {
            throw asyncError;
        }
    }

    /** Releases one answered request from the ledger. */
    private void release(RequestHandle handle) {
        outstanding.remove(handle);
        inFlight--;
        handle.state.inFlight.decrementAndGet();
    }

    /** Task-thread handler for an answered request, run as a mailbox mail. */
    private void onCompleted(RequestHandle handle, RowRequest<?> request, Object answer) {
        if (closed || !handle.settle()) {
            return;
        }
        release(handle);
        metrics.requestCompleted();
        try {
            request.onSuccess(answer, metrics);
        } catch (IOException | RuntimeException e) {
            if (asyncError == null) {
                asyncError =
                        e instanceof IOException
                                ? (IOException) e
                                : new IOException(
                                        "Failed to interpret the Bigtable request outcome.", e);
            }
        }
    }

    /** Task-thread handler for a failed request, run as a mailbox mail. */
    private void onFailed(RequestHandle handle, Throwable throwable) {
        if (closed || !handle.settle()) {
            return;
        }
        release(handle);
        // Every failure is counted, ambiguous and fatal ones included and fatal ones after the
        // first: with no retry anywhere, each is a distinct give-up.
        metrics.requestFailed(throwable);
        RequestFailures.Kind kind = RequestFailures.classify(throwable);
        if (kind == RequestFailures.Kind.ROW_LEVEL) {
            routeFailedRequest(handle, throwable);
        } else if (asyncError == null) {
            asyncError =
                    RequestFailures.jobFailure(
                            kind, handle.operation, handle.state.destination, throwable);
        }
    }

    /**
     * Hands a row-level failure to the configured handler. Runs as a mailbox mail, so a handler
     * that fails the job cannot throw at a caller: its failure is captured into {@code asyncError}
     * and rethrown from the next {@link #write} or {@link #flush}, exactly as a terminal failure
     * is. First failure wins. Routing is not skipped once {@code asyncError} is set: the request
     * really did fail terminally, and a dead-letter destination missing it is worse than one
     * holding a request a replay will produce again.
     */
    private void routeFailedRequest(RequestHandle handle, Throwable throwable) {
        metrics.requestRouted(handle.state.counters);
        try {
            failedRequestHandler.handle(
                    FailedRequest.of(
                            handle.state.destination,
                            handle.operation,
                            handle.rowKey,
                            "The request was rejected because "
                                    + RequestFailures.ROW_LEVEL_REASON
                                    + "."
                                    + RequestFailures.routingHint(handle.operation),
                            throwable));
        } catch (IOException | RuntimeException e) {
            if (asyncError == null) {
                asyncError =
                        e instanceof IOException
                                ? (IOException) e
                                : new IOException(
                                        "The failed-request handler failed for Bigtable table "
                                                + handle.state.destination
                                                + ".",
                                        e);
            }
        }
    }

    @VisibleForTesting
    int getInFlight() {
        return inFlight;
    }

    @VisibleForTesting
    int getActiveClients() {
        return clients.activeClients();
    }

    /**
     * Re-dispatches a request's answer onto the mailbox so state stays task-thread-only.
     *
     * <p>Both stamp before dispatching, on the client thread: what a wait measures is whether the
     * client is still answering, and a failure is an answer. The dispatch is guarded against a
     * mailbox that refuses work — quiesced, which Flink's finish path reaches before it closes
     * operators, or closed, which its failure path reaches after them: an answer arriving then —
     * including the cancellation {@link #close()} itself provokes — has nowhere to go and is logged
     * at debug, since the restart replays what it concerned.
     */
    private final class Completion implements ApiFutureCallback<Object> {

        private final RequestHandle handle;

        private final RowRequest<?> request;

        private Completion(RequestHandle handle, RowRequest<?> request) {
            this.handle = handle;
            this.request = request;
        }

        @Override
        public void onSuccess(Object result) {
            lastCompletionNanos = nanoClock.getAsLong();
            dispatch(
                    () -> onCompleted(handle, request, result), handle.state.completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            lastCompletionNanos = nanoClock.getAsLong();
            dispatch(() -> onFailed(handle, throwable), handle.state.failureDescription);
        }

        private void dispatch(Runnable mail, String description) {
            try {
                mailboxExecutor.execute(mail::run, description);
            } catch (RejectedExecutionException e) {
                LOG.debug(
                        "{} arrived after the task mailbox was quiesced or closed; the restart"
                                + " replays it.",
                        description,
                        e);
            }
        }
    }
}
