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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Base of the async-operator surface: one single-row request per input, whose answer becomes the
 * operator's output.
 *
 * <p>A subclass names the table an input goes to, builds its request, and maps the answer to the
 * output type; the per-operation functions of the conditional and read-modify-write sinks are its
 * subclasses, and the Flink SQL functions wrap those.
 *
 * <h2>Threading model</h2>
 *
 * <p>Unlike the sink writer, this surface has no mailbox to hop back onto: {@link #asyncInvoke} and
 * {@link #timeout} run on the task thread (Flink runs the operator's timers on its mailbox), while
 * a request's answer arrives on a client thread — or inline on the task thread, when the client
 * answered before the callback was registered — and must complete the {@link ResultFuture} from
 * there. So the destination pool stays task-thread-only, and everything an answer touches is
 * thread-safe: the in-flight ledger, the counters, and the handle's one-shot settlement that
 * decides between the answer and Flink's timeout when both arrive. {@link #result(Object, Object)}
 * runs on the client thread too.
 *
 * <h2>Capacity and timeouts</h2>
 *
 * <p>The operator's capacity — the number handed to {@code AsyncDataStream} — is this surface's
 * in-flight bound; {@code BigtableRequestOptions.maxInFlightRequests} is the sink writer's, and the
 * documentation asks that the two be the same number. The client's own deadline, {@code
 * BigtableRequestOptions.requestTimeout}, is where a slow request is expected to fail, with a
 * Bigtable-named message and an ambiguity verdict; Flink's operator timeout should sit above it,
 * and when it fires first {@link #timeout} cancels the request and fails the input naming both.
 *
 * <p>Under the operator's retry mode ({@code AsyncDataStream.unorderedWaitWithRetry} and its
 * ordered twin) the retries are the job's, chosen by its predicate: each attempt is a new call of
 * {@link #asyncInvoke} with the same {@link ResultFuture}, so a new request carrying the
 * at-least-once cost of a replay, and the operator timeout spans every attempt and every backoff of
 * one input. When it fires between attempts there is nothing to cancel, and {@link #timeout} fails
 * the input saying so; an answer arriving as it fires stands only if the operator processed it
 * first, as under Flink's default timeout. It completes the result on every path: the operator has
 * no fallback of its own, and an open result holds its queue slot until the task ends, or until end
 * of input forces one more attempt in a bounded job.
 *
 * <p>The instance cap never waits on in-flight work: at {@code
 * BigtableRequestOptions.maxActiveInstances} a new instance evicts the least recently used one with
 * nothing in flight, or fails the input naming the option if every held instance is busy. The one
 * wait that remains is the client factory's, for the evicted client's own close to free its slot —
 * bounded by that close, not by any request. Idle tables are swept as inputs arrive, at most once
 * per idle timeout.
 *
 * <h2>Failures and replay</h2>
 *
 * <p>There is no failure handler on this surface: the handler contract is task-thread, and an
 * answer arrives elsewhere. Every failed request fails the job, with {@link RequestFailures}'
 * message for its kind; a subclass that wants to model a row-level rejection as a value can do so
 * in its own output type. Flink checkpoints the inputs the operator has not yet emitted and replays
 * them after a restore, so a completed checkpoint means "emitted or replayed", not "applied" — a
 * replayed {@code ReadModifyWriteRow} applies its increment or append again.
 *
 * @param <IN> type of the inputs
 * @param <R> type of the request's answer
 * @param <OUT> type of the outputs
 */
@Internal
public abstract class BigtableRequestFunction<IN, R, OUT> extends RichAsyncFunction<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(BigtableRequestFunction.class);

    @Nullable private final String appProfileId;
    private final BigtableRequestOptions options;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /** An injected factory, or {@code null} for the production one built in {@link #open}. */
    @Nullable private final SingleRowClientFactory injectedFactory;

    /**
     * An injected clock, or {@code null} for {@link System#nanoTime()}. Transient: a test injects
     * it into the instance it opens, and a job never has one.
     */
    @Nullable private transient LongSupplier injectedNanoClock;

    private transient LongSupplier nanoClock;

    private transient SingleRowClientFactory clientFactory;
    private transient DestinationClients clients;
    private transient SingleRowRequestMetrics metrics;

    /** Requests accepted and not yet settled; incremented on the task thread, released anywhere. */
    private transient AtomicInteger inFlight;

    /**
     * The accepted requests whose outcome is not yet handed to Flink, for {@link #timeout} and
     * {@link #close()}, keyed by the {@link ResultFuture} Flink hands to both {@link #asyncInvoke}
     * and {@link #timeout} — never by the input, whose instance two records in flight can share.
     * Identity-keyed, as the operator's handler defines no equality; synchronized, since an answer
     * removes its entry from the client thread. An entry is dropped only after its completion was
     * handed off, so a missing entry means nothing is in flight and nothing is completing.
     */
    private transient Map<ResultFuture<OUT>, Answer> outstanding;

    private transient long lastIdleSweepNanos;
    private transient volatile boolean closed;

    /**
     * Creates the function for a job.
     *
     * @param appProfileId the app profile, or {@code null} for the instance's default
     * @param options the runtime options
     * @param serviceAccountKeyFile the service-account key file, or {@code null} for
     *     application-default credentials
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for production Bigtable
     */
    protected BigtableRequestFunction(
            @Nullable String appProfileId,
            BigtableRequestOptions options,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(appProfileId, options, serviceAccountKeyFile, emulatorEndpoint, null);
    }

    /**
     * Creates the function over an injected client factory, so tests need no client.
     *
     * @param clientFactory the factory
     * @param options the runtime options
     */
    @VisibleForTesting
    protected BigtableRequestFunction(
            SingleRowClientFactory clientFactory, BigtableRequestOptions options) {
        this(null, options, null, null, Preconditions.checkNotNull(clientFactory));
    }

    /**
     * Creates the function over an injected client factory and clock, so a test can fast-forward
     * the idle timeout.
     *
     * @param clientFactory the factory
     * @param options the runtime options
     * @param nanoClock the clock, in the units of {@link System#nanoTime()}
     */
    @VisibleForTesting
    protected BigtableRequestFunction(
            SingleRowClientFactory clientFactory,
            BigtableRequestOptions options,
            LongSupplier nanoClock) {
        this(clientFactory, options);
        this.injectedNanoClock = Preconditions.checkNotNull(nanoClock);
    }

    private BigtableRequestFunction(
            @Nullable String appProfileId,
            BigtableRequestOptions options,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable SingleRowClientFactory injectedFactory) {
        this.appProfileId = appProfileId;
        this.options = Preconditions.checkNotNull(options, "options must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.injectedFactory = injectedFactory;
    }

    /**
     * Names the table an input's request goes to.
     *
     * @param input the input
     * @return the table
     * @throws Exception if the table cannot be determined; fails the input
     */
    protected abstract TableDestination destination(IN input) throws Exception;

    /**
     * Builds an input's request.
     *
     * @param input the input
     * @return the request, or {@code null} to skip the input, which then emits nothing
     * @throws Exception if the request cannot be built; fails the input
     */
    @Nullable
    protected abstract RowRequest<R> request(IN input) throws Exception;

    /**
     * Maps an answered request to the output. Runs on the client thread that received the answer,
     * possibly concurrently for different inputs, so it must be thread-safe.
     *
     * @param input the input
     * @param answer the request's answer
     * @return the output
     * @throws Exception if the answer cannot be mapped; fails the input
     */
    protected abstract OUT result(IN input, R answer) throws Exception;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        // Re-checked for the reason the sink writer re-checks it: Java deserialization does not
        // run the builder, and a non-positive cap would refuse every input.
        Preconditions.checkArgument(
                options.getMaxActiveInstances() > 0, "maxActiveInstances must be positive");
        clientFactory =
                injectedFactory != null
                        ? injectedFactory
                        : new DefaultSingleRowClientFactory(
                                appProfileId,
                                options,
                                emulatorEndpoint,
                                BigtableCredentials.loadData(serviceAccountKeyFile));
        // Thread-safe counters: answers count from client threads, concurrently.
        metrics =
                SingleRowRequestMetrics.forOperator(
                        getRuntimeContext().getMetricGroup(),
                        options.isPerDestinationMetrics(),
                        ThreadSafeSimpleCounter::new);
        nanoClock = injectedNanoClock != null ? injectedNanoClock : System::nanoTime;
        clients = new DestinationClients(clientFactory, metrics, nanoClock);
        inFlight = new AtomicInteger();
        outstanding = Collections.synchronizedMap(new IdentityHashMap<>());
        lastIdleSweepNanos = nanoClock.getAsLong();
        closed = false;
        metrics.bindState(inFlight::get, clients::activeClients);
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        TableDestination destination;
        RowRequest<R> request;
        try {
            destination = destination(input);
            if (destination == null) {
                throw new IOException("The destination of a record resolved to null.");
            }
            request = request(input);
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
            return;
        }
        if (request == null) {
            // Skip by contract: the input emits nothing. Counted, because nothing else reports it.
            metrics.recordSkipped();
            resultFuture.complete(Collections.emptyList());
            return;
        }
        long now = nanoClock.getAsLong();
        sweepIdleDestinations(now);
        DestinationState state;
        try {
            state = stateFor(destination);
        } catch (IOException e) {
            resultFuture.completeExceptionally(e);
            return;
        }
        state.lastAccessNanos = now;
        ApiFuture<R> future;
        try {
            future = request.start(state.client, destination);
        } catch (RuntimeException e) {
            resultFuture.completeExceptionally(
                    new IOException(
                            "Failed to start a "
                                    + request.operation().getRpcName()
                                    + " request to Bigtable table "
                                    + destination
                                    + ".",
                            e));
            return;
        }
        // Counted only once the client accepted it: a synchronous throw registers no callback, so
        // nothing would ever release it.
        inFlight.incrementAndGet();
        state.inFlight.incrementAndGet();
        metrics.requestAccepted(state.counters);
        Answer answer =
                new Answer(
                        input,
                        new RequestHandle(state, request.operation(), request.rowKey(), future),
                        resultFuture);
        outstanding.put(resultFuture, answer);
        ApiFutures.addCallback(future, answer, Runnable::run);
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        Answer answer = outstanding.get(resultFuture);
        if (answer == null) {
            // Outside retry mode the operator reaches here only in the moment after an answer
            // completed the result and before its timer saw that, and drops this second
            // completion. No counter moves: no request ended, and the attempt before was counted
            // as what it was.
            resultFuture.completeExceptionally(betweenAttempts());
            return;
        }
        RequestHandle handle = answer.handle;
        if (!handle.settle()) {
            // The answer won the settlement and is handing its outcome to Flink. This call cannot
            // tell whether the operator already took that outcome for a retry it has just
            // cancelled — the one ordering in which nothing else would complete the result — so
            // it completes the result now, as Flink's own default timeout would: whichever of the
            // two completions the operator processes first stands, and it drops the other. The
            // request itself is counted by the answer.
            resultFuture.completeExceptionally(answeredAtTimeout(handle));
            return;
        }
        forget(resultFuture, answer);
        releaseCount(answer);
        metrics.requestTimedOut();
        // The cancellation's callback runs synchronously inside this call, finds the handle
        // settled and does nothing.
        handle.cancel();
        resultFuture.completeExceptionally(
                new IOException(
                        "A "
                                + handle.operation.getRpcName()
                                + " request to Bigtable table "
                                + handle.state.destination
                                + " did not complete within the async operator's timeout and was"
                                + " cancelled, so the service may or may not have applied it."
                                + " BigtableRequestOptions.requestTimeout ("
                                + options.getRequestTimeout()
                                + ") is the client's own deadline, where a slow request is expected"
                                + " to fail; set the operator timeout above it."));
    }

    /**
     * The failure for a timeout that found no request to cancel: the operator's retry mode holds
     * the input between attempts, and the timeout just cancelled the retry, so this is the only
     * completion the result will get.
     */
    private IOException betweenAttempts() {
        return new IOException(
                "The async operator's timeout elapsed with no Bigtable request in flight for the"
                        + " input: the operator was between attempts of its retry strategy, and"
                        + " nothing was cancelled. The operator timeout covers every attempt and"
                        + " every backoff of one input; set it above"
                        + " BigtableRequestOptions.requestTimeout ("
                        + options.getRequestTimeout()
                        + ") for each attempt the strategy allows, plus the backoff.");
    }

    /** The failure for a timeout that found the request answered and its outcome on its way. */
    private IOException answeredAtTimeout(RequestHandle handle) {
        return new IOException(
                "A "
                        + handle.operation.getRpcName()
                        + " request to Bigtable table "
                        + handle.state.destination
                        + " answered as the async operator's timeout elapsed."
                        + " BigtableRequestOptions.requestTimeout ("
                        + options.getRequestTimeout()
                        + ") is the client's own deadline, where a slow request is expected to"
                        + " fail; set the operator timeout above it.");
    }

    /** Returns the destination's state, leasing its instance's client on first use. */
    private DestinationState stateFor(TableDestination destination) throws IOException {
        DestinationState state = clients.find(destination);
        if (state != null) {
            return state;
        }
        if (!clients.holdsInstance(destination)) {
            ensureInstanceCapacity(destination);
        }
        try {
            return clients.open(destination);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while creating a Bigtable client for table " + destination + ".",
                    e);
        }
    }

    /**
     * Makes room for one more instance without waiting on any request: evicts the least recently
     * used idle one, or fails the input if every held instance has a request in flight —
     * deterministically, naming the option, rather than overshooting a cap the client factory
     * enforces by itself. The factory then waits for the evicted client's close before it creates
     * the next one; that wait is bounded by the close.
     */
    private void ensureInstanceCapacity(TableDestination destination) throws IOException {
        if (clients.instanceCount() < options.getMaxActiveInstances()) {
            return;
        }
        String idle = clients.leastRecentlyUsedIdleInstance();
        if (idle == null) {
            throw new IOException(
                    "Cannot open a Bigtable client for table "
                            + destination
                            + ": the "
                            + options.getMaxActiveInstances()
                            + " instance client(s) BigtableRequestOptions.maxActiveInstances"
                            + " allows are all held with requests in flight. Raise the option, or"
                            + " route fewer instances through one subtask.");
        }
        List<DestinationState> evicted = clients.removeInstance(idle);
        metrics.capacityEviction();
        releaseEvicted(evicted, "least-recently-used Bigtable instance " + idle);
    }

    /**
     * Drops the leases of tables idle beyond the timeout, at most once per timeout: the surface has
     * no flush to sweep from, so the sweep rides on the inputs, and the interval keeps it from
     * walking the pool per record.
     */
    private void sweepIdleDestinations(long now) {
        long idleTimeoutNanos = options.getDestinationIdleTimeout().toNanos();
        if (now - lastIdleSweepNanos < idleTimeoutNanos) {
            return;
        }
        lastIdleSweepNanos = now;
        releaseEvicted(clients.removeIdle(now, idleTimeoutNanos), "idle Bigtable destinations");
    }

    private void releaseEvicted(List<DestinationState> evicted, String reason) {
        if (evicted.isEmpty()) {
            return;
        }
        try {
            clients.release(evicted);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
        closed = true;
        if (outstanding != null) {
            // Every cancellation's callback settles and releases its handle, from this thread,
            // mutating the map: snapshot first.
            List<Answer> cancelled = new ArrayList<>(outstanding.values());
            for (Answer answer : cancelled) {
                answer.handle.cancel();
            }
            outstanding.clear();
            // The in-flight count is not reset: a handle an answer settled on a client thread just
            // before this releases itself when that thread gets to it, and a reset would let that
            // release take the gauge below zero.
        }
        try {
            if (clientFactory != null) {
                clientFactory.close();
            }
        } finally {
            if (clients != null) {
                clients.clear();
            }
            super.close();
        }
    }

    /**
     * Drops one settled request from the ledger; callable from any thread. The removal names its
     * own entry: retry mode hands the same {@link ResultFuture} to every attempt, so a removal that
     * raced the next attempt's registration must not take that entry.
     */
    private void forget(ResultFuture<OUT> resultFuture, Answer answer) {
        outstanding.remove(resultFuture, answer);
    }

    /** Releases one settled request's share of the in-flight counts; callable from any thread. */
    private void releaseCount(Answer answer) {
        inFlight.decrementAndGet();
        answer.handle.state.inFlight.decrementAndGet();
    }

    @VisibleForTesting
    int getInFlight() {
        return inFlight.get();
    }

    @VisibleForTesting
    int getActiveClients() {
        return clients.activeClients();
    }

    /**
     * Completes an input's result from the client thread that received its answer, unless Flink's
     * timeout or {@link #close()} settled the request first.
     *
     * <p>Completing a {@code ResultFuture} hands a mail to the task mailbox, which refuses one once
     * the task is finishing or failing — Flink quiesces the mailbox before it closes the operators
     * on the finish path, and closes it after them on the failure path. An answer landing in that
     * window is logged at debug and dropped: the operator's checkpointed inputs replay it after the
     * restart, and a finishing task has already drained its queue.
     *
     * <p>The in-flight counts are released before the hand-off, so an answered instance is idle for
     * the next input by the time Flink emits the result; the ledger entry is dropped after it, so a
     * timeout firing in between finds the entry and reads the request as answered rather than as
     * absent, which would make it report an input between retry attempts.
     */
    private final class Answer implements ApiFutureCallback<R> {

        private final IN input;
        private final RequestHandle handle;
        private final ResultFuture<OUT> resultFuture;

        private Answer(IN input, RequestHandle handle, ResultFuture<OUT> resultFuture) {
            this.input = input;
            this.handle = handle;
            this.resultFuture = resultFuture;
        }

        @Override
        public void onSuccess(R answer) {
            if (!handle.settle()) {
                return;
            }
            releaseCount(this);
            try {
                if (closed) {
                    return;
                }
                // The service answered, whatever the mapping below makes of it: the counter is the
                // request's, and a mapping failure is the function's own, reported on the result.
                metrics.requestCompleted();
                OUT output;
                try {
                    output = result(input, answer);
                } catch (Exception e) {
                    completeExceptionally(e);
                    return;
                }
                complete(output);
            } finally {
                forget(resultFuture, this);
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            if (!handle.settle()) {
                return;
            }
            releaseCount(this);
            try {
                if (closed) {
                    return;
                }
                metrics.requestFailed(throwable);
                RequestFailures.Kind kind = RequestFailures.classify(throwable);
                Throwable failure;
                if (kind == RequestFailures.Kind.ROW_LEVEL) {
                    failure =
                            new IOException(
                                    "A "
                                            + handle.operation.getRpcName()
                                            + " request to Bigtable table "
                                            + handle.state.destination
                                            + " was rejected because "
                                            + RequestFailures.ROW_LEVEL_REASON
                                            + ".",
                                    throwable);
                } else {
                    failure =
                            RequestFailures.jobFailure(
                                    kind, handle.operation, handle.state.destination, throwable);
                }
                completeExceptionally(failure);
            } finally {
                forget(resultFuture, this);
            }
        }

        private void complete(OUT output) {
            try {
                resultFuture.complete(Collections.singletonList(output));
            } catch (RejectedExecutionException e) {
                logDropped(handle.state.completionDescription, e);
            }
        }

        private void completeExceptionally(Throwable failure) {
            try {
                resultFuture.completeExceptionally(failure);
            } catch (RejectedExecutionException e) {
                logDropped(handle.state.failureDescription, e);
            }
        }

        private void logDropped(String description, RejectedExecutionException e) {
            LOG.debug(
                    "{} arrived after the task mailbox was quiesced or closed; the restart"
                            + " replays it.",
                    description,
                    e);
        }
    }
}
