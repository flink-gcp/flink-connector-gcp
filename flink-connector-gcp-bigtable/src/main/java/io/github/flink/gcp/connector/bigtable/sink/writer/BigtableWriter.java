/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;

import java.io.IOException;
import java.util.Arrays;

/**
 * At-least-once writer applying row mutations to one fixed Bigtable table.
 *
 * <h2>Threading model</h2>
 *
 * <p>All mutable state — the in-flight counters and the captured asynchronous error — is touched
 * only on the task thread. Mutation completion callbacks do not mutate state directly; they
 * re-dispatch onto the {@link MailboxExecutor}, whose mails run on the task thread inside {@link
 * MailboxExecutor#yield()} calls. This is the model the Pub/Sub sink's writer uses.
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless by design: it stores nothing in Flink state. {@link #flush(boolean)}
 * runs at every checkpoint barrier, sends what the client has buffered and blocks until every
 * outstanding mutation is acknowledged, so a successful checkpoint means Bigtable has applied every
 * record up to the barrier and discarding operator state can never lose sink-buffered records.
 * Checkpointing must be enabled in streaming jobs; without it {@code flush()} never runs mid-stream
 * and outstanding mutations are lost on failure. Batch execution is covered by the end-of-input
 * flush.
 *
 * <p>A record replayed after a restart is applied again. Whether that is idempotent is the
 * serializer's decision, not the writer's: a {@code setCell} with an explicit timestamp overwrites
 * the same cell, one without writes another cell version.
 *
 * <h2>Retries</h2>
 *
 * <p>Retrying is the client's, unlike the Cloud Tasks sink: {@code MutateRows} ships a non-empty
 * retryable-code set and retries per entry, so this writer adds classification and routing only. A
 * failure that reaches it is one the client gave up on.
 *
 * <h2>Per-mutation failures</h2>
 *
 * <p>Failures {@link BigtableErrorClassifier} calls {@code ROW_LEVEL}, plus records the serializer
 * rejects, are handed to the configured {@link FailureHandler} instead of failing the job outright:
 * they concern one mutation and applying the same one again cannot succeed. Classification is a
 * <em>precedence over the whole cause chain</em>, not a first match: a failure carrying a transient
 * status anywhere is transient even when a data-shaped status sits in front of it, so an unstable
 * service can never produce a dead letter. The handler drops the mutation by returning and fails
 * the job by throwing; the default {@code failJob()} throws, which is why the classes it does
 * <em>not</em> cover matter — an outage the client gave up on stays a job failure, so no drop
 * policy can quietly discard a backlog. A handler failing inside a completion callback is captured
 * into {@link #asyncError} like any other terminal failure, because a mailbox mail cannot throw a
 * checked exception at its caller.
 *
 * <p>Unacknowledged mutations are capped along both dimensions that bound memory: their number
 * ({@code BigtableWriterOptions.maxInFlightMutations}, default 1000) and their serialized size
 * ({@code BigtableWriterOptions.maxInFlightBytes}, default 64 MiB). At either cap {@link #write}
 * yields to the mailbox until completions bring the counters back down. Admission is checked before
 * a mutation rather than against the mutation's own size, so one larger than the cap is admitted on
 * an empty writer and overshoots it until it completes — deliberate, because {@link
 * MailboxExecutor#yield()} blocks until a mail arrives and no mail can arrive with nothing in
 * flight, so a "does it fit" predicate would be a task hang rather than backpressure.
 *
 * <p>These caps have to be reached before the client's own flow controller, which blocks the
 * calling thread instead of yielding; {@code BigtableWriterOptions} records why that is a
 * constraint on raising them rather than a knob.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigtableWriter<T> implements SinkWriter<T> {

    private final BigtableSinkConfig<T> config;
    private final TableDestination destination;
    private final MutationBatcher batcher;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightMutations;
    private final long maxInFlightBytes;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    private final BigtableWriterMetrics metrics;

    private final String completionDescription;
    private final String failureDescription;

    /** Number of mutations not yet acknowledged; touched only on the task thread. */
    private int inFlightMutations;

    /** Serialized size of the mutations not yet acknowledged; touched only on the task thread. */
    private long inFlightBytes;

    /**
     * First terminal failure; set and read only on the task thread (failure callbacks re-dispatch
     * through the mailbox).
     */
    private IOException asyncError;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param batcher the mutation batcher; closed with the writer
     * @param mailboxExecutor the task mailbox, used to run mutation completions on the task thread
     * @param metricGroup the writer's metric group, which {@link BigtableWriterMetrics} registers
     *     this sink's counters and gauges on
     */
    public BigtableWriter(
            BigtableSinkConfig<T> config,
            MutationBatcher batcher,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        this.config = config;
        this.destination = config.getDestination();
        this.batcher = batcher;
        this.mailboxExecutor = mailboxExecutor;
        BigtableWriterOptions options = config.getWriterOptions();
        // Checked here, not only on the options builder: a non-positive cap holds the
        // awaitCapacity predicate with nothing in flight, and yield() blocks until a mail arrives
        // — so it is a silent permanent park, not a rejected configuration. Fail where the
        // invariant is relied on rather than trusting that every options instance came from the
        // builder, which Java deserialization does not run.
        Preconditions.checkArgument(
                options.getMaxInFlightMutations() > 0, "maxInFlightMutations must be positive");
        Preconditions.checkArgument(
                options.getMaxInFlightBytes() > 0, "maxInFlightBytes must be positive");
        this.maxInFlightMutations = options.getMaxInFlightMutations();
        this.maxInFlightBytes = options.getMaxInFlightBytes();
        this.failedMutationHandler = config.getFailedMutationHandler();
        this.metrics = new BigtableWriterMetrics(metricGroup);
        this.metrics.bindWriterState(this::getInFlightMutations, this::getInFlightBytes);
        this.completionDescription = "Complete a Bigtable mutation of " + destination;
        this.failureDescription = "Fail a Bigtable mutation of " + destination;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        RowMutationEntry entry;
        try {
            entry = config.getSerializer().serialize(element, context);
        } catch (IOException | RuntimeException e) {
            // The record never became a mutation, so there is nothing to carry but the destination:
            // FailedMutation.getPayloadBytes() is null, as the shared contract prescribes. Handled
            // on the task thread, so a handler that fails the job throws at the caller directly.
            // Counted before the handler runs, because the counter says "routed", not "dropped" —
            // a handler that fails the job routed the record just as one that discarded it did.
            // Not counted under errorClass: a serialization failure carries no status.
            metrics.mutationFailed();
            failedMutationHandler.handle(
                    FailedMutation.of(destination, null, "The record could not be serialized.", e));
            return;
        }
        if (entry == null) {
            // Skip by contract, not a failure.
            return;
        }
        // Not memoized by the entry, which builds a fresh proto per call, so it is taken once here
        // and carried by the callback: it is both the byte counter's unit and the metric's.
        int serializedSize = entry.toProto().getSerializedSize();
        awaitCapacity();
        ApiFuture<Void> future;
        try {
            future = batcher.add(entry);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to hand a mutation to the Bigtable batcher for table "
                            + destination
                            + ".",
                    e);
        }
        // Counted only once the mutation is accepted: a synchronous throw registers no callback, so
        // nothing would ever release it.
        inFlightMutations++;
        inFlightBytes += serializedSize;
        metrics.mutationSent(serializedSize);
        ApiFutures.addCallback(future, new MutationCallback(entry, serializedSize), Runnable::run);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        // sendOutstanding rather than the batcher's own blocking flush: waiting has to happen on
        // the mailbox, or the completion mails this writer's state is mutated by would pile up
        // behind a blocked task thread.
        batcher.sendOutstanding();
        drainInFlight();
        // After the drain, never before it: the failures that reach the handler are discovered by
        // the drain, so flushing first would checkpoint past dead letters the drain is about to
        // produce.
        failedMutationHandler.flush();
    }

    @Override
    public void close() throws Exception {
        // No explicit flush here: on success Flink calls flush(true) before close. On the failure
        // path the writer applies no further records itself; note the batcher's own shutdown still
        // sends what is buffered inside it, which at-least-once tolerates as duplicates after the
        // restart.
        // Both counters back gauges a reporter may still sample between this call and the metric
        // group's own close, and nothing decrements them afterwards: the completions that would do
        // so run as mailbox mails, which no longer run once the task is torn down. So a writer
        // closed mid-flight would keep reporting mutations it will never wait for again. Zeroed
        // *before* closeAll rather than after it, because the batcher's shutdown throws a
        // BatchingException re-reporting every entry failure of its lifetime (#238) — which is
        // precisely the failure path this matters on, so a clear placed after the call would be
        // skipped exactly when it is needed. Same reason PubSubWriter.close() zeroes its parked
        // count and the BigQuery writers clear their in-flight maps.
        inFlightMutations = 0;
        inFlightBytes = 0;
        // Through closeAll, so the handler is closed even when the batcher's shutdown throws: the
        // lifecycle contract promises close on the failure path too.
        IOUtils.closeAll(Arrays.<AutoCloseable>asList(batcher, failedMutationHandler::close));
    }

    /** Releases one completed mutation from both in-flight counters. */
    private void releaseInFlight(int serializedSize) {
        inFlightMutations--;
        inFlightBytes -= serializedSize;
    }

    /**
     * Admission gate for {@link #write}: runs mailbox mails (mutation completions) until both
     * in-flight caps have room, surfacing any captured failure before the caller adds another
     * mutation.
     *
     * <p>Both predicates are "at or above the cap", never "would this mutation fit", so an empty
     * writer always admits — see the class documentation for why that is a correctness property and
     * not only accounting.
     */
    private void awaitCapacity() throws IOException, InterruptedException {
        while (inFlightMutations >= maxInFlightMutations || inFlightBytes >= maxInFlightBytes) {
            checkAsyncError();
            mailboxExecutor.yield();
        }
        checkAsyncError();
    }

    /**
     * Runs mailbox mails until <b>no</b> mutation is in flight, surfacing any captured failure —
     * including one processed by the final mail — before the caller proceeds.
     *
     * <p>This is a correctness primitive, not backpressure: it must stay independent of the
     * in-flight caps, since a completed checkpoint claims every record up to the barrier is
     * applied.
     *
     * <p>Keyed on the mutation count alone: a mutation can serialize to few bytes but never to a
     * count of zero, so {@code inFlightBytes == 0} would not imply an empty writer.
     */
    private void drainInFlight() throws IOException, InterruptedException {
        while (inFlightMutations > 0) {
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

    /** Task-thread handler for a failed mutation, run as a mailbox mail. */
    private void onMutationFailed(RowMutationEntry entry, int serializedSize, Throwable throwable) {
        releaseInFlight(serializedSize);
        // Every failure that gets here is counted, fatal ones included and fatal ones after the
        // first: the client has already spent its own retries, so each is a distinct give-up rather
        // than an attempt. Which means the sum over the transient codes is not this connector's
        // retry volume, unlike the Cloud Tasks sink's — the retries it would measure are inside the
        // SDK and never surface here.
        metrics.applyFailure(BigtableErrorClassifier.statusCode(throwable));
        if (BigtableErrorClassifier.classify(throwable) == BigtableErrorClassifier.Kind.ROW_LEVEL) {
            routeFailedMutation(entry, throwable);
        } else if (asyncError == null) {
            asyncError =
                    new IOException(
                            "A mutation of Bigtable table " + destination + " failed.", throwable);
        }
    }

    /**
     * Hands a row-level failure to the configured handler. Runs as a mailbox mail, so a handler
     * that fails the job cannot throw at a caller: its failure is captured into {@link #asyncError}
     * and rethrown from the next {@link #write} or {@link #flush}, exactly as a terminal failure
     * is. First failure wins, as everywhere else here.
     *
     * <p>Routing is <em>not</em> skipped once {@link #asyncError} is set. The writer is about to
     * fail either way, but this mutation really did fail terminally, and a dead-letter destination
     * missing it is worse than one holding a mutation a replay will produce again — the guarantee
     * is at-least-once.
     *
     * <p>The description does not name the table: every reader of it reaches the element's {@code
     * describeDestination()} too — the built-in handlers compose the two — so naming it here would
     * put the table in the sentence twice.
     */
    private void routeFailedMutation(RowMutationEntry entry, Throwable throwable) {
        metrics.mutationFailed();
        try {
            failedMutationHandler.handle(
                    FailedMutation.of(
                            destination,
                            entry,
                            "The mutation was rejected because "
                                    + BigtableErrorClassifier.ROW_LEVEL_REASON
                                    + ".",
                            throwable));
        } catch (IOException | RuntimeException e) {
            if (asyncError == null) {
                asyncError =
                        e instanceof IOException
                                ? (IOException) e
                                : new IOException(
                                        "The failed-mutation handler failed for Bigtable table "
                                                + destination
                                                + ".",
                                        e);
            }
        }
    }

    @VisibleForTesting
    int getInFlightMutations() {
        return inFlightMutations;
    }

    @VisibleForTesting
    long getInFlightBytes() {
        return inFlightBytes;
    }

    /**
     * Re-dispatches mutation completions onto the mailbox so state stays task-thread-only.
     *
     * <p>One instance per mutation: the callback carries the entry so a row-level failure can be
     * handed to the handler, and its serialized size so both in-flight counters can be released. It
     * is also its own success mail, so the success path allocates nothing beyond this object.
     */
    private final class MutationCallback
            implements ApiFutureCallback<Void>, ThrowingRunnable<Exception> {

        private final RowMutationEntry entry;
        private final int serializedSize;

        private MutationCallback(RowMutationEntry entry, int serializedSize) {
            this.entry = entry;
            this.serializedSize = serializedSize;
        }

        /** The success mail: runs on the task thread. */
        @Override
        public void run() {
            releaseInFlight(serializedSize);
        }

        @Override
        public void onSuccess(Void result) {
            mailboxExecutor.execute(this, completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> onMutationFailed(entry, serializedSize, throwable), failureDescription);
        }
    }
}
