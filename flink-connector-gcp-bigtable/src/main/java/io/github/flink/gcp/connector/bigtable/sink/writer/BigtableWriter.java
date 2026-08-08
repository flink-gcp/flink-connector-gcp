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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

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
 * record up to the barrier — other than those the serializer skipped by returning {@code null} —
 * and discarding operator state can never lose sink-buffered records. Checkpointing must be enabled
 * in streaming jobs; without it {@code flush()} never runs mid-stream and outstanding mutations are
 * lost on failure. Batch execution is covered by the end-of-input flush.
 *
 * <p>That guarantee assumes the default {@code failJob()} policy; what a successful checkpoint
 * means under a dropping policy is stated once on {@link FailureHandler}, and the per-mutation
 * failures below say which failures reach it.
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
 * service can never produce a dead letter. Drop-versus-throw semantics, the never-routed backlog
 * argument and the asynchronous capture of a handler failing inside a completion callback are
 * stated once on {@link FailureHandler}; here that capture lands in {@link #asyncError}.
 *
 * <p><b>A {@code ROW_LEVEL} verdict is confirmed solo before it is routed</b> (#239). Bigtable may
 * reject a whole {@code MutateRows} request rather than the entry that provoked it, and the client
 * then fails every entry of that batch with the same status — so routing on that report would hand
 * a batch to a dropping handler for one bad record. A verdict answering a batched submission is
 * therefore <em>parked</em> rather than routed, and {@link #runIsolationPass()} re-submits each
 * parked mutation as its own single-entry request: one that succeeds was collateral damage and is
 * now applied, one rejected again is the mutation the service really refused and reaches the
 * handler. The pass runs from {@link #flush(boolean)} and from {@link #write} — from both, so a
 * park cannot grow across a checkpoint interval — and it terminates because every submission inside
 * it is solo, which it bounds itself rather than assumes. Its cost is real: a stream whose
 * rejections are frequent spends roughly one request per record while isolating, which is what buys
 * back the records batched with a bad one. Under a dropping policy that degradation is bounded by
 * {@code BigtableWriterOptions.maxConsecutiveRejections} (#361): once that many confirmed
 * rejections arrive with no applied mutation between them, the stream's data is broken rather than
 * anomalous, and the writer fails the job instead of isolating it record by record. The bound is a
 * policy about the stream, accumulated across passes in {@link #consecutiveRejections}; the pass's
 * own loop budget is a per-pass invariant tripwire, and the two failures deliberately share no
 * message.
 *
 * <p>A failure that first surfaces during {@link #close()} reaches neither the handler nor {@link
 * #asyncError}: Flink quiesces the task mailbox before it closes operators, so a completion
 * callback's re-dispatch is rejected from there on. The batcher reports such a failure only inside
 * its accumulated shutdown report, which {@code DefaultMutationBatcherFactory} logs rather than
 * throws — throwing it would re-report every failure this writer had already routed, failing a job
 * the configured policy had kept running (#238).
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
    private final int maxConsecutiveRejections;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    private final BigtableWriterMetrics metrics;

    private final String completionDescription;
    private final String failureDescription;

    /** Number of mutations not yet acknowledged; touched only on the task thread. */
    private int inFlightMutations;

    /** Serialized size of the mutations not yet acknowledged; touched only on the task thread. */
    private long inFlightBytes;

    /**
     * Mutations whose rejection answered a batched submission and so names no entry, awaiting the
     * solo re-submission that gives each its own verdict (#239). Touched only on the task thread.
     *
     * <p>A deque so the pass can {@code poll()} one mutation at a time rather than iterate: its
     * opening drain runs mails that may park more, and those join the tail of the same pass instead
     * of needing another one.
     */
    private final Deque<ParkedMutation> pendingIsolation = new ArrayDeque<>();

    /**
     * Confirmed rejections routed since the last successfully applied mutation; touched only on the
     * task thread. Every success mail zeroes it, and {@code
     * BigtableWriterOptions.maxConsecutiveRejections} — whose javadoc carries the reasoning — is
     * what it is compared against (#361).
     */
    private int consecutiveRejections;

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
        // Re-checked for the same deserialization reason, though the failure mode is milder: a
        // zero would fail the job on the first confirmed rejection, silently overriding the
        // handler the user configured, rather than hanging anything.
        Preconditions.checkArgument(
                options.getMaxConsecutiveRejections() > 0
                        || options.getMaxConsecutiveRejections() == BigtableWriterOptions.UNBOUNDED,
                "maxConsecutiveRejections must be positive or -1 (unbounded)");
        this.maxInFlightMutations = options.getMaxInFlightMutations();
        this.maxInFlightBytes = options.getMaxInFlightBytes();
        this.maxConsecutiveRejections = options.getMaxConsecutiveRejections();
        this.failedMutationHandler = config.getFailedMutationHandler();
        this.metrics = new BigtableWriterMetrics(metricGroup);
        this.metrics.bindWriterState(
                this::getInFlightMutations, this::getInFlightBytes, this::getParkedMutations);
        this.completionDescription = "Complete a Bigtable mutation of " + destination;
        this.failureDescription = "Fail a Bigtable mutation of " + destination;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        // Before this record is even serialized, and this is what bounds the park at all: parking
        // happens in completion mails, mails run only inside a yield, and every park releases one
        // mutation from the in-flight counters — so between two writes at most maxInFlightMutations
        // can accumulate. Isolating only at the checkpoint barrier would instead let a stream of
        // rejections pile the whole interval's worth of mutations into the writer's heap, since a
        // parked mutation is counted by neither in-flight bound.
        if (!pendingIsolation.isEmpty()) {
            runIsolationPass();
        }
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
            // Skip by contract, not a failure. Counted, because nothing else reports it: a
            // serializer skipping every record leaves an empty table under a green job.
            metrics.recordSkipped();
            return;
        }
        // Not memoized by the entry, which builds a fresh proto per call, so it is taken once here
        // and carried by the callback: it is both the byte counter's unit and the metric's.
        int serializedSize = entry.toProto().getSerializedSize();
        awaitCapacity();
        submit(entry, serializedSize, true, false);
    }

    /**
     * Hands a mutation to the batcher, counts it in flight and registers its completion callback.
     *
     * <p>{@code firstAttempt} is what keeps {@code numRecordsSend} a count of <em>records</em>: the
     * isolation pass re-submits a parked mutation, and a record must be counted once however many
     * requests it took. The in-flight counters are the opposite — they track submissions, so they
     * are adjusted on every call.
     *
     * <p>{@code soloVerdict} says the mutation travels as its own single-entry {@code MutateRows}
     * request — true only inside {@link #runIsolationPass()}, which empties the batcher's
     * accumulator and drains around each submission — so an {@code INVALID_ARGUMENT} answering it
     * concerns this mutation alone and may be routed to the failure handler. From any other
     * submission that status may be a request-level rejection the client fans out over the whole
     * batch, so it must be isolated first, not routed (#239).
     */
    private void submit(
            RowMutationEntry entry, int serializedSize, boolean firstAttempt, boolean soloVerdict)
            throws IOException {
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
        if (firstAttempt) {
            metrics.mutationSent(serializedSize);
        }
        ApiFutures.addCallback(
                future, new MutationCallback(entry, serializedSize, soloVerdict), Runnable::run);
    }

    /**
     * Gives every parked mutation its own verdict, by re-submitting each as the only entry of its
     * request (#239).
     *
     * <p>The opening {@code sendOutstanding()} is what makes the submissions below solo: gax's
     * batcher accumulates into one open batch and swaps it out on that call, so a mutation added to
     * an emptied accumulator and flushed at once travels alone. It is also what lets the drain
     * after it finish at all — an entry still sitting in the accumulator is counted in flight and
     * its future cannot complete until a request carries it.
     *
     * <p>Consumed with {@code poll()} rather than iterated: the opening drain runs completion mails
     * that may park further mutations, and those are picked up by this same loop. Nothing parks
     * during the per-mutation drains, since the only submission in flight there is solo and a solo
     * verdict is routed rather than parked.
     *
     * <p>That last sentence is the loop's termination argument, and it lives in {@link
     * #onMutationFailed} rather than here — so the loop is <b>bounded by the park's size</b> and
     * raises rather than spins if the invariant is ever broken. The failure it converts is the
     * worst one this writer could have: a task thread inside a mailbox loop that no completion can
     * end is invisible to every timeout Flink has, and returning quietly instead would let a
     * checkpoint complete over mutations that were neither applied nor routed.
     *
     * <p>A failure raised here — a fatal status surfaced by a drain, a batcher refusing the
     * submission — abandons the remainder of the park, and the mutation being isolated with it:
     * neither applied nor routed. That is safe for the same reason {@link #close()}'s discard is,
     * and it is why the throw must not be swallowed: the checkpoint does not complete, so the
     * restart replays those records.
     */
    private void runIsolationPass() throws IOException, InterruptedException {
        batcher.sendOutstanding();
        drainInFlight();
        for (int budget = pendingIsolation.size(); budget > 0; budget--) {
            ParkedMutation parked = pendingIsolation.poll();
            submit(parked.entry, parked.serializedSize, false, true);
            batcher.sendOutstanding();
            drainInFlight();
        }
        if (!pendingIsolation.isEmpty()) {
            throw new IllegalStateException(
                    "A solo re-submission to Bigtable table "
                            + destination
                            + " was parked again instead of being routed, which cannot happen"
                            + " unless the isolation contract has been broken; "
                            + pendingIsolation.size()
                            + " mutation(s) would never get a verdict.");
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        // sendOutstanding rather than the batcher's own blocking flush: waiting has to happen on
        // the mailbox, or the completion mails this writer's state is mutated by would pile up
        // behind a blocked task thread.
        batcher.sendOutstanding();
        drainInFlight();
        // The handler's flush comes last, and the two steps before it are what it must not run
        // ahead of: the drain is what discovers this checkpoint's failures, and the pass is what
        // turns the batched ones among them into dead letters. Flushing earlier would checkpoint
        // past a dead letter one of them was about to produce.
        if (!pendingIsolation.isEmpty()) {
            runIsolationPass();
        }
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
        // *before* closeAll rather than after it, because either close below can still throw — the
        // client's own shutdown, an InterruptedException from the batcher's wait, the handler's
        // close — and a mid-flight teardown is precisely when both this clear and those failures
        // happen, so a clear placed after the call would be skipped exactly when it is needed.
        // Same reason PubSubWriter.close() zeroes its parked count and the BigQuery writers clear
        // their in-flight maps. The park is cleared for the gauge it backs and for the heap it
        // holds; the mutations in it are neither applied nor routed, which at-least-once covers —
        // no checkpoint completed with them parked, so the restart replays those records.
        inFlightMutations = 0;
        inFlightBytes = 0;
        pendingIsolation.clear();
        // Through Closers.closeAll, so the handler is closed even when the batcher's shutdown
        // throws: the lifecycle contract promises close on the failure path too.
        Closers.closeAll(batcher, failedMutationHandler::close);
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
    private void onMutationFailed(
            RowMutationEntry entry, int serializedSize, boolean soloVerdict, Throwable throwable) {
        releaseInFlight(serializedSize);
        BigtableErrorClassifier.Kind kind = BigtableErrorClassifier.classify(throwable);
        if (kind == BigtableErrorClassifier.Kind.ROW_LEVEL && !soloVerdict) {
            // The status may answer the request rather than this entry, so this mutation is not
            // known to be the invalid one. Park it for the isolation pass, which re-submits it
            // alone — routing here would drop a whole batch for one bad record (#239).
            //
            // Returning before the counter is the other half: the client reports one request-level
            // status against every co-batched entry, so counting them all would multiply a single
            // incident by the batch size. The pass counts the true rejections when it confirms
            // them. Pub/Sub excludes its cascades from publishFailure for the same reason.
            pendingIsolation.add(new ParkedMutation(entry, serializedSize));
            return;
        }
        // Every failure with a confirmed identity is counted, fatal ones included and fatal ones
        // after the first: the client has already spent its own retries, so each is a distinct
        // give-up rather than an attempt. Which means the sum over the transient codes is not this
        // connector's retry volume, unlike the Cloud Tasks sink's — the retries it would measure
        // are inside the SDK and never surface here.
        metrics.applyFailure(BigtableErrorClassifier.statusCode(throwable));
        if (kind == BigtableErrorClassifier.Kind.ROW_LEVEL) {
            routeFailedMutation(entry, throwable);
        } else if (asyncError == null) {
            asyncError =
                    new IOException(
                            "A mutation of Bigtable table " + destination + " failed.", throwable);
        }
    }

    /**
     * Hands a row-level failure to the configured handler. Reached only with a solo verdict — an
     * {@code INVALID_ARGUMENT} answering a single-entry request of the isolation pass — so the
     * mutation really is the one the service rejected. Runs as a mailbox mail, so a handler that
     * fails the job cannot throw at a caller: its failure is captured into {@link #asyncError} and
     * rethrown from the next {@link #write} or {@link #flush}, exactly as a terminal failure is.
     * First failure wins, as everywhere else here.
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
        consecutiveRejections++;
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
        // After the routing, not instead of it: the mutation that tripped the bound really was
        // refused, and a dead-letter destination missing it would be worse than one holding it —
        // the same argument as routing beside an existing asyncError. First failure still wins.
        if (maxConsecutiveRejections != BigtableWriterOptions.UNBOUNDED
                && consecutiveRejections >= maxConsecutiveRejections
                && asyncError == null) {
            String run =
                    consecutiveRejections == 1
                            ? "refused a mutation (status "
                                    + BigtableErrorClassifier.statusCode(throwable)
                                    + ")"
                            : "refused "
                                    + consecutiveRejections
                                    + " mutations in a row (the last with status "
                                    + BigtableErrorClassifier.statusCode(throwable)
                                    + ") with none applied between them";
            asyncError =
                    new IOException(
                            "Bigtable table "
                                    + destination
                                    + " "
                                    + run
                                    + ", reaching maxConsecutiveRejections("
                                    + maxConsecutiveRejections
                                    + "): the stream's data looks broken rather than anomalous, so"
                                    + " the job fails instead of isolating it record by record."
                                    + " Every rejected mutation, this one included, was routed to"
                                    + " the configured handler first;"
                                    + " BigtableWriterOptions.builder().maxConsecutiveRejections(-1)"
                                    + " removes this bound.",
                            throwable);
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

    @VisibleForTesting
    int getParkedMutations() {
        return pendingIsolation.size();
    }

    /** A mutation held for the isolation pass, with the size both in-flight counters release by. */
    private static final class ParkedMutation {

        private final RowMutationEntry entry;
        private final int serializedSize;

        private ParkedMutation(RowMutationEntry entry, int serializedSize) {
            this.entry = entry;
            this.serializedSize = serializedSize;
        }
    }

    /**
     * Re-dispatches mutation completions onto the mailbox so state stays task-thread-only.
     *
     * <p>One instance per submission: the callback carries the entry so a row-level failure can be
     * handed to the handler or parked, its serialized size so both in-flight counters can be
     * released, and whether the submission was solo so a row-level status can be told from a
     * request-level one fanned out over a batch. It is also its own success mail, so the success
     * path allocates nothing beyond this object.
     */
    private final class MutationCallback
            implements ApiFutureCallback<Void>, ThrowingRunnable<Exception> {

        private final RowMutationEntry entry;
        private final int serializedSize;
        private final boolean soloVerdict;

        private MutationCallback(RowMutationEntry entry, int serializedSize, boolean soloVerdict) {
            this.entry = entry;
            this.serializedSize = serializedSize;
            this.soloVerdict = soloVerdict;
        }

        /** The success mail: runs on the task thread. */
        @Override
        public void run() {
            releaseInFlight(serializedSize);
            // An applied mutation is evidence the stream is not wholly broken, whichever request
            // shape carried it — a solo re-submission included — so the consecutive-rejection
            // bound resets on every success.
            consecutiveRejections = 0;
        }

        @Override
        public void onSuccess(Void result) {
            mailboxExecutor.execute(this, completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> onMutationFailed(entry, serializedSize, soloVerdict, throwable),
                    failureDescription);
        }
    }
}
