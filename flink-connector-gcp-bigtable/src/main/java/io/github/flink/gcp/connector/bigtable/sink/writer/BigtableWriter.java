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
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.tables.TableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * At-least-once writer applying row mutations to the Bigtable tables a {@link DestinationResolver}
 * names.
 *
 * <h2>Threading model</h2>
 *
 * <p>All mutable state — the destination pool, the in-flight counters and the captured asynchronous
 * error — is touched only on the task thread. Mutation completion callbacks do not mutate state
 * directly; they re-dispatch onto the {@link MailboxExecutor}, whose mails run on the task thread
 * inside {@link MailboxExecutor#yield()} calls. This is the model the Pub/Sub sink's writer uses.
 *
 * <h2>Destinations</h2>
 *
 * <p>A bulk mutation batcher is bound to one table, so the writer holds one per destination, built
 * on the first record routed there and dropped once the destination has been idle for {@code
 * BigtableWriterOptions.destinationIdleTimeout}. Under those batchers sits one client per (project,
 * instance), which the {@link MutationBatcherFactory} owns and shares.
 *
 * <p>The destination is resolved <em>before</em> the record is serialized, which is what lets a
 * record the serializer rejects be reported against the table it was headed for; the null-skip
 * check still sits ahead of the pool, so a skipped record opens no batcher. A resolver returning
 * {@code null} fails the write rather than reaching the failure handler: it is a configuration
 * failure, not a bad record, and routing it would let a dropping policy write nothing at all under
 * a green job.
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
 * it is solo <em>on every batcher</em>, which it bounds itself rather than assumes. Its cost is
 * real: a stream whose rejections are frequent spends roughly one request per record while
 * isolating, which is what buys back the records batched with a bad one. Under a dropping policy
 * that degradation is bounded by {@code BigtableWriterOptions.maxConsecutiveRejections} (#361):
 * once that many confirmed rejections arrive with no applied mutation between them — on any table,
 * since a broken stream is a property of the stream and not of one destination — the stream's data
 * is broken rather than anomalous, and the writer fails the job instead of isolating it record by
 * record. The bound is a policy about the stream, accumulated across passes in {@link
 * #consecutiveRejections}; the pass's own loop budget is a per-pass invariant tripwire, and the two
 * failures deliberately share no message.
 *
 * <h2>Table auto-creation</h2>
 *
 * <p>Under {@code CreateDisposition.CREATE_IF_NEEDED} a mutation failing {@code NOT_FOUND} — the
 * table or one of its column families does not exist — is <em>parked</em> into {@link
 * #pendingRepair} rather than failing the job, and {@link #runRepair()} repairs the incident from
 * the next {@link #write} or {@link #flush(boolean)}: it drains the writer, ensures every table
 * that reported itself missing and its declared families exist through the {@link TableAdmin},
 * re-applies the parked mutations, and retries on a jittered backoff schedule until they land or
 * {@code BigtableWriterOptions.recoveryMaxAttempts} is spent. The disposition gates the
 * <em>parking</em>, unlike the Pub/Sub writer's (where a cascade behind a dropped ordered message
 * must be parked whatever the disposition): this writer has no ordering keys and no cascades, so
 * under {@code CREATE_NEVER} a {@code NOT_FOUND} is simply fatal, with the disposition named in the
 * failure. {@link #tablesMissing} carries the repair's reason — added to only where a {@code
 * NOT_FOUND} is parked, consumed per table per attempt, so the admin is called only for a table
 * that actually reported itself missing, and not again once its ensure has succeeded.
 *
 * <p>One repair covers every table parked at the time, and its budget is shared: a mutation naming
 * a column family {@code tableCreateOptions} does not declare can never be repaired, and spending
 * the budget on it now fails the job while other tables' parked work is still outstanding. That
 * work is neither applied nor routed, which at-least-once covers for the reason {@link #close()}'s
 * discard is covered — no checkpoint completed with it parked.
 *
 * <p>A failure that first surfaces during {@link #close()} reaches neither the handler nor {@link
 * #asyncError}: Flink quiesces the task mailbox before it closes operators, so a completion
 * callback's re-dispatch is rejected from there on. A batcher reports such a failure only inside
 * its accumulated shutdown report, which {@code DefaultMutationBatcherFactory} logs rather than
 * throws — throwing it would re-report every failure this writer had already routed, failing a job
 * the configured policy had kept running (#238).
 *
 * <p>Unacknowledged mutations are capped along both dimensions that bound memory: their number
 * ({@code BigtableWriterOptions.maxInFlightMutations}, default 1000) and their serialized size
 * ({@code BigtableWriterOptions.maxInFlightBytes}, default 64 MiB). Both are the <em>writer's</em>,
 * summed across every destination rather than shared out among them: that is what keeps {@link
 * #drainInFlight()} meaning "the writer is empty" and keeps the park bound in {@link #write} a
 * single number. At either cap {@link #write} yields to the mailbox until completions bring the
 * counters back down. Admission is checked before a mutation rather than against the mutation's own
 * size, so one larger than the cap is admitted on an empty writer and overshoots it until it
 * completes — deliberate, because {@link MailboxExecutor#yield()} blocks until a mail arrives and
 * no mail can arrive with nothing in flight, so a "does it fit" predicate would be a task hang
 * rather than backpressure.
 *
 * <p>What the writer retains is therefore one cap's worth of in-flight mutations, at most one cap's
 * worth parked, and one accumulator per live batcher — the third term being what per-record
 * destinations added.
 *
 * <p>These caps have to be reached before the client's own flow controller, which blocks the
 * calling thread instead of yielding; {@code BigtableWriterOptions} records why that is a
 * constraint on raising them rather than a knob.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigtableWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableWriter.class);

    private final BigtableSinkConfig<T> config;
    private final DestinationResolver<? super T> destinationResolver;
    private final MutationBatcherFactory batcherFactory;
    private final TableAdmin tableAdmin;
    private final MailboxExecutor mailboxExecutor;
    private final int maxInFlightMutations;
    private final long maxInFlightBytes;
    private final int maxConsecutiveRejections;
    private final long destinationIdleTimeoutNanos;
    private final LongSupplier nanoClock;
    private final RetrySchedule recoverySchedule;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    private final BigtableWriterMetrics metrics;

    /**
     * The batcher and bookkeeping of each table written to, built on that table's first record;
     * touched only on the task thread.
     *
     * <p>Insertion-ordered so a teardown reports the first failure of a deterministic sequence, and
     * so an eviction sweep visits tables in the order they were first written to.
     */
    private final Map<TableDestination, DestinationState> states = new LinkedHashMap<>();

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
     *
     * <p>One queue for every destination, each entry naming its own: the pass's property is that a
     * submission travels alone across the <em>writer</em>, which is a fact about all the batchers
     * at once and not about any one of them.
     */
    private final Deque<ParkedMutation> pendingIsolation = new ArrayDeque<>();

    /**
     * Mutations that failed {@code NOT_FOUND} under {@code CREATE_IF_NEEDED}, awaiting the repair
     * that creates what is missing and re-applies them. Touched only on the task thread.
     *
     * <p>Its non-emptiness is the repair trigger itself — the Pub/Sub writer's separate {@code
     * repairNeeded} flag exists for a repair with nothing parked (a dropped ordered message's
     * paused key), which having no ordering keys cannot produce.
     */
    private final Deque<ParkedMutation> pendingRepair = new ArrayDeque<>();

    /**
     * The tables a parked {@code NOT_FOUND} said were missing (the table itself, or a family of it)
     * since the repair last checked. The only thing that makes {@link #runRepair()} call the admin
     * for a table: everything else the repair does — re-applying the parked batch on a backoff — is
     * needed whatever parked it, and a repair that has not seen a {@code NOT_FOUND} for a table
     * must not issue a creation for it. Touched only on the task thread.
     */
    private final Set<TableDestination> tablesMissing = new LinkedHashSet<>();

    /**
     * The failure that parked the current repair's first mutation, carried so a repair that
     * exhausts its budget can name what actually went wrong; nulled when a repair completes so it
     * cannot be reported as some later incident's cause. First wins rather than last, as everywhere
     * else here — with several tables in one repair, the last would be whichever mail happened to
     * arrive latest. Touched only on the task thread.
     */
    private Throwable repairCause;

    /**
     * Confirmed rejections routed since the last successfully applied mutation, across every
     * destination; touched only on the task thread. Every success mail zeroes it, and {@code
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
     * @param batcherFactory the factory building one batcher per destination; closed with the
     *     writer, after every batcher it created
     * @param tableAdmin the table admin the auto-creation repair creates through; closed with the
     *     writer
     * @param mailboxExecutor the task mailbox, used to run mutation completions on the task thread
     * @param metricGroup the writer's metric group, which {@link BigtableWriterMetrics} registers
     *     this sink's counters and gauges on
     */
    public BigtableWriter(
            BigtableSinkConfig<T> config,
            MutationBatcherFactory batcherFactory,
            TableAdmin tableAdmin,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        this(
                config,
                batcherFactory,
                tableAdmin,
                mailboxExecutor,
                metricGroup,
                config.getWriterOptions().toRecoverySchedule(),
                System::nanoTime);
    }

    /**
     * Creates the writer with an explicit recovery schedule and clock, so tests can shrink the
     * production backoffs out of the wall clock and fast-forward the idle timeout.
     */
    @VisibleForTesting
    BigtableWriter(
            BigtableSinkConfig<T> config,
            MutationBatcherFactory batcherFactory,
            TableAdmin tableAdmin,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup,
            RetrySchedule recoverySchedule,
            LongSupplier nanoClock) {
        this.config = config;
        this.destinationResolver = config.getDestinationResolver();
        this.batcherFactory = batcherFactory;
        this.tableAdmin = tableAdmin;
        this.recoverySchedule = recoverySchedule;
        this.nanoClock = nanoClock;
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
        // The builder's cross-check, re-stated for the same deserialization reason. Failing here
        // beats the alternative — an NPE inside the repair, at the first moment a table actually
        // goes missing, attributed to the creation rather than to the configuration.
        Preconditions.checkArgument(
                config.getCreateDisposition() != CreateDisposition.CREATE_IF_NEEDED
                        || config.getTableCreateOptions() != null,
                "createDisposition is CREATE_IF_NEEDED but tableCreateOptions is null");
        this.maxInFlightMutations = options.getMaxInFlightMutations();
        this.maxInFlightBytes = options.getMaxInFlightBytes();
        this.maxConsecutiveRejections = options.getMaxConsecutiveRejections();
        // The setter bounds this at what a nanosecond clock can express (ADR-0068), which is what
        // keeps its own "set a very large duration to never evict" instruction from throwing here
        // — on a TaskManager, as the job starts, naming neither the knob nor the value. Unlike the
        // three checks above it gets no re-check for a deserialized options object that bypassed
        // the builder: those failures are silent (a permanent park) or misattributed (an NPE
        // inside the repair), where this one is an immediate throw at construction.
        this.destinationIdleTimeoutNanos = options.getDestinationIdleTimeout().toNanos();
        this.failedMutationHandler = config.getFailedMutationHandler();
        this.metrics = new BigtableWriterMetrics(metricGroup, options.isPerDestinationMetrics());
        this.metrics.bindWriterState(
                this::getInFlightMutations, this::getInFlightBytes, this::getParkedMutations);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        // The repair runs before the isolation pass, and the order is load-bearing both ways: the
        // pass submits solo requests, and against a still-missing table each would fail NOT_FOUND
        // and migrate to the repair queue at a cost of one round trip apiece — repairing first
        // restores the tables they need. And a repair's own re-submissions can park
        // INVALID_ARGUMENT entries for isolation, which the pass right after it then confirms.
        // Neither loop drains the other's queue.
        if (!pendingRepair.isEmpty()) {
            runRepair();
        }
        // Before this record is even serialized, and this is what bounds the parks at all: parking
        // happens in completion mails, mails run only inside a yield, and every park releases one
        // mutation from the in-flight counters — so between two writes at most maxInFlightMutations
        // can accumulate, across both queues and every destination. Isolating only at the
        // checkpoint barrier would instead let a stream of rejections pile the whole interval's
        // worth of mutations into the writer's heap, since a parked mutation is counted by neither
        // in-flight bound.
        if (!pendingIsolation.isEmpty()) {
            runIsolationPass();
        }
        TableDestination destination = destinationResolver.resolve(element, context);
        if (destination == null) {
            // Not routed to the failure handler: a resolver returning null is a configuration
            // failure rather than a bad record, and routing it would let a dropping policy write
            // nothing at all under a green job. The Pub/Sub writer draws the line in the same
            // place.
            throw new IOException("The destination resolver returned null for a record.");
        }
        RowMutationEntry entry;
        try {
            entry = config.getSerializer().serialize(element, context);
        } catch (IOException | RuntimeException e) {
            // The record never became a mutation, so there is nothing to carry but the destination
            // — which is why the resolve above runs first: FailedMutation.getPayloadBytes() is
            // null, as the shared contract prescribes, but its destination is not nullable. Handled
            // on the task thread, so a handler that fails the job throws at the caller directly.
            // Counted before the handler runs, because the counter says "routed", not "dropped" —
            // a handler that fails the job routed the record just as one that discarded it did.
            // Not counted under errorClass: a serialization failure carries no status.
            //
            // The per-table counter is looked up rather than read off a DestinationState: there is
            // none, and there must not be — a poison record must reach the handler without opening
            // a batcher for a table that may never receive a mutation.
            metrics.mutationFailed(metrics.forTable(destination));
            failedMutationHandler.handle(
                    FailedMutation.of(destination, null, "The record could not be serialized.", e));
            return;
        }
        if (entry == null) {
            // Skip by contract, not a failure. Counted, because nothing else reports it: a
            // serializer skipping every record leaves an empty table under a green job. Ahead of
            // stateFor(...), so a record written nowhere opens no batcher.
            metrics.recordSkipped();
            return;
        }
        DestinationState state = stateFor(destination);
        state.lastAccessNanos = nanoClock.getAsLong();
        // Not memoized by the entry, which builds a fresh proto per call, so it is taken once here
        // and carried by the callback: it is both the byte counter's unit and the metric's. What
        // that construction costs, and that no local route avoids it, are measured in ADR-0041 —
        // read those numbers before treating this line as an optimisation target.
        int serializedSize = entry.toProto().getSerializedSize();
        awaitCapacity();
        submit(state, entry, serializedSize, true, false);
    }

    /**
     * Returns the destination's state, building its batcher on first use.
     *
     * <p>Built and then put, so a failure leaves no half-populated entry and the next record routed
     * here retries the creation. The failure is thrown at the caller rather than routed: a client
     * that cannot be built is a configuration or credentials failure, not a bad record.
     */
    private DestinationState stateFor(TableDestination destination) throws IOException {
        DestinationState state = states.get(destination);
        if (state == null) {
            MutationBatcher batcher;
            try {
                batcher = batcherFactory.create(destination);
            } catch (IOException | RuntimeException e) {
                throw new IOException(
                        "Failed to create a Bigtable mutation batcher for table "
                                + destination
                                + ".",
                        e);
            }
            state = new DestinationState(destination, batcher, nanoClock.getAsLong());
            states.put(destination, state);
        }
        return state;
    }

    /**
     * Hands a mutation to its destination's batcher, counts it in flight and registers its
     * completion callback.
     *
     * <p>{@code firstAttempt} is what keeps {@code numRecordsSend} a count of <em>records</em>: the
     * isolation pass re-submits a parked mutation, and a record must be counted once however many
     * requests it took. The in-flight counters are the opposite — they track submissions, so they
     * are adjusted on every call.
     *
     * <p>{@code soloVerdict} says the mutation travels as its own single-entry {@code MutateRows}
     * request — true only inside {@link #runIsolationPass()}, which empties every batcher's
     * accumulator and drains around each submission — so an {@code INVALID_ARGUMENT} answering it
     * concerns this mutation alone and may be routed to the failure handler. From any other
     * submission that status may be a request-level rejection the client fans out over the whole
     * batch, so it must be isolated first, not routed (#239).
     */
    private void submit(
            DestinationState state,
            RowMutationEntry entry,
            int serializedSize,
            boolean firstAttempt,
            boolean soloVerdict)
            throws IOException {
        ApiFuture<Void> future;
        try {
            future = state.batcher.add(entry);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to hand a mutation to the Bigtable batcher for table "
                            + state.destination
                            + ".",
                    e);
        }
        // Counted only once the mutation is accepted: a synchronous throw registers no callback, so
        // nothing would ever release it.
        inFlightMutations++;
        inFlightBytes += serializedSize;
        if (firstAttempt) {
            metrics.mutationSent(state.metrics, serializedSize);
        }
        ApiFutures.addCallback(
                future,
                new MutationCallback(state, entry, serializedSize, soloVerdict),
                Runnable::run);
    }

    /**
     * Gives every parked mutation its own verdict, by re-submitting each as the only entry of its
     * request (#239).
     *
     * <p>The opening {@code sendOutstanding()} — over <b>every</b> live batcher, not only the ones
     * holding parked work — is what makes the submissions below solo: gax's batcher accumulates
     * into one open batch and swaps it out on that call, so a mutation added to an emptied
     * accumulator and flushed at once travels alone. It is also what lets the drain after it finish
     * at all — an entry still sitting in some accumulator is counted in flight and its future
     * cannot complete until a request carries it. Missing one batcher fails in one of two ways,
     * neither of which names the cause: the drain never reaches zero and the task thread parks
     * inside {@code yield()} forever, or gax's delay-threshold timer sends that entry as a batch of
     * its own, whose rejection parks after this pass's budget was taken and trips the tripwire
     * below on a healthy stream.
     *
     * <p>The per-mutation send is that mutation's batcher alone, which is safe for a reason worth
     * writing down: {@link #submit} is the only thing that puts an entry into an accumulator, and
     * inside this loop it is called once per iteration, so no other batcher can have accumulated
     * anything since the opening send.
     *
     * <p>Consumed with {@code poll()} rather than iterated: the opening drain runs completion mails
     * that may park further mutations, and those are picked up by this same loop. Nothing parks
     * <em>for isolation</em> during the per-mutation drains: the only submission in flight there is
     * solo, and a solo verdict is routed, made fatal, or — a {@code NOT_FOUND} under {@code
     * CREATE_IF_NEEDED}, a table vanishing mid-pass — migrated to {@link #pendingRepair}, never
     * parked for isolation again.
     *
     * <p>That last sentence is the loop's termination argument, and it lives in {@link
     * #onMutationFailed} rather than here — so the loop is <b>bounded by the park's size</b> and
     * raises rather than spins if the invariant is ever broken. The failure it converts is the
     * worst one this writer could have: a task thread inside a mailbox loop that no completion can
     * end stops the subtask where nothing Flink reports names the invariant that broke — the
     * symptoms are checkpoints that stop completing and a subtask that will not cancel, neither of
     * which points here — and returning quietly instead would let a checkpoint complete over
     * mutations that were neither applied nor routed.
     *
     * <p>A failure raised here — a fatal status surfaced by a drain, a batcher refusing the
     * submission — abandons the remainder of the park, and the mutation being isolated with it:
     * neither applied nor routed. That is safe for the same reason {@link #close()}'s discard is,
     * and it is why the throw must not be swallowed: the checkpoint does not complete, so the
     * restart replays those records.
     */
    private void runIsolationPass() throws IOException, InterruptedException {
        sendEveryBatcher();
        drainInFlight();
        for (int budget = pendingIsolation.size(); budget > 0; budget--) {
            ParkedMutation parked = pendingIsolation.poll();
            DestinationState state = stateFor(parked.destination);
            submit(state, parked.entry, parked.serializedSize, false, true);
            state.batcher.sendOutstanding();
            drainInFlight();
        }
        if (!pendingIsolation.isEmpty()) {
            throw new IllegalStateException(
                    "A solo re-submission to Bigtable "
                            + describe(destinationsOf(pendingIsolation))
                            + " was parked again instead of being routed, which cannot happen"
                            + " unless the isolation contract has been broken; "
                            + pendingIsolation.size()
                            + " mutation(s) would never get a verdict.");
        }
    }

    /**
     * Repairs a missing-table incident: ensures every table that reported itself missing, and its
     * declared families, exist, then re-applies the parked mutations, retrying on the jittered
     * recovery schedule until they land or the budget is spent.
     *
     * <p>The opening drain is what makes re-parked failures attributable to the re-submissions —
     * and, before anything is re-applied, what surfaces a fatal root through {@code
     * checkAsyncError} so a cascade of one is never re-applied over it. Once a table's ensure has
     * succeeded it is not repeated within the repair, but the decision is re-taken per table per
     * attempt off {@link #tablesMissing}: a batch parked while a creation was still propagating
     * re-parks with its table named again, and the set consumed here is what keeps a later incident
     * from inheriting this repair's answer. An ensure that <em>fails</em> spends an attempt from
     * the same schedule instead of failing the job: the admin client retries neither of its RPCs,
     * so this loop is the only thing standing between one transient admin failure and a restart —
     * and with the budget spent, the ensure's own failure is what surfaces. A failure part way
     * through the set leaves the tables it did not reach owed, so the next attempt ensures them
     * rather than re-applying against a table nothing created.
     *
     * <p>Re-submissions are exempt from {@link #awaitCapacity()}: the park can never exceed {@code
     * maxInFlightMutations} (the bound argument in {@link #write}), so re-applying it wholesale
     * peaks at one cap's worth. An entry that fails {@code INVALID_ARGUMENT} here parks for the
     * isolation pass, not this loop, which is why the loop keys on its own queue alone.
     *
     * <p>Budget exhaustion names the one condition creation cannot repair: a mutation writing to a
     * family {@code tableCreateOptions} does not declare fails {@code NOT_FOUND} forever, since the
     * sink creates only the declared families. One such table spends the budget the whole repair
     * shares, which is the cost of one repair over every parked table rather than one per table.
     */
    private void runRepair() throws IOException, InterruptedException {
        sendEveryBatcher();
        drainInFlight();
        Set<TableDestination> ensured = new LinkedHashSet<>();
        for (int attempt = 1; ; attempt++) {
            if (!tablesMissing.isEmpty()) {
                List<TableDestination> owed = new ArrayList<>(tablesMissing);
                tablesMissing.clear();
                for (int i = 0; i < owed.size(); i++) {
                    TableDestination table = owed.get(i);
                    if (!ensured.add(table)) {
                        continue;
                    }
                    LOG.info(
                            "A mutation of Bigtable table {} failed because the table or a column"
                                    + " family does not exist; creating what is missing"
                                    + " (CREATE_IF_NEEDED).",
                            table);
                    TableAdmin.EnsureResult result;
                    try {
                        result = tableAdmin.ensureTable(table, config.getTableCreateOptions());
                    } catch (IOException e) {
                        if (attempt >= recoverySchedule.maxAttempts()) {
                            throw e;
                        }
                        // The creation is still owed — for this table and for every one this pass
                        // has not reached, or they would silently wait for a later NOT_FOUND to
                        // name them again while this repair spent its budget re-applying to a
                        // table nothing had created.
                        ensured.remove(table);
                        tablesMissing.addAll(owed.subList(i, owed.size()));
                        long ensureBackoffMs = recoverySchedule.backoffMs(attempt);
                        LOG.info(
                                "Creating Bigtable table {} or its column families failed (attempt"
                                        + " {} of {}); retrying in {} ms.",
                                table,
                                attempt,
                                recoverySchedule.maxAttempts(),
                                ensureBackoffMs,
                                e);
                        Thread.sleep(ensureBackoffMs);
                        break;
                    }
                    if (result.tableCreated()) {
                        metrics.tableCreated();
                    }
                    if (result.columnFamiliesAdded() > 0) {
                        metrics.columnFamiliesAdded(result.columnFamiliesAdded());
                    }
                }
                if (!tablesMissing.isEmpty()) {
                    // An ensure failed and spent this attempt; the ones it did not reach are owed
                    // again above.
                    continue;
                }
            }
            for (int budget = pendingRepair.size(); budget > 0; budget--) {
                ParkedMutation parked = pendingRepair.poll();
                submit(
                        stateFor(parked.destination),
                        parked.entry,
                        parked.serializedSize,
                        false,
                        false);
            }
            sendEveryBatcher();
            drainInFlight();
            if (pendingRepair.isEmpty()) {
                repairCause = null;
                return;
            }
            if (attempt >= recoverySchedule.maxAttempts()) {
                throw new IOException(
                        "Re-applying mutations to Bigtable "
                                + describe(destinationsOf(pendingRepair))
                                + " kept failing after creating the table(s) and their declared"
                                + " column families ("
                                + attempt
                                + " attempt(s)); "
                                + pendingRepair.size()
                                + " mutation(s) are still failing. A mutation naming a column"
                                + " family absent from tableCreateOptions cannot be repaired: the"
                                + " sink creates only the declared families.",
                        repairCause);
            }
            long backoffMs = recoverySchedule.backoffMs(attempt);
            LOG.info(
                    "Re-applying {} mutation(s) to Bigtable {} still failed (attempt {} of"
                            + " {}); retrying in {} ms.",
                    pendingRepair.size(),
                    describe(destinationsOf(pendingRepair)),
                    attempt,
                    recoverySchedule.maxAttempts(),
                    backoffMs);
            Thread.sleep(backoffMs);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        // One loop iteration with nothing parked is the plain flush; the loop is what makes "a
        // completed checkpoint leaves nothing parked, on any destination" true, since the isolation
        // pass's solos can migrate mutations to the repair queue and a repair's re-submissions can
        // park for isolation — and either can concern a table the other never touched. It
        // terminates because both passes are self-bounding — the repair by its recovery schedule,
        // the pass by its park size with every entry applied, routed or migrated — so an iteration
        // ends with both queues empty unless the outside world changed between the passes (a table
        // deleted again), and each such external incident is itself bounded by a fresh recovery
        // budget.
        do {
            if (!pendingRepair.isEmpty()) {
                runRepair();
            }
            // sendOutstanding rather than a batcher's own blocking flush: waiting has to happen
            // on the mailbox, or the completion mails this writer's state is mutated by would pile
            // up behind a blocked task thread.
            sendEveryBatcher();
            drainInFlight();
            // The handler's flush comes last, and the steps before it are what it must not run
            // ahead of: the drain is what discovers this checkpoint's failures, and the pass is
            // what turns the batched ones among them into dead letters. Flushing earlier would
            // checkpoint past a dead letter one of them was about to produce.
            if (!pendingIsolation.isEmpty()) {
                runIsolationPass();
            }
        } while (!pendingRepair.isEmpty() || !pendingIsolation.isEmpty());
        failedMutationHandler.flush();
        if (!endOfInput) {
            evictIdleDestinations();
        }
    }

    /**
     * Sends what every live batcher has accumulated.
     *
     * <p>Every one of them, on every call: a drain waits for the writer to be empty, and an entry
     * still sitting in some other table's accumulator is counted in flight with no request carrying
     * it. The cost is one call per live batcher, and gax's own call returns immediately on an empty
     * accumulator.
     */
    private void sendEveryBatcher() {
        for (DestinationState state : states.values()) {
            state.batcher.sendOutstanding();
        }
    }

    /**
     * Closes and drops the state of destinations idle beyond the configured timeout — memory
     * hygiene for long-lived jobs with per-record destinations (for example date-suffixed tables),
     * whose {@link #states} map otherwise grows without bound. Runs at the end of a successful
     * flush, where every batcher has been drained and both parks are empty, so the batcher closed
     * here is empty: its close sends nothing and waits for nothing, which is what makes an
     * unbounded close acceptable on the task thread at all. Correctness is unaffected — an evicted
     * table that receives a mutation again rebuilds its batcher transparently.
     *
     * <p>The shared client is <em>not</em> closed with it: it belongs to the factory and to the
     * other tables of the same instance.
     */
    private void evictIdleDestinations() {
        long now = nanoClock.getAsLong();
        Iterator<Map.Entry<TableDestination, DestinationState>> iterator =
                states.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableDestination, DestinationState> entry = iterator.next();
            DestinationState state = entry.getValue();
            if (now - state.lastAccessNanos <= destinationIdleTimeoutNanos) {
                continue;
            }
            iterator.remove();
            try {
                state.batcher.close();
            } catch (Exception e) {
                // Hygiene must never fail a checkpoint; the batcher is abandoned either way.
                LOG.warn(
                        "Failed to close the batcher of idle Bigtable table {}", entry.getKey(), e);
            }
            LOG.info(
                    "Evicted Bigtable table {} after {} without mutations",
                    entry.getKey(),
                    Duration.ofNanos(now - state.lastAccessNanos));
        }
    }

    @Override
    public void close() throws Exception {
        // No explicit flush here: on success Flink calls flush(true) before close. On the failure
        // path the writer applies no further records itself; note a batcher's own shutdown still
        // sends what is buffered inside it, which at-least-once tolerates as duplicates after the
        // restart.
        // Both counters back gauges a reporter may still sample between this call and the metric
        // group's own close, and nothing decrements them afterwards: the completions that would do
        // so run as mailbox mails, which no longer run once the task is torn down. So a writer
        // closed mid-flight would keep reporting mutations it will never wait for again. Zeroed
        // *before* closeAll rather than after it, because the closes below can still throw — a
        // client's own shutdown, an InterruptedException from a batcher's wait, the handler's
        // close — and a mid-flight teardown is precisely when both this clear and those failures
        // happen, so a clear placed after the call would be skipped exactly when it is needed.
        // Same reason PubSubWriter.close() zeroes its parked count and the BigQuery writers clear
        // their in-flight maps. The parks are cleared for the gauge they back and for the heap
        // they hold; the mutations in them are neither applied nor routed, which at-least-once
        // covers — no checkpoint completed with them parked, so the restart replays those records.
        inFlightMutations = 0;
        inFlightBytes = 0;
        pendingIsolation.clear();
        pendingRepair.clear();
        tablesMissing.clear();
        repairCause = null;
        // One list rather than a loop and then a call (#297): closeAll runs every entry before
        // reporting anything, so one batcher failing to close cannot strand the rest.
        //
        // Every shutdown before any close, the shape PubSubWriter.close() uses. A batcher's close
        // is an unbounded wait by design, and a writer with per-record destinations holds one per
        // table: closed one after another they cost the sum of their waits, and a teardown that
        // overruns Flink's task.cancellation.timeout turns a cancelling task into a fatal
        // TaskManager error. Starting every shutdown first makes those waits overlap.
        //
        // The factory comes after every batcher, and it is what releases the clients they were
        // built over — a client closed while a sibling batcher still had one to send would abandon
        // that send against a dead channel.
        List<AutoCloseable> closeables = new ArrayList<>(states.size() * 2 + 3);
        for (DestinationState state : states.values()) {
            closeables.add(state.batcher::shutdown);
        }
        for (DestinationState state : states.values()) {
            closeables.add(state.batcher);
        }
        closeables.add(batcherFactory);
        closeables.add(tableAdmin);
        closeables.add(failedMutationHandler::close);
        try {
            Closers.closeAll(closeables);
        } finally {
            states.clear();
        }
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
     * Runs mailbox mails until <b>no</b> mutation is in flight on any destination, surfacing any
     * captured failure — including one processed by the final mail — before the caller proceeds.
     *
     * <p>This is a correctness primitive, not backpressure: it must stay independent of the
     * in-flight caps, since a completed checkpoint claims every record up to the barrier is
     * applied. It is also why the caps stay writer-global: a per-destination split would leave this
     * with no single number to wait on.
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
            DestinationState state,
            RowMutationEntry entry,
            int serializedSize,
            boolean soloVerdict,
            Throwable throwable) {
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
            pendingIsolation.add(new ParkedMutation(state.destination, entry, serializedSize));
            return;
        }
        // Every failure with a confirmed identity is counted, fatal ones included and fatal ones
        // after the first: the client has already spent its own retries, so each is a distinct
        // give-up rather than an attempt. Which means the sum over the transient codes is not this
        // connector's retry volume, unlike the Cloud Tasks sink's — the retries it would measure
        // are inside the SDK and never surface here. A NOT_FOUND parked for repair below is
        // counted too, unlike a parked row-level report: the table's absence fails every entry
        // alike, so there is no identity left to confirm, and each re-application that fails again
        // is a fresh give-up. The Pub/Sub writer counts its parked NOT_FOUNDs the same way.
        metrics.applyFailure(BigtableErrorClassifier.statusCode(throwable));
        if (kind == BigtableErrorClassifier.Kind.TABLE_NOT_FOUND && repairsTables()) {
            pendingRepair.add(new ParkedMutation(state.destination, entry, serializedSize));
            // The only thing that makes the repair call the admin for this table. Re-applying the
            // parked batch is needed whatever parked it, and a repair that has not seen a NOT_FOUND
            // for a table must not issue a creation for it.
            tablesMissing.add(state.destination);
            if (repairCause == null) {
                repairCause = throwable;
            }
            return;
        }
        if (kind == BigtableErrorClassifier.Kind.ROW_LEVEL) {
            routeFailedMutation(state, entry, throwable);
        } else if (asyncError == null) {
            asyncError = wrapMutationFailure(state.destination, kind, throwable);
        }
    }

    /** Whether the sink may repair a missing table by creating it. */
    private boolean repairsTables() {
        return config.getCreateDisposition() == CreateDisposition.CREATE_IF_NEEDED;
    }

    /**
     * Wraps a fatal mutation failure. A {@code NOT_FOUND} reaching this under {@code CREATE_NEVER}
     * names the disposition, so the reader meeting the failure learns the knob that changes it
     * rather than only the missing table.
     */
    private IOException wrapMutationFailure(
            TableDestination destination, BigtableErrorClassifier.Kind kind, Throwable throwable) {
        String reason =
                kind == BigtableErrorClassifier.Kind.TABLE_NOT_FOUND
                        ? " because the table or one of its column families does not exist and"
                                + " createDisposition is CREATE_NEVER"
                        : "";
        return new IOException(
                "A mutation of Bigtable table " + destination + " failed" + reason + ".",
                throwable);
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
    private void routeFailedMutation(
            DestinationState state, RowMutationEntry entry, Throwable throwable) {
        metrics.mutationFailed(state.metrics);
        consecutiveRejections++;
        try {
            failedMutationHandler.handle(
                    FailedMutation.of(
                            state.destination,
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
                                                + state.destination
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
            // The run is the stream's, not one table's: it accumulates across destinations and any
            // applied mutation anywhere resets it, so the message names the table of the last
            // rejection rather than claiming the run belongs to it.
            String run =
                    consecutiveRejections == 1
                            ? "refused a mutation of table "
                                    + state.destination
                                    + " (status "
                                    + BigtableErrorClassifier.statusCode(throwable)
                                    + ")"
                            : "refused "
                                    + consecutiveRejections
                                    + " mutations in a row (the last of table "
                                    + state.destination
                                    + ", with status "
                                    + BigtableErrorClassifier.statusCode(throwable)
                                    + ") with none applied between them";
            asyncError =
                    new IOException(
                            "Bigtable "
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

    /** The distinct destinations of a park, in the order they were parked. */
    private static Set<TableDestination> destinationsOf(Deque<ParkedMutation> park) {
        return park.stream()
                .map(p -> p.destination)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Names one table or several, so a message reads the same however many are involved. */
    private static String describe(Set<TableDestination> destinations) {
        if (destinations.size() == 1) {
            return "table " + destinations.iterator().next();
        }
        return "tables "
                + destinations.stream().map(Object::toString).collect(Collectors.joining(", "));
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
        // Both queues: the gauge means "held by the writer, counted by neither the in-flight
        // counters nor the handler", which is as true of a mutation awaiting a repair as of one
        // awaiting its solo verdict. Two int reads and never a walk over the destination map: the
        // reporter thread calls this, and the task thread mutates that map.
        return pendingIsolation.size() + pendingRepair.size();
    }

    /** A mutation held for the isolation pass or the repair, with the table it belongs to. */
    private static final class ParkedMutation {

        private final TableDestination destination;
        private final RowMutationEntry entry;
        private final int serializedSize;

        private ParkedMutation(
                TableDestination destination, RowMutationEntry entry, int serializedSize) {
            this.destination = destination;
            this.entry = entry;
            this.serializedSize = serializedSize;
        }
    }

    /** One destination's batcher and the bookkeeping that belongs to it alone. */
    private final class DestinationState {

        private final TableDestination destination;
        private final MutationBatcher batcher;
        private final DestinationMetrics.Counters metrics;
        private final String completionDescription;
        private final String failureDescription;

        /**
         * When this table last received a mutation ({@code nanoClock} time), for idle eviction.
         * Initialized to creation time so a state rebuilt outside {@code write()} — by a repair or
         * an isolation pass — is not instantly idle.
         */
        private long lastAccessNanos;

        private DestinationState(
                TableDestination destination, MutationBatcher batcher, long createdNanos) {
            this.destination = destination;
            this.batcher = batcher;
            // Resolved once per destination, not per record: the handle is stable, and composing
            // the table's name per record is what DestinationMetrics exists to avoid.
            this.metrics = BigtableWriter.this.metrics.forTable(destination);
            this.completionDescription = "Complete a Bigtable mutation of " + destination;
            this.failureDescription = "Fail a Bigtable mutation of " + destination;
            this.lastAccessNanos = createdNanos;
        }
    }

    /**
     * Re-dispatches mutation completions onto the mailbox so state stays task-thread-only.
     *
     * <p>One instance per submission: the callback carries the destination's state so a failure can
     * be attributed to its table, the entry so a row-level failure can be handed to the handler or
     * parked, its serialized size so both in-flight counters can be released, and whether the
     * submission was solo so a row-level status can be told from a request-level one fanned out
     * over a batch. It is also its own success mail, so the success path allocates nothing beyond
     * this object.
     */
    private final class MutationCallback
            implements ApiFutureCallback<Void>, ThrowingRunnable<Exception> {

        private final DestinationState state;
        private final RowMutationEntry entry;
        private final int serializedSize;
        private final boolean soloVerdict;

        private MutationCallback(
                DestinationState state,
                RowMutationEntry entry,
                int serializedSize,
                boolean soloVerdict) {
            this.state = state;
            this.entry = entry;
            this.serializedSize = serializedSize;
            this.soloVerdict = soloVerdict;
        }

        /** The success mail: runs on the task thread. */
        @Override
        public void run() {
            releaseInFlight(serializedSize);
            // An applied mutation is evidence the stream is not wholly broken, whichever request
            // shape and whichever table carried it — a solo re-submission included — so the
            // consecutive-rejection bound resets on every success.
            consecutiveRejections = 0;
        }

        @Override
        public void onSuccess(Void result) {
            mailboxExecutor.execute(this, state.completionDescription);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> onMutationFailed(state, entry, serializedSize, soloVerdict, throwable),
                    state.failureDescription);
        }
    }
}
