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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;
import io.github.flink.gcp.connector.spanner.sink.FailedMutation;
import io.github.flink.gcp.connector.spanner.sink.OrderingMode;
import io.github.flink.gcp.connector.spanner.sink.SpannerSinkConfig;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.sink.serializer.SingleTableSerializationSchema;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The Spanner sink's writer: buffers mutations, applies them with {@code batchWriteAtLeastOnce},
 * and decides per mutation what a refusal means.
 *
 * <h2>Threading model</h2>
 *
 * <p>Every write happens on the task thread. There is no mailbox, no callback thread and no
 * in-flight bookkeeping, because {@code batchWriteAtLeastOnce} has no asynchronous or self-batching
 * form: it is one streaming call that this writer makes and consumes to completion, and the
 * per-group callback runs inside that iteration. That is a genuine difference from the Bigtable and
 * Pub/Sub sinks of this project rather than a simplification of them — those wrap SDK batchers that
 * complete futures on their own threads.
 *
 * <p>The exception is the three gauges, which Flink's metric reporter samples from a thread of its
 * own. They are deliberately unsynchronised: a torn read costs one wrong sample of a number that is
 * moving anyway, and the sibling sinks of this project report their own state the same way.
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>At-least-once, and stateless: the writer keeps nothing across checkpoints, because {@link
 * #flush(boolean)} empties the batch before the barrier passes. A completed checkpoint means every
 * record up to it was applied, skipped by the serializer, or handed to the failure handler.
 *
 * <p>Spanner's batch write has no replay protection, so a mutation can be applied more than once —
 * on job restart, and also inside one attempt, when a request whose outcome never arrived is
 * re-sent. Whether that matters is the serializer's choice of mutation operation; see {@link
 * SpannerMutationSerializationSchema}.
 *
 * <h2>Retries</h2>
 *
 * <p>The writer owns the whole retry loop, because the client library retries this RPC not at all —
 * {@code SpannerStubSettings} gives {@code batchWrite} an empty retryable-code set. A batch is
 * re-sent carrying exactly the mutations that are still undecided: those whose group came back with
 * a transient status, and those whose group the service never reported on, which a stream that
 * failed part-way through leaves behind.
 *
 * <p>A mutation the service <em>reported</em> applied is never re-sent. An unreported one may
 * already have been applied — that is exactly why it is undecided rather than known-failed — so a
 * retry can duplicate it, up to once per attempt. That is the at-least-once guarantee doing what it
 * says, and it is why the serializer's choice of mutation operation matters: an {@code insert}
 * re-sent this way answers {@code ALREADY_EXISTS} and reaches the failure handler despite having
 * been written.
 *
 * <h2>Per-mutation failures</h2>
 *
 * <p>{@code batchWriteAtLeastOnce} reports a status per mutation group, and the writer puts one
 * mutation in each, so a refusal names exactly the mutation it is about. A refusal {@link
 * SpannerErrorClassifier} calls row-level goes to the failure handler and the rest of the batch is
 * unaffected; anything else fails the job. A failure of the request as a whole is never routed —
 * see that class for why.
 *
 * <h2>Order per row</h2>
 *
 * <p>Spanner applies the groups of one request in an unspecified order, and the requests one at a
 * time: a batch's undecided mutations are retried before the next batch is sent. So mutations of
 * one row keep their order exactly when no request carries the row twice. Under {@link
 * OrderingMode#PER_KEY} the writer sends its batch before a mutation whose row the batch already
 * holds, naming rows through {@link PrimaryKeys}. A mutation it cannot name a row for is ordered
 * against its whole table. The one order this cannot keep is against an attempt that timed out on
 * the client and is applied by the service after the writer moved on.
 *
 * <p>A mutation of a table the primary keys do not list makes the writer send its batch and read
 * them again, once per such name: the table may have been created since the writer opened. A name
 * the new read does not list either, such as a synonym, stays ordered against the whole batch and
 * never triggers another read. A failed read is retried as a batch write is, and then fails the
 * job.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class SpannerWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriter.class);

    private final DatabaseDestination database;
    private final SpannerMutationSerializationSchema<? super T> serializer;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    private final SpannerDatabaseAccess access;
    private final CellWeights cellWeights;
    private final SpannerWriterMetrics metrics;
    private final RetrySchedule retrySchedule;
    private final ConstraintViolationPolicy constraintViolationPolicy;
    private final int maxBatchMutations;
    private final int maxBatchCells;
    private final long maxBatchBytes;

    /** The one table the writer reads the schema of, or {@code null} for the whole database. */
    @Nullable private final String schemaTable;

    /**
     * The primary keys rows are named by; {@code null} unless the order per row is kept. Replaced
     * when a mutation names a table they do not list. Task-thread only.
     */
    @Nullable private PrimaryKeys primaryKeys;

    /**
     * The table names the primary keys have been read again for; {@code null} unless the order per
     * row is kept. Task-thread only.
     */
    @Nullable private final Set<String> rereadTables;

    /** The batch being accumulated. Task-thread only. */
    private final List<PendingMutation> buffer = new ArrayList<>();

    /** The rows {@code buffer} addresses; {@code null} unless the order per row is kept. */
    @Nullable private final BatchedRows batchedRows;

    private int bufferedCells;
    private long bufferedBytes;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param access the database access, owned by this writer from here on
     * @param cellWeights the mutation-cell weights read from the database
     * @param primaryKeys the primary keys read from the database, required under {@link
     *     OrderingMode#PER_KEY} and {@code null} otherwise
     * @param metricGroup the writer's metric group
     */
    public SpannerWriter(
            SpannerSinkConfig<T> config,
            SpannerDatabaseAccess access,
            CellWeights cellWeights,
            @Nullable PrimaryKeys primaryKeys,
            SinkWriterMetricGroup metricGroup) {
        this.database = config.getDatabase();
        this.serializer = config.getSerializer();
        this.failedMutationHandler = config.getFailedMutationHandler();
        this.access = Preconditions.checkNotNull(access, "access must not be null");
        this.cellWeights = Preconditions.checkNotNull(cellWeights, "cellWeights must not be null");
        this.metrics = new SpannerWriterMetrics(metricGroup);
        this.constraintViolationPolicy = config.getConstraintViolationPolicy();
        this.schemaTable = SingleTableSerializationSchema.tableOf(config.getSerializer());

        SpannerWriterOptions options = config.getWriterOptions();
        // Validates the three recovery knobs on the way past, which is why they are not re-checked
        // below.
        this.retrySchedule = options.toRecoverySchedule();
        // The batch limits are re-checked here and not only in the builder: Java deserialization
        // reconstructs the options object without running it, so a hand-rolled instance reaches
        // the task manager unvalidated, and a non-positive limit would flush an empty batch
        // forever rather than fail.
        Preconditions.checkArgument(
                options.getMaxBatchMutations() > 0, "maxBatchMutations must be positive");
        Preconditions.checkArgument(
                options.getMaxBatchCells() > 0, "maxBatchCells must be positive");
        Preconditions.checkArgument(
                options.getMaxBatchBytes() > 0, "maxBatchBytes must be positive");
        this.maxBatchMutations = options.getMaxBatchMutations();
        this.maxBatchCells = options.getMaxBatchCells();
        this.maxBatchBytes = options.getMaxBatchBytes();
        if (options.getOrderingMode() == OrderingMode.PER_KEY) {
            this.primaryKeys =
                    Preconditions.checkNotNull(primaryKeys, "primaryKeys must not be null");
            this.batchedRows = new BatchedRows();
            this.rereadTables = new HashSet<>();
        } else {
            this.primaryKeys = null;
            this.batchedRows = null;
            this.rereadTables = null;
        }

        metrics.bindWriterState(buffer::size, () -> bufferedCells, () -> bufferedBytes);
        LOG.info(
                "Spanner sink writer opened for {} with mutation-cell weights for {} indexed"
                        + " table(s){}.",
                database,
                cellWeights.indexedTableCount(),
                this.primaryKeys == null
                        ? ""
                        : ", keeping the order per row by the primary keys of "
                                + this.primaryKeys.tableCount()
                                + " table(s)");
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        Mutation mutation;
        try {
            mutation = serializer.serialize(element, context);
        } catch (Exception e) {
            // The record never became a mutation, so there is nothing to send and nothing to
            // classify — it goes straight to the handler, which is the one failure path that does
            // not pass through the service.
            route(null, "Failed to serialize the record into a mutation.", e);
            return;
        }
        // Immediately after the serializer, ahead of any batch state: a skipped record must not
        // reach the buffer, the weights or the metrics that count sends.
        if (mutation == null) {
            metrics.recordSkipped();
            return;
        }
        PendingMutation pending;
        try {
            pending =
                    new PendingMutation(
                            mutation,
                            cellWeights.weigh(mutation),
                            MutationSizeEstimator.sizeOf(mutation));
        } catch (RuntimeException e) {
            // Insurance rather than a path with a known trigger: nothing the serializer can build
            // trips the weights or the estimator today, and a value carrying no type at all — the
            // one case that used to — is counted at the unknown-type fallback instead. The guard
            // is here because both read user-supplied data, and a Spanner type added later could
            // reintroduce a throw; without it, one such record would fail the job even under a
            // dropping policy, which is the opposite of what that policy is chosen for.
            route(mutation, "Failed to weigh the mutation against the batch limits.", e);
            return;
        }
        PrimaryKeys.Footprint footprint = null;
        if (primaryKeys != null) {
            footprint = footprintOf(mutation);
            if (footprint == null) {
                return;
            }
            if (footprint.isWholeDatabase() && rereadTables.add(primaryKeys.tableKeyOf(mutation))) {
                // Outside the per-record guard: a schema the writer cannot read is not this
                // record's failure, and routing it would drop a record the service never refused.
                rereadPrimaryKeys(mutation.getTable());
                footprint = footprintOf(mutation);
                if (footprint == null) {
                    return;
                }
            }
        }
        if (!buffer.isEmpty()
                && (wouldOverflow(pending)
                        || (footprint != null && batchedRows.overlaps(footprint)))) {
            flushBatch();
        }
        if (footprint != null) {
            batchedRows.add(footprint);
        }
        buffer.add(pending);
        bufferedCells += pending.cells;
        bufferedBytes += pending.bytes;
    }

    /**
     * Returns the rows the mutation addresses, or {@code null} when naming them threw and the
     * record went to the failure handler.
     */
    @Nullable
    private PrimaryKeys.Footprint footprintOf(Mutation mutation) throws IOException {
        try {
            return primaryKeys.footprintOf(mutation);
        } catch (RuntimeException e) {
            // Insurance, as above: naming the rows reads user-supplied values too.
            route(mutation, "Failed to name the rows the mutation addresses.", e);
            return null;
        }
    }

    /**
     * Reads the primary keys again for a table they do not list, which may have been created since
     * the writer opened.
     *
     * <p>The batch is sent first. The mutation would have sent it anyway, since an unlisted table
     * is ordered against the whole batch, and sending it means every row the batch held was named
     * by the keys the next rows are compared with.
     *
     * <p>A failed read is retried under the batch write's recovery budget when its status is one
     * {@link SpannerErrorClassifier} retries, and fails the job otherwise or once the budget is
     * spent. Carrying on with the old keys would keep the order, but would leave a table created
     * since the writer opened at one request per mutation behind a job that looks healthy.
     */
    private void rereadPrimaryKeys(String table) throws IOException, InterruptedException {
        if (!buffer.isEmpty()) {
            flushBatch();
        }
        for (int attempt = 1; ; attempt++) {
            try {
                primaryKeys = access.readPrimaryKeys(schemaTable);
                LOG.info(
                        "Read the primary keys of {} again for table '{}', which they did not list:"
                                + " {} table(s) now.",
                        database,
                        table,
                        primaryKeys.tableCount());
                return;
            } catch (IOException e) {
                if (SpannerErrorClassifier.classify(e) != SpannerErrorClassifier.Kind.TRANSIENT) {
                    throw e;
                }
                if (attempt >= retrySchedule.maxAttempts()) {
                    throw new IOException(
                            "Giving up on reading the primary keys of "
                                    + database
                                    + " again for table '"
                                    + table
                                    + "' after "
                                    + attempt
                                    + " attempt(s). Raise recoveryMaxAttempts if the database is"
                                    + " expected to be unavailable for longer than the current"
                                    + " budget.",
                            e);
                }
                Retries.sleep(
                        retrySchedule.backoffMs(attempt),
                        "Interrupted while backing off before reading the Spanner primary keys"
                                + " again.");
            }
        }
    }

    /** Hands one record to the failure handler, counting it on the way. */
    private void route(@Nullable Mutation mutation, String message, Throwable cause)
            throws IOException {
        metrics.mutationFailed();
        failedMutationHandler.handle(FailedMutation.of(database, mutation, message, cause));
    }

    /**
     * Returns whether adding the mutation to the current batch would take it past any of the three
     * limits. Checked before adding rather than after, so a batch only ever exceeds a limit when
     * one mutation does so on its own — which is a request Spanner will refuse, and the refusal
     * names the limit better than anything this writer could say in advance.
     */
    private boolean wouldOverflow(PendingMutation pending) {
        return buffer.size() + 1 > maxBatchMutations
                || bufferedCells + (long) pending.cells > maxBatchCells
                || bufferedBytes + pending.bytes > maxBatchBytes;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (!buffer.isEmpty()) {
            flushBatch();
        }
        // Last, and after the write path has drained: the handler's flush is what makes a
        // dead-letter destination durable for everything routed up to this barrier.
        failedMutationHandler.flush();
    }

    private void flushBatch() throws IOException, InterruptedException {
        List<PendingMutation> batch = new ArrayList<>(buffer);
        buffer.clear();
        if (batchedRows != null) {
            batchedRows.clear();
        }
        bufferedCells = 0;
        bufferedBytes = 0;
        send(batch);
    }

    /**
     * Sends the batch, retrying whatever stays undecided until it is decided or the retry budget is
     * spent.
     */
    private void send(List<PendingMutation> batch) throws IOException, InterruptedException {
        // Counted once here rather than per attempt: numRecordsSend is a record count, and a
        // record re-sent by the loop below is the same record.
        for (PendingMutation pending : batch) {
            metrics.mutationSent(pending.bytes);
        }
        List<PendingMutation> pending = batch;
        Throwable lastFailure = null;
        for (int attempt = 1; ; attempt++) {
            Attempt outcome = attempt(pending);
            // Carried across attempts: a final attempt that returns an incomplete stream without
            // throwing would otherwise leave the give-up below with no cause at all, discarding
            // every transport failure that led to it.
            if (outcome.lastFailure != null) {
                lastFailure = outcome.lastFailure;
            }
            for (FailedMutation failure : outcome.routed) {
                metrics.mutationFailed();
                failedMutationHandler.handle(failure);
            }
            if (outcome.undecided.isEmpty()) {
                return;
            }
            if (attempt >= retrySchedule.maxAttempts()) {
                throw new IOException(
                        "Giving up on "
                                + outcome.undecided.size()
                                + " Spanner mutation(s) after "
                                + attempt
                                + " attempt(s) against "
                                + database
                                + ". Raise recoveryMaxAttempts if the database is expected to be"
                                + " unavailable for longer than the current budget.",
                        lastFailure);
            }
            metrics.mutationsRetried(outcome.undecided.size());
            Retries.sleep(
                    retrySchedule.backoffMs(attempt),
                    "Interrupted while backing off before retrying a Spanner batch write.");
            pending = outcome.undecided;
        }
    }

    /** Sends one batch write request and sorts its mutations into decided, routed and undecided. */
    private Attempt attempt(List<PendingMutation> pending) throws IOException {
        List<MutationGroup> groups = new ArrayList<>(pending.size());
        for (PendingMutation mutation : pending) {
            // One mutation per group: the group is the unit the service reports a status for, so
            // this is what makes a refusal name a single record.
            groups.add(MutationGroup.of(mutation.mutation));
        }
        Status[] reported = new Status[pending.size()];
        Throwable requestFailure = null;
        metrics.batchSent();
        try {
            // The callback only records: routing calls user code, which must not run while a
            // server stream is open, and which may throw a checked exception the callback cannot.
            access.batchWrite(
                    groups,
                    (groupIndex, status) -> {
                        if (groupIndex >= 0 && groupIndex < reported.length) {
                            reported[groupIndex] = status;
                        }
                    });
        } catch (RuntimeException e) {
            // Deliberately broad. The client library throws SpannerException, but a broken channel
            // or a classloading failure arrives as something else, and the classifier's default is
            // to fail the job — so a failure this writer does not understand is never retried into
            // silence.
            requestFailure = e;
        }

        List<PendingMutation> undecided = new ArrayList<>();
        List<FailedMutation> routed = new ArrayList<>();
        for (int i = 0; i < pending.size(); i++) {
            Status status = reported[i];
            if (status == null) {
                // The service never got to this group: the stream failed before it, or ended
                // without reporting it. Undecided, so it is re-sent rather than assumed applied.
                undecided.add(pending.get(i));
                continue;
            }
            if (status.getCode() == Code.OK_VALUE) {
                continue;
            }
            // A number no code is known for maps to null, which the classifier calls fatal —
            // guessing at a status this client library does not know either is how a real
            // rejection becomes a silent retry.
            StatusCode.Code code = SpannerErrorClassifier.fromCanonicalCode(status.getCode());
            metrics.writeFailure(code);
            switch (SpannerErrorClassifier.classify(code, constraintViolationPolicy)) {
                case TRANSIENT:
                    undecided.add(pending.get(i));
                    break;
                case ROW_LEVEL:
                    routed.add(
                            FailedMutation.of(
                                    database,
                                    pending.get(i).mutation,
                                    "Spanner refused the mutation with "
                                            + code
                                            + ": "
                                            + status.getMessage(),
                                    null));
                    break;
                default:
                    IOException fatal =
                            new IOException(
                                    "Spanner refused a mutation on table "
                                            + pending.get(i).mutation.getTable()
                                            + " of "
                                            + database
                                            + " with "
                                            + describe(code)
                                            + ", which is not a failure of that one mutation: "
                                            + status.getMessage());
                    if (requestFailure != null) {
                        // The request failed as well. The group status is the more specific
                        // diagnosis, so it stays the message — but losing the transport failure
                        // entirely would leave an operator with half the story.
                        fatal.addSuppressed(requestFailure);
                    }
                    throw fatal;
            }
        }

        if (requestFailure != null) {
            StatusCode.Code code = SpannerErrorClassifier.statusCode(requestFailure);
            metrics.writeFailure(code);
            if (SpannerErrorClassifier.classify(requestFailure)
                    != SpannerErrorClassifier.Kind.TRANSIENT) {
                throw new IOException(
                        "Spanner rejected a batch write of "
                                + pending.size()
                                + " mutation(s) against "
                                + database
                                + " with "
                                + describe(code)
                                + ".",
                        requestFailure);
            }
        }
        return new Attempt(undecided, routed, requestFailure);
    }

    private static String describe(@Nullable StatusCode.Code code) {
        return code == null ? "no status code" : code.toString();
    }

    @Override
    public void close() throws Exception {
        // No flush: close runs on the failure path too, and re-sending a batch while the job is
        // already coming down would be a write nobody asked for. Whatever is buffered was never
        // acknowledged to a checkpoint, so it is replayed from the source.
        buffer.clear();
        if (batchedRows != null) {
            batchedRows.clear();
        }
        bufferedCells = 0;
        bufferedBytes = 0;
        Closers.closeAll(access, failedMutationHandler::close);
    }

    /** A mutation with the two weights the batch limits are checked against. */
    private static final class PendingMutation {

        private final Mutation mutation;
        private final int cells;
        private final long bytes;

        private PendingMutation(Mutation mutation, int cells, long bytes) {
            this.mutation = mutation;
            this.cells = cells;
            this.bytes = bytes;
        }
    }

    /** What one batch write attempt decided. */
    private static final class Attempt {

        private final List<PendingMutation> undecided;
        private final List<FailedMutation> routed;
        @Nullable private final Throwable lastFailure;

        private Attempt(
                List<PendingMutation> undecided,
                List<FailedMutation> routed,
                @Nullable Throwable lastFailure) {
            this.undecided = undecided;
            this.routed = routed;
            this.lastFailure = lastFailure;
        }
    }
}
