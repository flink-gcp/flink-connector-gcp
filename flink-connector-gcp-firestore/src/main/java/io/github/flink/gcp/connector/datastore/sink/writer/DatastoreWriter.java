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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSinkConfig;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.sink.FailedMutation;
import io.github.flink.gcp.connector.datastore.sink.serializer.DatastoreMutationSerializationSchema;
import io.github.flink.gcp.connector.datastore.sink.serializer.KeyAllocatingSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The Datastore sink's writer: buffers entity writes, applies them in non-transactional commits,
 * and finds out which write a refused commit was refused for.
 *
 * <h2>Threading model</h2>
 *
 * <p>Every call happens on the task thread, and the writer waits for each commit before it sends
 * the next: the client's commit is a blocking call with no batching of its own, so there is no
 * mailbox, no callback thread and no in-flight bookkeeping — the Spanner sink's shape. The two
 * gauges are read from the metric reporter's thread and are deliberately unsynchronised: a torn
 * read costs one wrong sample of a number that is moving anyway.
 *
 * <h2>Delivery guarantees, state and order</h2>
 *
 * <p>At-least-once, and stateless: the writer keeps nothing across checkpoints, because {@link
 * #flush(boolean)} commits the batch before the barrier passes. A completed checkpoint means every
 * record up to it was applied, skipped by the serializer, or handed to the failure handler.
 *
 * <p>Because commits go one at a time and a batch never holds two writes to one key — the writer
 * commits the batch before a key repeats — the writes of one subtask to one key are applied in the
 * order the serializer returned them, retries and solo confirmations included. The one exception is
 * beyond the writer's sight: an attempt that timed out on the client may still be applied by the
 * service after a later commit.
 *
 * <h2>Retries</h2>
 *
 * <p>The writer owns the whole retry loop; the client makes one attempt per call. A commit that
 * failed with a transient status is re-sent whole, within the {@code recovery*} budget. A failed
 * non-transactional commit may have applied some of its writes, so a re-send can apply a write a
 * second time; that is the at-least-once guarantee, and why the serializer's choice of operation
 * matters.
 *
 * <p>For a serializer that writes under ids the service allocates ({@link
 * KeyAllocatingSerializationSchema}), the writer also allocates ids, through the same client and
 * within the same budget; a {@link #write} can then block in an allocation's backoff. An allocation
 * the service refuses, or the budget cannot finish, fails the job like such a commit, whatever the
 * failure handler.
 *
 * <h2>Per-mutation failures</h2>
 *
 * <p>A commit carries no per-mutation status: a refusal is the request's, and does not say which
 * write earned it. So a commit refused with a status one write could have earned — {@code
 * INVALID_ARGUMENT}, {@code ALREADY_EXISTS} when the commit holds an insert, {@code NOT_FOUND} when
 * it holds an update — is re-sent one write at a time, and only a refusal that repeats alone, for
 * an operation that can earn it, reaches the failure handler (ADR-0045's shape). The rest of the
 * commit is applied by those solo commits. A {@code NOT_FOUND} is routed only once a lookup of the
 * same key has been answered, which a missing database refuses as it refused the update. See {@link
 * DatastoreErrorClassifier} for what fails the job instead.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class DatastoreWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreWriter.class);

    private final DatabaseDestination database;
    private final DatastoreMutationSerializationSchema<? super T> serializer;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    private final DatastoreWriterMetrics metrics;
    private final RetrySchedule retrySchedule;
    private final int maxBatchMutations;
    private final long maxBatchBytes;
    private final int idAllocationBatchSize;
    private final int maxConsecutiveRejections;

    /** What a commit takes besides its mutations, counted against {@code maxBatchBytes} too. */
    private final long requestHeaderBytes;

    @Nullable private final RampUpThrottle throttle;
    private final DatastoreDatabaseAccess access;

    /** The batch being accumulated. Task-thread only. */
    private final List<PendingMutation> buffer = new ArrayList<>();

    /** The keys of {@code buffer}, so a repeated key is found without a scan. */
    private final Set<Key> bufferedKeys = new HashSet<>();

    private long bufferedBytes;

    /** Confirmed rejections counting toward the bound since the last applied mutation. */
    private int consecutiveRejections;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param factory opens the database access, which this writer owns from then on
     * @param parallelism the sink's parallelism, which shares the ramp-up budget unless the options
     *     name another number
     * @param metricGroup the writer's metric group
     * @throws IOException if the database access cannot be opened
     */
    public DatastoreWriter(
            DatastoreSinkConfig<T> config,
            DatastoreDatabaseAccessFactory factory,
            int parallelism,
            SinkWriterMetricGroup metricGroup)
            throws IOException {
        this(config, factory, throttle(config.getWriterOptions(), parallelism), metricGroup);
    }

    @VisibleForTesting
    DatastoreWriter(
            DatastoreSinkConfig<T> config,
            DatastoreDatabaseAccessFactory factory,
            @Nullable RampUpThrottle throttle,
            SinkWriterMetricGroup metricGroup)
            throws IOException {
        this.database = config.getDatabase();
        this.serializer = config.getSerializer();
        this.failedMutationHandler = config.getFailedMutationHandler();
        this.metrics = new DatastoreWriterMetrics(metricGroup);
        this.throttle = throttle;

        DatastoreWriterOptions options = config.getWriterOptions();
        // Re-checked here and not only in the builder, through the builder's own setters: Java
        // deserialization reconstructs the options object without running it, so a hand-rolled
        // instance reaches the task manager unvalidated. A non-positive limit would commit an
        // empty batch forever rather than fail, and an unbounded timeout would reach the client
        // as a call with no deadline.
        DatastoreWriterOptions.Builder recheck =
                DatastoreWriterOptions.builder()
                        .maxBatchMutations(options.getMaxBatchMutations())
                        .maxBatchBytes(options.getMaxBatchBytes())
                        .requestTimeout(options.getRequestTimeout())
                        .recoveryInitialBackoff(options.getRecoveryInitialBackoff())
                        .recoveryMaxBackoff(options.getRecoveryMaxBackoff())
                        .recoveryMaxAttempts(options.getRecoveryMaxAttempts())
                        .maxConsecutiveRejections(options.getMaxConsecutiveRejections())
                        .idAllocationBatchSize(options.getIdAllocationBatchSize());
        if (options.getThrottlingParallelism() != null) {
            recheck.throttlingParallelism(options.getThrottlingParallelism());
        }
        recheck.build();
        this.retrySchedule = options.toRecoverySchedule();
        this.maxBatchMutations = options.getMaxBatchMutations();
        this.maxBatchBytes = options.getMaxBatchBytes();
        this.maxConsecutiveRejections = options.getMaxConsecutiveRejections();
        this.idAllocationBatchSize = options.getIdAllocationBatchSize();
        this.requestHeaderBytes = MutationSizeEstimator.requestHeaderSize(database);

        metrics.bindWriterState(buffer::size, () -> bufferedBytes);
        // Last, so that a failure above leaves nothing open.
        this.access = factory.create();
        if (serializer instanceof KeyAllocatingSerializationSchema) {
            ((KeyAllocatingSerializationSchema<?>) serializer).setKeyAllocator(this::allocate);
        }
        LOG.info("Datastore sink writer opened for {}.", database);
    }

    @VisibleForTesting
    @Nullable
    static RampUpThrottle throttle(DatastoreWriterOptions options, int parallelism) {
        if (!options.isThrottlingEnabled()) {
            return null;
        }
        Integer shared = options.getThrottlingParallelism();
        return new RampUpThrottle(
                shared != null ? shared : Math.max(1, parallelism),
                // Monotonic, so a wall-clock step neither jumps the ramp ahead nor sets it back.
                () -> System.nanoTime() / 1_000_000,
                Thread::sleep);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        DatastoreMutation write;
        try {
            write = serializer.serialize(element, context);
        } catch (Exception e) {
            AllocationFailure allocation = allocationFailureIn(e);
            if (allocation != null) {
                // The database refused or could not answer an id allocation: that is the
                // request's failure, as a commit's would be, not this record's.
                throw allocation;
            }
            // The record never became a write, so there is nothing to send and nothing to
            // classify — it goes straight to the handler.
            route(null, "Failed to serialize the record into a Datastore write.", e);
            return;
        }
        // Immediately after the serializer, ahead of any batch state: a skipped record must not
        // reach the buffer, the throttle or the metrics that count sends.
        if (write == null) {
            metrics.recordSkipped();
            return;
        }
        String mismatch = addressMismatch(write.getKey());
        if (mismatch != null) {
            // Counted toward the bound like a confirmed INVALID_ARGUMENT, which is what the service
            // would answer: a sink whose keys all name another database is a configuration error,
            // and without the count a dropping handler would shed the whole stream behind a green
            // job. The buffer is committed first, because its writes came before this one and an
            // applied write resets the run; counting past them would fail a stream that mixes
            // well-addressed writes with a few strays.
            if (!buffer.isEmpty()) {
                flushBatch();
            }
            consecutiveRejections++;
            route(write, mismatch, null);
            enforceRejectionBound(write.getKey(), "a key addressing another database", null);
            return;
        }
        long size;
        try {
            size = MutationSizeEstimator.sizeOf(write);
        } catch (RuntimeException e) {
            // Insurance rather than a path with a known trigger: the size is the client library's
            // own encoding of a value it accepted, but it reads user-supplied data, and without
            // this one such record would fail the job even under a dropping policy.
            route(write, "Failed to size the Datastore write against the batch limits.", e);
            return;
        }
        if (throttle != null) {
            metrics.throttled(throttle.acquire());
        }
        if (!buffer.isEmpty() && wouldOverflowOrRepeat(write.getKey(), size)) {
            flushBatch();
        }
        buffer.add(new PendingMutation(write, size));
        bufferedKeys.add(write.getKey());
        bufferedBytes += size;
    }

    /**
     * Returns why the key cannot be written by this sink, or {@code null}. A commit names one
     * project and database, and the service refuses a key of another; refusing it here names the
     * cause, and keeps one such record from failing the commit of everything batched with it.
     */
    @Nullable
    private String addressMismatch(Key key) {
        // Objects.equals: the client library accepts a null database id on a key without
        // checking it, and one such record must be routed rather than fail the job.
        if (Objects.equals(key.getProjectId(), database.getProject())
                && Objects.equals(key.getDatabaseId(), database.getDatabaseId())) {
            return null;
        }
        String hint =
                DatabaseDestination.DEFAULT_DATABASE_NAME.equals(key.getDatabaseId())
                        ? " The Datastore API spells the default database as an empty id, never"
                                + " '(default)': build the key with Key.newBuilder(project, kind,"
                                + " name) instead."
                        : " Build keys with the sink's project and database id:"
                                + " Key.newBuilder(project, kind, name) for the default database,"
                                + " Key.newBuilder(project, kind, name, databaseId) for a named"
                                + " one.";
        return "The write's key "
                + key
                + " addresses project '"
                + key.getProjectId()
                + "' and database id '"
                + key.getDatabaseId()
                + "', but the sink writes to "
                + database
                + "."
                + hint;
    }

    /**
     * Returns whether adding the write to the current batch would take it past a limit, or put a
     * second write to one key in it. Checked before adding, so a batch only exceeds the byte limit
     * when one write does so on its own — a request the service will refuse, which names the limit
     * better than anything this writer could say in advance.
     */
    private boolean wouldOverflowOrRepeat(Key key, long size) {
        return bufferedKeys.contains(key)
                || buffer.size() + 1 > maxBatchMutations
                || requestHeaderBytes + bufferedBytes + size > maxBatchBytes;
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

    private void flushBatch() throws IOException {
        List<PendingMutation> batch = new ArrayList<>(buffer);
        buffer.clear();
        bufferedKeys.clear();
        bufferedBytes = 0;
        send(batch);
    }

    /** Commits the batch, confirming each write alone if the commit is refused for one of them. */
    private void send(List<PendingMutation> batch) throws IOException {
        // Counted once here rather than per attempt: numRecordsSend is a record count, and a
        // record re-sent by the loop or alone is the same record.
        for (PendingMutation pending : batch) {
            metrics.mutationSent(pending.size);
        }
        RuntimeException failure = commitWithRetries(batch);
        if (failure == null) {
            consecutiveRejections = 0;
            return;
        }
        if (batch.size() > 1 && isCandidateFor(failure, batch)) {
            for (PendingMutation pending : batch) {
                // A blocking call ignores an interrupt, so a cancelled task would otherwise run the
                // whole pass first, one bounded commit after another.
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedIOException(
                            "Interrupted while confirming a refused Datastore commit one mutation"
                                    + " at a time.");
                }
                metrics.mutationConfirmedAlone();
                RuntimeException alone = commitWithRetries(List.of(pending));
                if (alone == null) {
                    consecutiveRejections = 0;
                } else {
                    decide(pending, alone);
                }
            }
            return;
        }
        if (batch.size() == 1) {
            // A commit of one write is already its own confirmation.
            decide(batch.get(0), failure);
            return;
        }
        throw fatal(failure, batch.size());
    }

    /**
     * Returns whether a refused commit's status is one a write of it could have earned: {@code
     * INVALID_ARGUMENT} for any write, {@code ALREADY_EXISTS} only beside an insert, and {@code
     * NOT_FOUND} only beside an update. Any other refusal is the commit's, and fails the job.
     */
    private static boolean isCandidateFor(RuntimeException failure, List<PendingMutation> batch) {
        if (DatastoreErrorClassifier.classify(failure) != DatastoreErrorClassifier.Kind.CANDIDATE) {
            return false;
        }
        StatusCode.Code code = DatastoreErrorClassifier.statusCode(failure);
        if (code == StatusCode.Code.INVALID_ARGUMENT) {
            return true;
        }
        DatastoreMutation.Operation earner = earnerOf(code);
        for (PendingMutation pending : batch) {
            if (pending.write.getOperation() == earner) {
                return true;
            }
        }
        return false;
    }

    /** The only operation that can earn the status alone, or {@code null} for any operation. */
    @Nullable
    private static DatastoreMutation.Operation earnerOf(@Nullable StatusCode.Code code) {
        if (code == StatusCode.Code.ALREADY_EXISTS) {
            return DatastoreMutation.Operation.INSERT;
        }
        if (code == StatusCode.Code.NOT_FOUND) {
            return DatastoreMutation.Operation.UPDATE;
        }
        return null;
    }

    /** Decides what a refusal of a commit holding only this write means. */
    private void decide(PendingMutation pending, RuntimeException failure) throws IOException {
        StatusCode.Code code = DatastoreErrorClassifier.statusCode(failure);
        boolean candidate =
                DatastoreErrorClassifier.classify(failure)
                        == DatastoreErrorClassifier.Kind.CANDIDATE;
        DatastoreMutation.Operation operation = pending.write.getOperation();
        if (candidate && code == StatusCode.Code.INVALID_ARGUMENT) {
            reject(pending, code, failure, true);
            return;
        }
        if (!candidate) {
            // Not a status any one mutation earns: the commit's, whatever its size.
            throw fatal(failure, 1);
        }
        if (operation == earnerOf(code)) {
            if (code == StatusCode.Code.NOT_FOUND) {
                confirmDatabaseAnswers(pending, failure);
            }
            // Neither counts toward the bound: both are what a restart's replay answers — a
            // replayed insert, and a replayed update of an entity the stream deleted afterwards.
            reject(pending, code, failure, false);
            return;
        }
        metrics.writeFailure(code);
        throw new IOException(
                "Datastore refused the "
                        + operation
                        + " of "
                        + pending.write.getKey()
                        + " in "
                        + database
                        + " with "
                        + describe(code)
                        + ", which this sink does not route for that operation."
                        + (code == StatusCode.Code.NOT_FOUND
                                ? " A NOT_FOUND for anything but an update means the database"
                                        + " is missing or unreachable."
                                : ""),
                failure);
    }

    /**
     * Fails the job unless a lookup of the update's key is answered: a missing database refuses it
     * with the {@code NOT_FOUND} the update got, while a missing entity is an ordinary answer.
     */
    private void confirmDatabaseAnswers(PendingMutation pending, RuntimeException updateFailure)
            throws IOException {
        Key key = pending.write.getKey();
        for (int attempt = 1; ; attempt++) {
            try {
                access.lookup(key);
                return;
            } catch (RuntimeException e) {
                StatusCode.Code code = DatastoreErrorClassifier.statusCode(e);
                metrics.writeFailure(code);
                if (DatastoreErrorClassifier.classify(e) != DatastoreErrorClassifier.Kind.TRANSIENT
                        || attempt >= retrySchedule.maxAttempts()) {
                    IOException fatal =
                            new IOException(
                                    "Datastore refused an update of "
                                            + key
                                            + " in "
                                            + database
                                            + " with NOT_FOUND, and a lookup of the same key"
                                            + " failed with "
                                            + describe(code)
                                            + ", so the sink cannot tell a missing entity from a"
                                            + " missing or unreachable database and fails rather"
                                            + " than route the update.",
                                    e);
                    fatal.addSuppressed(updateFailure);
                    throw fatal;
                }
                Retries.sleep(
                        retrySchedule.backoffMs(attempt),
                        "Interrupted while backing off before retrying a Datastore lookup.");
            }
        }
    }

    /**
     * Allocates keys for a serializer that writes under service-allocated ids, retrying a transient
     * failure within the recovery budget, as a commit is retried.
     *
     * @throws AllocationFailure if the service refuses the allocation, the budget is spent, the
     *     answer is short, or the backoff is interrupted; {@link #write} fails the job on it
     */
    private List<Key> allocate(IncompleteKey key) throws AllocationFailure {
        int count = idAllocationBatchSize;
        List<IncompleteKey> keys = Collections.nCopies(count, key);
        for (int attempt = 1; ; attempt++) {
            try {
                List<Key> allocated = access.allocateIds(keys);
                if (allocated.size() != count) {
                    throw new AllocationFailure(
                            "Datastore allocated "
                                    + allocated.size()
                                    + " ids for "
                                    + count
                                    + " keys of kind "
                                    + key.getKind()
                                    + " in "
                                    + database
                                    + ".",
                            null);
                }
                return allocated;
            } catch (RuntimeException e) {
                StatusCode.Code code = DatastoreErrorClassifier.statusCode(e);
                metrics.writeFailure(code);
                if (DatastoreErrorClassifier.classify(e) != DatastoreErrorClassifier.Kind.TRANSIENT
                        || attempt >= retrySchedule.maxAttempts()) {
                    throw new AllocationFailure(
                            "Datastore failed to allocate ids for kind "
                                    + key.getKind()
                                    + " in "
                                    + database
                                    + " after "
                                    + attempt
                                    + " attempt(s), with "
                                    + describe(code)
                                    + ".",
                            e);
                }
                try {
                    Retries.sleep(
                            retrySchedule.backoffMs(attempt),
                            "Interrupted while backing off before retrying a Datastore id"
                                    + " allocation.");
                } catch (IOException interrupted) {
                    throw new AllocationFailure(interrupted.getMessage(), interrupted);
                }
            }
        }
    }

    /** Returns the allocation failure in a serializer's failure's cause chain, if there is one. */
    @Nullable
    private static AllocationFailure allocationFailureIn(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof AllocationFailure) {
                return (AllocationFailure) t;
            }
        }
        return null;
    }

    /**
     * An id allocation the writer could not complete. It fails the job whatever the failure handler
     * is, as a commit the recovery budget cannot finish does: it says nothing about the record.
     */
    static final class AllocationFailure extends IOException {
        private static final long serialVersionUID = 1L;

        AllocationFailure(String message, @Nullable Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Commits the writes, retrying a transient failure within the recovery budget.
     *
     * @return {@code null} once the commit succeeded, or the first failure that is not transient
     * @throws IOException if the budget is spent, or the backoff is interrupted
     */
    @Nullable
    private RuntimeException commitWithRetries(List<PendingMutation> mutations) throws IOException {
        List<DatastoreMutation> writes = new ArrayList<>(mutations.size());
        for (PendingMutation pending : mutations) {
            writes.add(pending.write);
        }
        for (int attempt = 1; ; attempt++) {
            metrics.batchSent();
            try {
                access.commit(writes);
                return null;
            } catch (RuntimeException e) {
                // Deliberately broad. The client library throws DatastoreException, but a broken
                // channel or a classloading failure arrives as something else, and the classifier
                // calls a failure it cannot read fatal — so nothing this writer does not
                // understand is retried into silence.
                if (DatastoreErrorClassifier.classify(e)
                        != DatastoreErrorClassifier.Kind.TRANSIENT) {
                    return e;
                }
                metrics.writeFailure(DatastoreErrorClassifier.statusCode(e));
                if (attempt >= retrySchedule.maxAttempts()) {
                    throw new IOException(
                            "Giving up on "
                                    + writes.size()
                                    + " Datastore mutation(s) after "
                                    + attempt
                                    + " attempt(s) against "
                                    + database
                                    + ". Raise recoveryMaxAttempts if the database is expected to"
                                    + " be unavailable for longer than the current budget.",
                            e);
                }
                metrics.mutationsRetried(writes.size());
                Retries.sleep(
                        retrySchedule.backoffMs(attempt),
                        "Interrupted while backing off before retrying a Datastore commit.");
            }
        }
    }

    /** Routes a write the service refused alone, then enforces the rejection bound. */
    private void reject(
            PendingMutation pending,
            @Nullable StatusCode.Code code,
            RuntimeException failure,
            boolean countsTowardBound)
            throws IOException {
        metrics.writeFailure(code);
        if (countsTowardBound) {
            consecutiveRejections++;
        }
        route(
                pending.write,
                "Datastore refused the "
                        + pending.write.getOperation()
                        + " with "
                        + describe(code)
                        + ": "
                        + failure.getMessage(),
                failure);
        if (countsTowardBound) {
            enforceRejectionBound(pending.write.getKey(), "status " + describe(code), failure);
        }
    }

    /** Fails the job once the run of counted rejections reaches the bound. */
    private void enforceRejectionBound(Key lastKey, String lastCause, @Nullable Throwable failure)
            throws IOException {
        if (maxConsecutiveRejections != DatastoreWriterOptions.UNBOUNDED
                && consecutiveRejections >= maxConsecutiveRejections) {
            throw new IOException(
                    consecutiveRejections
                            + " mutation(s) in a row were refused (the last, "
                            + lastKey
                            + " in "
                            + database
                            + ", with "
                            + lastCause
                            + ") with none applied between them, reaching"
                            + " maxConsecutiveRejections("
                            + maxConsecutiveRejections
                            + "): the stream's data looks broken rather than anomalous, so the job"
                            + " fails instead of routing it record by record. Every refused"
                            + " mutation, this one included, was routed to the configured handler"
                            + " first; DatastoreWriterOptions.builder().maxConsecutiveRejections(-1)"
                            + " removes this bound.",
                    failure);
        }
    }

    /** Hands one record to the failure handler, counting it on the way. */
    private void route(@Nullable DatastoreMutation write, String message, @Nullable Throwable cause)
            throws IOException {
        metrics.mutationFailed();
        failedMutationHandler.handle(FailedMutation.of(database, write, message, cause));
    }

    private IOException fatal(RuntimeException failure, int mutations) {
        StatusCode.Code code = DatastoreErrorClassifier.statusCode(failure);
        metrics.writeFailure(code);
        return new IOException(
                "Datastore refused a commit of "
                        + mutations
                        + " mutation(s) against "
                        + database
                        + " with "
                        + describe(code)
                        + ", which is not a failure of one mutation."
                        + hint(code),
                failure);
    }

    /** What a request-level status most often means for this sink, or nothing. */
    private static String hint(@Nullable StatusCode.Code code) {
        if (code == StatusCode.Code.NOT_FOUND) {
            return " The database is missing, or its id is mistyped.";
        }
        if (code == StatusCode.Code.PERMISSION_DENIED) {
            return " The job's identity needs datastore.entities.* permissions on the database,"
                    + " which roles/datastore.user carries.";
        }
        if (code == StatusCode.Code.FAILED_PRECONDITION) {
            return " A database in Firestore Native mode refuses the Datastore API; write it with"
                    + " FirestoreSink instead.";
        }
        return "";
    }

    private static String describe(@Nullable StatusCode.Code code) {
        return code == null ? "no status code" : code.toString();
    }

    @Override
    public void close() throws Exception {
        // No flush: close runs on the failure path too, and committing a batch while the job is
        // already coming down would be a write nobody asked for. Whatever is buffered was never
        // acknowledged to a checkpoint, so it is replayed from the source.
        buffer.clear();
        bufferedKeys.clear();
        bufferedBytes = 0;
        Closers.closeAll(access, failedMutationHandler::close);
    }

    /** A write with its size on the wire. */
    private static final class PendingMutation {

        private final DatastoreMutation write;
        private final long size;

        private PendingMutation(DatastoreMutation write, long size) {
            this.write = write;
            this.size = size;
        }
    }
}
