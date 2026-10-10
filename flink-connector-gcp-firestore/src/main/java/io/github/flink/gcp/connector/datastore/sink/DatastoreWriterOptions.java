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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the sink's writer: how large a commit grows, how long one may take, the
 * recovery budget the writer spends on transient failures, the ramp-up throttle, and the bound on
 * consecutive rejections.
 *
 * <p>Set via {@link DatastoreSinkBuilder#writerOptions(DatastoreWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all.
 *
 * <h2>Batches</h2>
 *
 * <p>The writer applies its mutations in non-transactional commits, flushing one when the next
 * mutation would take it past {@link Builder#maxBatchMutations(int)} or {@link
 * Builder#maxBatchBytes(long)}, when the next mutation's key is already in it, and at every
 * checkpoint. The defaults come from Apache Beam. Datastore documents no mutation count for a
 * non-transactional commit, so the count is this connector's choice rather than a service limit;
 * the size defends the documented 10 MiB request limit, and is counted as the request's protobuf
 * size.
 *
 * <h2>Why there are recovery knobs at all</h2>
 *
 * <p>The writer owns the whole retry loop, and the client makes one attempt per call, bounded by
 * {@link Builder#requestTimeout(Duration)}. The client library's own retries would re-send a commit
 * on its own schedule with no hook for the writer, which needs to see each failure: a commit the
 * service refused for one of its mutations is re-sent one mutation at a time to find that one.
 *
 * <h2>Throttling</h2>
 *
 * <p>By default each writer subtask paces itself with Datastore's 500/50/5 ramp-up guidance for new
 * traffic: the sink as a whole starts at 500 operations per second and grows by half every five
 * minutes, beginning five minutes after the subtask's first write. The 500 are shared among {@link
 * Builder#throttlingParallelism(int)} subtasks, the sink's parallelism unless set. A restarted job
 * ramps up again from its first write.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class DatastoreWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The default {@link Builder#maxBatchMutations(int)}: Apache Beam's ceiling on its adaptive
     * batch size, which starts at 50.
     */
    public static final int DEFAULT_MAX_BATCH_MUTATIONS = 500;

    /** The default {@link Builder#maxBatchBytes(long)}: Apache Beam's byte threshold, 9,000,000. */
    public static final long DEFAULT_MAX_BATCH_BYTES = 9_000_000L;

    /**
     * The largest {@link Builder#maxBatchBytes(long)}: 10 MiB, Datastore's documented maximum API
     * request size. Package-private: a public compile-time constant is inlined into whatever refers
     * to it, which would pin a caller to a value a later release changed.
     */
    static final long MAX_BATCH_BYTES_LIMIT = 10L * 1024 * 1024;

    /**
     * The default {@link Builder#maxConsecutiveRejections(int)}: enough confirmed rejections in a
     * row to say the stream's data is broken rather than anomalous, at an isolation cost of about a
     * hundred solo commits before the job fails.
     */
    public static final int DEFAULT_MAX_CONSECUTIVE_REJECTIONS = 100;

    /**
     * The default {@link Builder#idAllocationBatchSize(int)}: the measured point past which one
     * {@code AllocateIds} call grows with its size rather than with the round trip.
     */
    public static final int DEFAULT_ID_ALLOCATION_BATCH_SIZE = 1000;

    /** {@link Builder#maxConsecutiveRejections(int)} value under which the bound never fires. */
    public static final int UNBOUNDED = -1;

    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(60);

    private static final DatastoreWriterOptions DEFAULTS = builder().build();

    private final int maxBatchMutations;
    private final long maxBatchBytes;
    private final Duration requestTimeout;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;
    private final boolean throttlingEnabled;
    @Nullable private final Integer throttlingParallelism;
    private final int maxConsecutiveRejections;
    private final int idAllocationBatchSize;

    private DatastoreWriterOptions(Builder builder) {
        this.maxBatchMutations = builder.maxBatchMutations;
        this.maxBatchBytes = builder.maxBatchBytes;
        this.requestTimeout = builder.requestTimeout;
        this.recoveryInitialBackoff = builder.recoveryInitialBackoff;
        this.recoveryMaxBackoff = builder.recoveryMaxBackoff;
        this.recoveryMaxAttempts = builder.recoveryMaxAttempts;
        this.throttlingEnabled = builder.throttlingEnabled;
        this.throttlingParallelism = builder.throttlingParallelism;
        this.maxConsecutiveRejections = builder.maxConsecutiveRejections;
        this.idAllocationBatchSize = builder.idAllocationBatchSize;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default options: commits of at most {@value #DEFAULT_MAX_BATCH_MUTATIONS}
     * mutations and 9,000,000 bytes, a 60 s timeout for each commit, a recovery budget of 500 ms
     * doubling to 10 s over at most 10 attempts, ramp-up throttling shared across the sink's
     * parallelism, {@value #DEFAULT_MAX_CONSECUTIVE_REJECTIONS} consecutive confirmed rejections
     * under a dropping policy, and {@value #DEFAULT_ID_ALLOCATION_BATCH_SIZE} ids per allocation.
     *
     * @return the default options
     */
    public static DatastoreWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the cap on mutations per commit. */
    public int getMaxBatchMutations() {
        return maxBatchMutations;
    }

    /** Returns the cap on the protobuf size of one commit request. */
    public long getMaxBatchBytes() {
        return maxBatchBytes;
    }

    /** Returns the timeout of one call: a commit attempt, or a lookup. */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /** Returns the first backoff of the writer's retry loop. */
    public Duration getRecoveryInitialBackoff() {
        return recoveryInitialBackoff;
    }

    /** Returns the backoff cap of the writer's retry loop. */
    public Duration getRecoveryMaxBackoff() {
        return recoveryMaxBackoff;
    }

    /** Returns the maximum attempts of the writer's retry loop. */
    public int getRecoveryMaxAttempts() {
        return recoveryMaxAttempts;
    }

    /** Returns whether the writer paces itself with the ramp-up throttle. */
    public boolean isThrottlingEnabled() {
        return throttlingEnabled;
    }

    /**
     * Returns the number of subtasks the ramp-up budget is shared among, or {@code null} for the
     * sink's parallelism.
     */
    @Nullable
    public Integer getThrottlingParallelism() {
        return throttlingParallelism;
    }

    /** Returns how many consecutive confirmed rejections fail the job, or {@link #UNBOUNDED}. */
    public int getMaxConsecutiveRejections() {
        return maxConsecutiveRejections;
    }

    /** Returns how many ids one {@code AllocateIds} call allocates. */
    public int getIdAllocationBatchSize() {
        return idAllocationBatchSize;
    }

    /**
     * Returns the retry schedule the {@code recovery*} knobs describe. Jittered: every subtask
     * writing to a database that has just become unavailable retries on the same schedule, so
     * unjittered they would all come back at the same instant.
     */
    @Internal
    public RetrySchedule toRecoverySchedule() {
        return new RetrySchedule(
                recoveryInitialBackoff.toMillis(),
                recoveryMaxBackoff.toMillis(),
                recoveryMaxAttempts,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatastoreWriterOptions that = (DatastoreWriterOptions) o;
        return maxBatchMutations == that.maxBatchMutations
                && maxBatchBytes == that.maxBatchBytes
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && throttlingEnabled == that.throttlingEnabled
                && maxConsecutiveRejections == that.maxConsecutiveRejections
                && idAllocationBatchSize == that.idAllocationBatchSize
                && requestTimeout.equals(that.requestTimeout)
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff)
                && Objects.equals(throttlingParallelism, that.throttlingParallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxBatchMutations,
                maxBatchBytes,
                requestTimeout,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts,
                throttlingEnabled,
                throttlingParallelism,
                maxConsecutiveRejections,
                idAllocationBatchSize);
    }

    @Override
    public String toString() {
        return "DatastoreWriterOptions{maxBatchMutations="
                + maxBatchMutations
                + ", maxBatchBytes="
                + maxBatchBytes
                + ", requestTimeout="
                + requestTimeout
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + ", throttlingEnabled="
                + throttlingEnabled
                + ", throttlingParallelism="
                + throttlingParallelism
                + ", maxConsecutiveRejections="
                + maxConsecutiveRejections
                + ", idAllocationBatchSize="
                + idAllocationBatchSize
                + "}";
    }

    /** Builder for {@link DatastoreWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        private int maxBatchMutations = DEFAULT_MAX_BATCH_MUTATIONS;
        private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
        private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        private Duration recoveryInitialBackoff = Duration.ofMillis(500);
        private Duration recoveryMaxBackoff = Duration.ofSeconds(10);
        private int recoveryMaxAttempts = 10;
        private boolean throttlingEnabled = true;
        @Nullable private Integer throttlingParallelism;
        private int maxConsecutiveRejections = DEFAULT_MAX_CONSECUTIVE_REJECTIONS;
        private int idAllocationBatchSize = DEFAULT_ID_ALLOCATION_BATCH_SIZE;

        private Builder() {}

        /**
         * Caps the mutations the writer puts in one commit. Defaults to {@value
         * #DEFAULT_MAX_BATCH_MUTATIONS}.
         *
         * @param maxBatchMutations the mutation cap, positive
         * @return this builder
         */
        public Builder maxBatchMutations(int maxBatchMutations) {
            Preconditions.checkArgument(
                    maxBatchMutations > 0,
                    "maxBatchMutations must be positive, was %s",
                    maxBatchMutations);
            this.maxBatchMutations = maxBatchMutations;
            return this;
        }

        /**
         * Caps the protobuf size of one commit request: its mutations, and its project, database
         * and mode fields. Defaults to 9,000,000 bytes.
         *
         * <p>A single mutation larger than the cap is still sent, alone, and the service decides.
         *
         * @param maxBatchBytes the byte cap, positive and at most 10 MiB
         * @return this builder
         */
        public Builder maxBatchBytes(long maxBatchBytes) {
            Preconditions.checkArgument(
                    maxBatchBytes > 0, "maxBatchBytes must be positive, was %s", maxBatchBytes);
            Preconditions.checkArgument(
                    maxBatchBytes <= MAX_BATCH_BYTES_LIMIT,
                    "maxBatchBytes must be at most %s, Datastore's maximum request size, was %s",
                    MAX_BATCH_BYTES_LIMIT,
                    maxBatchBytes);
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }

        /**
         * Sets the timeout of one call the writer makes: a commit attempt, or the lookup that
         * checks an update's {@code NOT_FOUND}. Defaults to 60 s, the generated client's own commit
         * timeout.
         *
         * <p>The client makes one attempt per commit; the {@code recovery*} settings own every
         * retry, so a timed-out commit is retried like any other transient failure.
         *
         * @param requestTimeout the per-attempt timeout, at least 1 ms
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            // Check the tighter nanosecond bound first so an oversized value reports this knob's
            // own limit rather than the shared millisecond conversion limit (ADR-0180).
            this.requestTimeout =
                    OptionChecks.checkAtLeastOneMilli(
                            OptionChecks.checkExpressibleInNanos(requestTimeout, "requestTimeout"),
                            "requestTimeout");
            return this;
        }

        /**
         * Sets the first backoff of the writer's retry loop. Defaults to 500 ms.
         *
         * @param recoveryInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder recoveryInitialBackoff(Duration recoveryInitialBackoff) {
            this.recoveryInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            recoveryInitialBackoff, "recoveryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the writer's retry loop. Defaults to 10 s.
         *
         * @param recoveryMaxBackoff the backoff cap, at least 1 ms and at least the initial backoff
         * @return this builder
         */
        public Builder recoveryMaxBackoff(Duration recoveryMaxBackoff) {
            this.recoveryMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(recoveryMaxBackoff, "recoveryMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts of the writer's retry loop. Defaults to 10. Exhausting it fails the job
         * — a transient failure the service never recovers from within the budget is not something
         * a sink can drop.
         *
         * <p>The budget is per commit: a mutation re-sent alone to confirm a rejection gets a
         * budget of its own.
         *
         * @param recoveryMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder recoveryMaxAttempts(int recoveryMaxAttempts) {
            Preconditions.checkArgument(
                    recoveryMaxAttempts > 0,
                    "recoveryMaxAttempts must be positive, was %s",
                    recoveryMaxAttempts);
            this.recoveryMaxAttempts = recoveryMaxAttempts;
            return this;
        }

        /**
         * Sets whether the writer paces itself with Datastore's ramp-up guidance. Defaults to
         * {@code true}.
         *
         * <p>Disabling it removes the pacing; a database without the warm-up it provides may then
         * answer a burst with {@code RESOURCE_EXHAUSTED}, which the recovery budget absorbs or,
         * once spent, fails the job over.
         *
         * @param throttlingEnabled whether to throttle
         * @return this builder
         */
        public Builder throttlingEnabled(boolean throttlingEnabled) {
            this.throttlingEnabled = throttlingEnabled;
            return this;
        }

        /**
         * Sets how many subtasks share the ramp-up's starting budget of 500 operations per second.
         * Defaults to the sink's parallelism.
         *
         * <p>Set it when more than this sink writes to the database — several sinks, or several
         * jobs — so that together they start where the guidance says. Each subtask's share never
         * drops below one operation per second.
         *
         * @param throttlingParallelism the number of subtasks, positive
         * @return this builder
         */
        public Builder throttlingParallelism(int throttlingParallelism) {
            Preconditions.checkArgument(
                    throttlingParallelism > 0,
                    "throttlingParallelism must be positive, was %s",
                    throttlingParallelism);
            this.throttlingParallelism = throttlingParallelism;
            return this;
        }

        /**
         * Sets how many <em>consecutive</em> confirmed rejections fail the job. Defaults to {@value
         * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS}; {@link #UNBOUNDED} (-1) never fails it.
         *
         * <p>This bound only matters beside a {@code failedMutationHandler} that does not fail the
         * job, dropping or dead-lettering — under the default {@code failJob()} the first rejection
         * already fails the job. Beside one, it is what stops a stream whose mutations Datastore
         * refuses wholesale from being shed one record at a time behind a green job: every refused
         * mutation is still routed, and the bound fails the job once that many arrive with none
         * applied in between. A confirmed {@code INVALID_ARGUMENT} counts toward it, and so does a
         * key addressing another database than the sink's, which the service would refuse the same
         * way: {@code ALREADY_EXISTS} for an insert and {@code NOT_FOUND} for an update do not,
         * because they are what a restart's replay answers rather than evidence of broken data.
         * Records the serializer rejects do not count either.
         *
         * @param maxConsecutiveRejections the bound, positive, or {@link #UNBOUNDED}
         * @return this builder
         */
        public Builder maxConsecutiveRejections(int maxConsecutiveRejections) {
            Preconditions.checkArgument(
                    maxConsecutiveRejections > 0 || maxConsecutiveRejections == UNBOUNDED,
                    "maxConsecutiveRejections must be positive or -1 (unbounded), was %s",
                    maxConsecutiveRejections);
            this.maxConsecutiveRejections = maxConsecutiveRejections;
            return this;
        }

        /**
         * Sets how many ids one {@code AllocateIds} call allocates, for a serializer that writes
         * under ids the service allocates (the {@code datastore} table sink's tables without a
         * PRIMARY KEY). Defaults to {@value #DEFAULT_ID_ALLOCATION_BATCH_SIZE}.
         *
         * <p>Each sink subtask allocates this many ids at a time and draws them in order; ids it
         * holds when it stops are never written, which costs nothing. Datastore documents no limit
         * on the keys of one call; a call of 20,000 was measured to succeed. Past about a thousand
         * a call takes longer in proportion to its size, and a larger batch only delays the first
         * write.
         *
         * @param idAllocationBatchSize the ids per call, positive
         * @return this builder
         */
        public Builder idAllocationBatchSize(int idAllocationBatchSize) {
            Preconditions.checkArgument(
                    idAllocationBatchSize > 0,
                    "idAllocationBatchSize must be positive, was %s",
                    idAllocationBatchSize);
            this.idAllocationBatchSize = idAllocationBatchSize;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         * @throws IllegalStateException if the recovery backoff cap is below its initial value
         */
        public DatastoreWriterOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be at least recoveryInitialBackoff.");
            return new DatastoreWriterOptions(this);
        }
    }
}
