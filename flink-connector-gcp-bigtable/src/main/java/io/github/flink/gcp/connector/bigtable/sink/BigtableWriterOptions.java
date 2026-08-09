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

package io.github.flink.gcp.connector.bigtable.sink;

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
 * Tuning options for the sink's writer: the batch thresholds handed to the client, and the writer's
 * own bounds on unacknowledged mutations.
 *
 * <p>Set via {@link BigtableSinkBuilder#writerOptions(BigtableWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all. An unset
 * batch threshold leaves the client's own default in place rather than restating it here, so a
 * client upgrade that retunes it is inherited.
 *
 * <p>There are deliberately <em>no</em> retry knobs. Unlike Cloud Tasks, the Bigtable client
 * retries {@code MutateRows} itself — per entry, for the transient codes, on a schedule of its own
 * — so the sink owns no retry loop and has nothing to expose. The {@code recovery*} knobs are not
 * an exception: they budget the sink-owned table auto-creation repair (re-applying mutations after
 * creating a missing table), not the client's mutation retries.
 *
 * <h2>Why the in-flight bounds are the writer's own</h2>
 *
 * <p>The client also has a flow controller of its own, and it is the wrong instrument here: it
 * <em>blocks</em> the calling thread when its limits are reached, and the calling thread is Flink's
 * task thread, which must stay free to run mailbox mails. So the writer keeps its own bounds and
 * yields to the mailbox instead. That only works while the writer's bounds are reached first, and
 * the client's limits — 1000 entries per channel and 100 MB, blocking — cannot be raised through
 * its public API. Raising {@link Builder#maxInFlightMutations(int)} far above the default therefore
 * moves the effective bound into the client, where it stalls the task thread rather than
 * backpressuring the stream.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class BigtableWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The default {@link Builder#maxConsecutiveRejections(int)}: enough confirmed rejections in a
     * row to say the stream's data is broken rather than anomalous, at an isolation cost of about a
     * hundred solo requests — one {@code MutateRows} round trip each — before the job fails.
     */
    public static final int DEFAULT_MAX_CONSECUTIVE_REJECTIONS = 100;

    /** {@link Builder#maxConsecutiveRejections(int)} value under which the bound never fires. */
    public static final int UNBOUNDED = -1;

    private static final BigtableWriterOptions DEFAULTS = builder().build();

    @Nullable private final Long batchElementCount;
    @Nullable private final Long batchByteSize;
    private final int maxInFlightMutations;
    private final long maxInFlightBytes;
    private final int maxConsecutiveRejections;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;

    private BigtableWriterOptions(Builder builder) {
        this.batchElementCount = builder.batchElementCount;
        this.batchByteSize = builder.batchByteSize;
        this.maxInFlightMutations = builder.maxInFlightMutations;
        this.maxInFlightBytes = builder.maxInFlightBytes;
        this.maxConsecutiveRejections = builder.maxConsecutiveRejections;
        this.recoveryInitialBackoff = builder.recoveryInitialBackoff;
        this.recoveryMaxBackoff = builder.recoveryMaxBackoff;
        this.recoveryMaxAttempts = builder.recoveryMaxAttempts;
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
     * Returns the default options: the client's own batch thresholds, at most 1000 unacknowledged
     * mutations, at most 64 MiB of them, a job failure after {@value
     * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS} consecutive confirmed rejections under a dropping
     * policy, and a table auto-creation recovery budget of 500 ms doubling to 10 s over at most 10
     * attempts.
     *
     * @return the default options
     */
    public static BigtableWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the batch element-count threshold, or {@code null} to use the client's default. */
    @Nullable
    public Long getBatchElementCount() {
        return batchElementCount;
    }

    /** Returns the batch byte threshold, or {@code null} to use the client's default. */
    @Nullable
    public Long getBatchByteSize() {
        return batchByteSize;
    }

    /** Returns the writer's cap on unacknowledged mutations. */
    public int getMaxInFlightMutations() {
        return maxInFlightMutations;
    }

    /** Returns the writer's cap on the serialized size of unacknowledged mutations. */
    public long getMaxInFlightBytes() {
        return maxInFlightBytes;
    }

    /**
     * Returns how many consecutive confirmed rejections fail the job, or {@link #UNBOUNDED} for
     * none.
     */
    public int getMaxConsecutiveRejections() {
        return maxConsecutiveRejections;
    }

    /** Returns the first backoff of the table auto-creation recovery. */
    public Duration getRecoveryInitialBackoff() {
        return recoveryInitialBackoff;
    }

    /** Returns the backoff cap of the table auto-creation recovery. */
    public Duration getRecoveryMaxBackoff() {
        return recoveryMaxBackoff;
    }

    /** Returns the maximum re-apply attempts of the table auto-creation recovery. */
    public int getRecoveryMaxAttempts() {
        return recoveryMaxAttempts;
    }

    /**
     * Returns the table auto-creation recovery schedule the {@code recovery*} knobs describe.
     * Jittered: every subtask that parked mutations for the same missing table resumes against the
     * same freshly created table, so unjittered they would re-apply in lockstep.
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
        BigtableWriterOptions that = (BigtableWriterOptions) o;
        return maxInFlightMutations == that.maxInFlightMutations
                && maxInFlightBytes == that.maxInFlightBytes
                && maxConsecutiveRejections == that.maxConsecutiveRejections
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && Objects.equals(batchElementCount, that.batchElementCount)
                && Objects.equals(batchByteSize, that.batchByteSize)
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchElementCount,
                batchByteSize,
                maxInFlightMutations,
                maxInFlightBytes,
                maxConsecutiveRejections,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts);
    }

    @Override
    public String toString() {
        return "BigtableWriterOptions{batchElementCount="
                + batchElementCount
                + ", batchByteSize="
                + batchByteSize
                + ", maxInFlightMutations="
                + maxInFlightMutations
                + ", maxInFlightBytes="
                + maxInFlightBytes
                + ", maxConsecutiveRejections="
                + maxConsecutiveRejections
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + "}";
    }

    /** Builder for {@link BigtableWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Long batchElementCount;
        @Nullable private Long batchByteSize;
        private int maxInFlightMutations = 1000;
        private long maxInFlightBytes = 64L * 1024 * 1024;
        private int maxConsecutiveRejections = DEFAULT_MAX_CONSECUTIVE_REJECTIONS;
        private Duration recoveryInitialBackoff = Duration.ofMillis(500);
        private Duration recoveryMaxBackoff = Duration.ofSeconds(10);
        private int recoveryMaxAttempts = 10;

        private Builder() {}

        /**
         * Sets how many mutations the client accumulates before sending a batch. Defaults to the
         * client's own threshold (100).
         *
         * @param batchElementCount the element-count threshold, positive
         * @return this builder
         */
        public Builder batchElementCount(long batchElementCount) {
            Preconditions.checkArgument(
                    batchElementCount > 0, "batchElementCount must be positive");
            this.batchElementCount = batchElementCount;
            return this;
        }

        /**
         * Sets how many bytes of mutations the client accumulates before sending a batch. Defaults
         * to the client's own threshold (20 MB). Element count alone bounds no memory: Bigtable
         * accepts up to 100 MB of mutations per request.
         *
         * @param batchByteSize the byte threshold, positive
         * @return this builder
         */
        public Builder batchByteSize(long batchByteSize) {
            Preconditions.checkArgument(batchByteSize > 0, "batchByteSize must be positive");
            this.batchByteSize = batchByteSize;
            return this;
        }

        /**
         * Caps the mutations the writer keeps unacknowledged. A write at the cap yields to the task
         * mailbox until completions bring the count down, bounding sink memory between checkpoints.
         * Defaults to 1000.
         *
         * <p>Raising this far above the default moves the effective bound into the client's own
         * flow controller, which blocks the task thread instead of yielding — see the class
         * documentation.
         *
         * @param maxInFlightMutations the in-flight cap, positive
         * @return this builder
         */
        public Builder maxInFlightMutations(int maxInFlightMutations) {
            Preconditions.checkArgument(
                    maxInFlightMutations > 0, "maxInFlightMutations must be positive");
            this.maxInFlightMutations = maxInFlightMutations;
            return this;
        }

        /**
         * Caps the serialized size of the mutations the writer keeps unacknowledged. Defaults to 64
         * MiB. This is the bound that actually bounds memory — a single row mutation may be
         * megabytes, so a count alone does not.
         *
         * @param maxInFlightBytes the in-flight byte cap, positive
         * @return this builder
         */
        public Builder maxInFlightBytes(long maxInFlightBytes) {
            Preconditions.checkArgument(maxInFlightBytes > 0, "maxInFlightBytes must be positive");
            this.maxInFlightBytes = maxInFlightBytes;
            return this;
        }

        /**
         * Sets how many <em>consecutive</em> confirmed rejections fail the job. Defaults to {@value
         * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS}; {@link #UNBOUNDED} (-1) never fails it.
         *
         * <p>This bound only matters beside a dropping {@code failedMutationHandler} — under the
         * default {@code failJob()} the first confirmed rejection fails the job anyway. A dropping
         * policy is a decision to keep running through <em>anomalous</em> records, and the sink
         * pays one solo request per rejection to isolate each from the good records batched with
         * it. When every record is being refused, that is no longer a stream with anomalies but a
         * broken pipeline degraded to unbatched writes under a green job — so once this many
         * confirmed rejections arrive in a row, with not one successfully applied mutation between
         * them, the job fails with a message naming this option. Any applied mutation resets the
         * count: an occasional bad record can never accumulate into a failure, however long the job
         * runs.
         *
         * <p>Only rejections the isolation pass has <em>confirmed</em> against a single mutation
         * count; records the serializer rejects do not, since they say nothing about the service's
         * view of the stream.
         *
         * @param maxConsecutiveRejections the bound, positive or {@link #UNBOUNDED}
         * @return this builder
         */
        public Builder maxConsecutiveRejections(int maxConsecutiveRejections) {
            Preconditions.checkArgument(
                    maxConsecutiveRejections > 0 || maxConsecutiveRejections == UNBOUNDED,
                    "maxConsecutiveRejections must be positive or -1 (unbounded)");
            this.maxConsecutiveRejections = maxConsecutiveRejections;
            return this;
        }

        /**
         * Sets the first backoff of the table auto-creation recovery (re-applying mutations after
         * creating a missing table). Defaults to 500 ms.
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
         * Caps the backoff of the table auto-creation recovery. Defaults to 10 s.
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
         * Caps the re-apply attempts of the table auto-creation recovery. Defaults to 10.
         *
         * @param recoveryMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder recoveryMaxAttempts(int recoveryMaxAttempts) {
            Preconditions.checkArgument(
                    recoveryMaxAttempts > 0, "recoveryMaxAttempts must be positive");
            this.recoveryMaxAttempts = recoveryMaxAttempts;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public BigtableWriterOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be at least recoveryInitialBackoff.");
            return new BigtableWriterOptions(this);
        }
    }
}
