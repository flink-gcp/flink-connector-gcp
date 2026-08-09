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

package io.github.flink.gcp.connector.spanner.sink;

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
 * Tuning options for the sink's writer: how large a batch write request grows, how Spanner should
 * schedule it, and the retry budget the writer spends on transient failures.
 *
 * <p>Set via {@link SpannerSinkBuilder#writerOptions(SpannerWriterOptions)}; optional — every knob
 * is defaulted, so {@link #defaults()} is equivalent to not setting options at all.
 *
 * <h2>Why the batch limits are correctness, not tidiness</h2>
 *
 * <p>Spanner applies its commit limits to a batch write <em>request</em> as a whole, not to each
 * mutation group in it: "the maximum size for a batch write request is the same as the limit for a
 * commit request", which is 80,000 mutations — index entries included — and 100 MiB. A request over
 * either is rejected outright, taking every mutation in it with it. The three limits below are what
 * keeps the writer under those, and they carry Apache Beam's long-proven values.
 *
 * <p>{@link Builder#maxBatchCells(int)} is counted the way Spanner counts: a written column costs
 * one cell for the table plus one for every secondary index containing it, read from the database's
 * {@code INFORMATION_SCHEMA} when the writer opens. Raising it toward 80,000 removes the headroom
 * that makes the default safe.
 *
 * <h2>Why there are retry knobs at all</h2>
 *
 * <p>Unlike every other Google client this project builds on, the Spanner client does <em>not</em>
 * retry the RPC this sink writes with: {@code SpannerStubSettings} configures {@code batchWrite}
 * with an empty retryable-code set (checked against google-cloud-spanner 6.119.0), and the only
 * retry around it re-creates a lost session. So the sink owns the whole retry loop — the Cloud
 * Tasks shape rather than the Bigtable one — and these knobs budget it.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class SpannerWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The default {@link Builder#maxBatchCells(int)}: Beam's value, and 16 times under Spanner's
     * 80,000-per-request limit, so the headroom absorbs a schema whose indexes the writer could not
     * read.
     */
    public static final int DEFAULT_MAX_BATCH_CELLS = 5_000;

    /** The default {@link Builder#maxBatchMutations(int)}: Beam's value. */
    public static final int DEFAULT_MAX_BATCH_MUTATIONS = 500;

    /** The default {@link Builder#maxBatchBytes(long)}: Beam's value, 1 MiB. */
    public static final long DEFAULT_MAX_BATCH_BYTES = 1024L * 1024;

    /**
     * The largest {@link Builder#maxCommitDelay(Duration)} Spanner accepts. Rejected here rather
     * than by the service, which would fail the write on a task manager rather than the job on
     * submission.
     */
    public static final Duration MAX_COMMIT_DELAY_LIMIT = Duration.ofMillis(500);

    private static final SpannerWriterOptions DEFAULTS = builder().build();

    private final int maxBatchCells;
    private final int maxBatchMutations;
    private final long maxBatchBytes;
    @Nullable private final Duration maxCommitDelay;
    @Nullable private final SpannerRpcPriority rpcPriority;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final int retryMaxAttempts;

    private SpannerWriterOptions(Builder builder) {
        this.maxBatchCells = builder.maxBatchCells;
        this.maxBatchMutations = builder.maxBatchMutations;
        this.maxBatchBytes = builder.maxBatchBytes;
        this.maxCommitDelay = builder.maxCommitDelay;
        this.rpcPriority = builder.rpcPriority;
        this.retryInitialBackoff = builder.retryInitialBackoff;
        this.retryMaxBackoff = builder.retryMaxBackoff;
        this.retryMaxAttempts = builder.retryMaxAttempts;
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
     * Returns the default options: at most {@value #DEFAULT_MAX_BATCH_CELLS} mutation cells,
     * {@value #DEFAULT_MAX_BATCH_MUTATIONS} mutations and 1 MiB per batch write request, the
     * service's own commit delay and priority, and a retry budget of 500 ms doubling to 10 s over
     * at most 10 attempts.
     *
     * @return the default options
     */
    public static SpannerWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the cap on mutation cells per batch write request, index entries included. */
    public int getMaxBatchCells() {
        return maxBatchCells;
    }

    /** Returns the cap on mutations per batch write request. */
    public int getMaxBatchMutations() {
        return maxBatchMutations;
    }

    /** Returns the cap on the estimated size of a batch write request. */
    public long getMaxBatchBytes() {
        return maxBatchBytes;
    }

    /** Returns the commit delay, or {@code null} to leave the service's own handling in place. */
    @Nullable
    public Duration getMaxCommitDelay() {
        return maxCommitDelay;
    }

    /** Returns the RPC priority, or {@code null} to leave it unspecified (meaning high). */
    @Nullable
    public SpannerRpcPriority getRpcPriority() {
        return rpcPriority;
    }

    /** Returns the first backoff of the writer's retry loop. */
    public Duration getRetryInitialBackoff() {
        return retryInitialBackoff;
    }

    /** Returns the backoff cap of the writer's retry loop. */
    public Duration getRetryMaxBackoff() {
        return retryMaxBackoff;
    }

    /** Returns the maximum attempts of the writer's retry loop. */
    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    /**
     * Returns the retry schedule the {@code retry*} knobs describe. Jittered: every subtask writing
     * to a database that has just become unavailable retries on the same schedule, so unjittered
     * they would all come back at the same instant.
     */
    @Internal
    public RetrySchedule toRetrySchedule() {
        return new RetrySchedule(
                retryInitialBackoff.toMillis(),
                retryMaxBackoff.toMillis(),
                retryMaxAttempts,
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
        SpannerWriterOptions that = (SpannerWriterOptions) o;
        return maxBatchCells == that.maxBatchCells
                && maxBatchMutations == that.maxBatchMutations
                && maxBatchBytes == that.maxBatchBytes
                && retryMaxAttempts == that.retryMaxAttempts
                && Objects.equals(maxCommitDelay, that.maxCommitDelay)
                && rpcPriority == that.rpcPriority
                && retryInitialBackoff.equals(that.retryInitialBackoff)
                && retryMaxBackoff.equals(that.retryMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxBatchCells,
                maxBatchMutations,
                maxBatchBytes,
                maxCommitDelay,
                rpcPriority,
                retryInitialBackoff,
                retryMaxBackoff,
                retryMaxAttempts);
    }

    @Override
    public String toString() {
        return "SpannerWriterOptions{maxBatchCells="
                + maxBatchCells
                + ", maxBatchMutations="
                + maxBatchMutations
                + ", maxBatchBytes="
                + maxBatchBytes
                + ", maxCommitDelay="
                + maxCommitDelay
                + ", rpcPriority="
                + rpcPriority
                + ", retryInitialBackoff="
                + retryInitialBackoff
                + ", retryMaxBackoff="
                + retryMaxBackoff
                + ", retryMaxAttempts="
                + retryMaxAttempts
                + "}";
    }

    /** Builder for {@link SpannerWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        private int maxBatchCells = DEFAULT_MAX_BATCH_CELLS;
        private int maxBatchMutations = DEFAULT_MAX_BATCH_MUTATIONS;
        private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
        @Nullable private Duration maxCommitDelay;
        @Nullable private SpannerRpcPriority rpcPriority;
        private Duration retryInitialBackoff = Duration.ofMillis(500);
        private Duration retryMaxBackoff = Duration.ofSeconds(10);
        private int retryMaxAttempts = 10;

        private Builder() {}

        /**
         * Caps the mutation cells the writer puts in one batch write request. Defaults to {@value
         * #DEFAULT_MAX_BATCH_CELLS}.
         *
         * <p>Cells are counted as Spanner counts them against its 80,000-per-request limit: an
         * insert or update costs one cell per column it writes — primary-key columns always count —
         * plus one for every secondary index covering each of those columns, and a delete costs one
         * plus its index entries. The index part is read from {@code INFORMATION_SCHEMA} when the
         * writer opens; a table created after that is counted without it, which the default's
         * 16-fold headroom is there to absorb.
         *
         * @param maxBatchCells the cell cap, positive
         * @return this builder
         */
        public Builder maxBatchCells(int maxBatchCells) {
            Preconditions.checkArgument(maxBatchCells > 0, "maxBatchCells must be positive");
            this.maxBatchCells = maxBatchCells;
            return this;
        }

        /**
         * Caps the mutations the writer puts in one batch write request. Defaults to {@value
         * #DEFAULT_MAX_BATCH_MUTATIONS}.
         *
         * @param maxBatchMutations the mutation cap, positive
         * @return this builder
         */
        public Builder maxBatchMutations(int maxBatchMutations) {
            Preconditions.checkArgument(
                    maxBatchMutations > 0, "maxBatchMutations must be positive");
            this.maxBatchMutations = maxBatchMutations;
            return this;
        }

        /**
         * Caps the estimated size of one batch write request. Defaults to 1 MiB.
         *
         * <p>Estimated, not measured: the client library exposes no way to size a {@code Mutation}
         * as it goes on the wire, so the writer adds up the values it can see. Keep the ratio to
         * Spanner's 100 MiB request limit wide enough for the estimate to be wrong in.
         *
         * @param maxBatchBytes the byte cap, positive
         * @return this builder
         */
        public Builder maxBatchBytes(long maxBatchBytes) {
            Preconditions.checkArgument(maxBatchBytes > 0, "maxBatchBytes must be positive");
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }

        /**
         * Sets how long Spanner may delay a commit to group it with others, trading latency for
         * throughput. Defaults to unset, leaving the service's own handling in place.
         *
         * <p>Not rounded to milliseconds: the client forwards seconds and nanoseconds unchanged, so
         * whatever is set here is what the service sees.
         *
         * @param maxCommitDelay the commit delay, between zero and {@link #MAX_COMMIT_DELAY_LIMIT}
         * @return this builder
         */
        public Builder maxCommitDelay(Duration maxCommitDelay) {
            Preconditions.checkNotNull(maxCommitDelay, "maxCommitDelay must not be null");
            Preconditions.checkArgument(
                    !maxCommitDelay.isNegative(),
                    "maxCommitDelay must not be negative: %s",
                    maxCommitDelay);
            Preconditions.checkArgument(
                    maxCommitDelay.compareTo(MAX_COMMIT_DELAY_LIMIT) <= 0,
                    "maxCommitDelay must be at most %s, which is what Spanner accepts: %s",
                    MAX_COMMIT_DELAY_LIMIT,
                    maxCommitDelay);
            this.maxCommitDelay = maxCommitDelay;
            return this;
        }

        /**
         * Sets the priority Spanner schedules the sink's writes at. Defaults to unset, which
         * Spanner treats as {@link SpannerRpcPriority#HIGH}.
         *
         * @param rpcPriority the priority
         * @return this builder
         */
        public Builder rpcPriority(SpannerRpcPriority rpcPriority) {
            this.rpcPriority =
                    Preconditions.checkNotNull(rpcPriority, "rpcPriority must not be null");
            return this;
        }

        /**
         * Sets the first backoff of the writer's retry loop. Defaults to 500 ms.
         *
         * @param retryInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder retryInitialBackoff(Duration retryInitialBackoff) {
            this.retryInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(retryInitialBackoff, "retryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the writer's retry loop. Defaults to 10 s.
         *
         * @param retryMaxBackoff the backoff cap, at least 1 ms and at least the initial backoff
         * @return this builder
         */
        public Builder retryMaxBackoff(Duration retryMaxBackoff) {
            this.retryMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(retryMaxBackoff, "retryMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts of the writer's retry loop. Defaults to 10. Exhausting it fails the job
         * — a transient failure the service never recovers from within the budget is not something
         * a sink can drop.
         *
         * @param retryMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder retryMaxAttempts(int retryMaxAttempts) {
            Preconditions.checkArgument(retryMaxAttempts > 0, "retryMaxAttempts must be positive");
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public SpannerWriterOptions build() {
            Preconditions.checkState(
                    retryMaxBackoff.compareTo(retryInitialBackoff) >= 0,
                    "retryMaxBackoff must be at least retryInitialBackoff.");
            return new SpannerWriterOptions(this);
        }
    }
}
