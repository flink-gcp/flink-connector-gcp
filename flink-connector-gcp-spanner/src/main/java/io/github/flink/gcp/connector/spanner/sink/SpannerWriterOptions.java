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
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <h2>What the batch limits defend</h2>
 *
 * <p>A sink that accumulates mutations has to bound the request it builds, since a request Spanner
 * refuses is refused as a whole, taking every mutation in it with it. That much is correctness
 * rather than tidiness. <em>Which</em> limit each knob defends is narrower than three of them make
 * it look: Spanner documents <b>no per-request mutation count for batch write at all</b>, so {@link
 * Builder#maxBatchBytes(long)} is the one defending a documented request-level limit, and the other
 * two bound the request as a proxy for its size. How large that size limit is can be read two ways,
 * and the ceiling below takes the looser one. {@code docs/adr/0077} carries the documentation rows
 * this rests on.
 *
 * <p>{@link Builder#maxBatchCells(int)} is counted the way Spanner counts a mutation: a written
 * column costs one cell for the table plus one for every secondary index containing it, read from
 * the database's {@code INFORMATION_SCHEMA} when the writer opens. On a wide row that is a better
 * proxy for the request's size than a count of mutations would be.
 *
 * <p>The defaults are Apache Beam's, and Beam batches for {@code Commit} rather than for batch
 * write — which is where the commit-shaped figures entered this connector. They sit far under every
 * reading of every limit all the same.
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

    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterOptions.class);

    private static final long serialVersionUID = 1L;

    /**
     * The default {@link Builder#maxBatchCells(int)}: Beam's value, and 16 times under the 80,000
     * ceiling, so the headroom absorbs a schema whose indexes the writer could not read — and,
     * should Spanner enforce a per-request mutation count it does not document, keeps an
     * undercounted batch under that too.
     */
    public static final int DEFAULT_MAX_BATCH_CELLS = 5_000;

    /** The default {@link Builder#maxBatchMutations(int)}: Beam's value. */
    public static final int DEFAULT_MAX_BATCH_MUTATIONS = 500;

    /** The default {@link Builder#maxBatchBytes(long)}: Beam's value, 1 MiB. */
    public static final long DEFAULT_MAX_BATCH_BYTES = 1024L * 1024;

    /**
     * The largest {@link Builder#maxBatchCells(int)} this connector accepts: 80,000, the only
     * mutation number Spanner documents for a batch write request — and it bounds one <em>mutation
     * group</em> rather than the request. Taken as the request-level ceiling because no
     * request-level count is documented at all, so a batch under it is under every row that could
     * apply. Precautionary rather than a refusal anyone has seen: whether the service refuses a
     * request over 80,000 mutations is undocumented either way, and this holds a batch to the one
     * figure it does publish.
     *
     * <p>Package-private, and the setter's {@code @param} names the number rather than this symbol:
     * nothing outside this package has asked for it, and a public compile-time constant is inlined
     * into whatever refers to it, which would leave a caller pinned to a value a later release
     * changed. Widen it when something asks, not before.
     */
    static final int MAX_BATCH_CELLS_LIMIT = 80_000;

    /**
     * The largest {@link Builder#maxBatchMutations(int)} this connector accepts — <b>derived from
     * {@link #MAX_BATCH_CELLS_LIMIT}, not a second figure</b>. Every mutation costs at least one
     * cell, so a batch never holds more mutations than cells; a value above the cell ceiling
     * therefore names a batch that cannot exist. Deriving it rather than repeating 80,000 is what
     * keeps the two moving together: a cell ceiling raised later would otherwise leave this one
     * cutting below what a batch may legally hold.
     *
     * <p>A value that is legal here but still above the configured {@link
     * Builder#maxBatchCells(int)} cannot take effect either. That one is warned about rather than
     * rejected — it describes a working configuration, just not the one the user meant.
     */
    static final int MAX_BATCH_MUTATIONS_LIMIT = MAX_BATCH_CELLS_LIMIT;

    /**
     * The largest {@link Builder#maxBatchBytes(long)} this connector accepts: 100 MiB, from "the
     * maximum size for a batch write request is the same as the limit for a commit request" and
     * Spanner's 100 MiB commit size. Package-private for the reason {@link #MAX_BATCH_CELLS_LIMIT}
     * gives, which binds hardest here — #441 may lower this very number.
     *
     * <p><b>The looser of two readings</b>, deliberately. The quotas page also carries a "request
     * size other than for commits" of 10 MiB, which a batch write can be read as falling under, and
     * it has no batch-write size row to break the tie. Bounding at the looser reading rejects only
     * what is illegal under both; measuring which one holds, and tightening this to 10 MiB if that
     * is the answer, is #441.
     */
    static final long MAX_BATCH_BYTES_LIMIT = 100L * 1024 * 1024;

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
         * <p>Cells are counted as Spanner counts a mutation: an insert or update costs one cell per
         * column it writes — primary-key columns always count — plus one for every secondary index
         * covering each of those columns, and a delete costs one plus its index entries. The index
         * part is read from {@code INFORMATION_SCHEMA} when the writer opens; a table created after
         * that is counted without it, which the default's 16-fold headroom is there to absorb.
         *
         * @param maxBatchCells the cell cap, positive and at most 80,000
         * @return this builder
         */
        public Builder maxBatchCells(int maxBatchCells) {
            Preconditions.checkArgument(maxBatchCells > 0, "maxBatchCells must be positive");
            Preconditions.checkArgument(
                    maxBatchCells <= MAX_BATCH_CELLS_LIMIT,
                    "maxBatchCells must be at most %s, the only mutation count Spanner documents for"
                            + " a batch write request: %s",
                    MAX_BATCH_CELLS_LIMIT,
                    maxBatchCells);
            this.maxBatchCells = maxBatchCells;
            return this;
        }

        /**
         * Caps the mutations the writer puts in one batch write request. Defaults to {@value
         * #DEFAULT_MAX_BATCH_MUTATIONS}.
         *
         * <p>Every mutation costs at least one cell, so a batch never holds more mutations than
         * cells. A value above 80,000 therefore names a batch that cannot exist and is rejected
         * here; a value that is merely above the configured {@link #maxBatchCells(int)} cannot take
         * effect either, and {@link #build()} warns about that rather than refusing it.
         *
         * @param maxBatchMutations the mutation cap, positive and at most 80,000
         * @return this builder
         */
        public Builder maxBatchMutations(int maxBatchMutations) {
            Preconditions.checkArgument(
                    maxBatchMutations > 0, "maxBatchMutations must be positive");
            Preconditions.checkArgument(
                    maxBatchMutations <= MAX_BATCH_MUTATIONS_LIMIT,
                    "maxBatchMutations must be at most %s, which is maxBatchCells' own ceiling: %s",
                    MAX_BATCH_MUTATIONS_LIMIT,
                    maxBatchMutations);
            this.maxBatchMutations = maxBatchMutations;
            return this;
        }

        /**
         * Caps the estimated size of one batch write request. Defaults to 1 MiB.
         *
         * <p>Estimated, not measured: the client library exposes no way to size a {@code Mutation}
         * as it goes on the wire, so the writer adds up the values it can see, and it reads low.
         * Keep the ratio to the real request limit wide enough for the estimate to be wrong in. The
         * ceiling is a guard against a misconfiguration, not a recommendation — set there, the
         * estimate's undercount alone puts the request over — and it is the looser of two readings
         * of how large a batch write request may be.
         *
         * @param maxBatchBytes the byte cap, positive and at most 100 MiB
         * @return this builder
         */
        public Builder maxBatchBytes(long maxBatchBytes) {
            Preconditions.checkArgument(maxBatchBytes > 0, "maxBatchBytes must be positive");
            Preconditions.checkArgument(
                    maxBatchBytes <= MAX_BATCH_BYTES_LIMIT,
                    "maxBatchBytes must be at most %s, the largest a batch write request is"
                            + " documented to allow under the looser of two readings: %s",
                    MAX_BATCH_BYTES_LIMIT,
                    maxBatchBytes);
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
         * <p>Warns, rather than fails, when {@code maxBatchMutations} cannot take effect: a value
         * above {@code maxBatchCells} describes a batch that cannot exist, since every mutation
         * costs at least one cell. The configuration works — the cell cap simply decides every
         * flush — so refusing it would reject something harmless, and saying nothing would leave a
         * user believing they had capped a batch by count.
         *
         * @return the options
         */
        public SpannerWriterOptions build() {
            Preconditions.checkState(
                    retryMaxBackoff.compareTo(retryInitialBackoff) >= 0,
                    "retryMaxBackoff must be at least retryInitialBackoff.");
            if (maxBatchMutations > maxBatchCells) {
                // Logged where the options are built rather than in the writer, which would repeat
                // it once per subtask. That is normally the job's main method — but not only:
                // initializing this class runs DEFAULTS = builder().build(), and a task manager
                // holding a deserialized instance has initialized it. What keeps this line off a
                // task manager is that the defaults do not satisfy the condition, which
                // SpannerWriterOptionsTest pins rather than leaving to chance.
                //
                // No remedy is suggested. "Lower it below maxBatchCells" would be false — whether
                // the count cap binds depends on what each mutation costs in cells, not on the two
                // knobs' order — and "raise maxBatchCells" trades away the headroom that absorbs an
                // unreadable schema.
                LOG.warn(
                        "maxBatchMutations is {} but maxBatchCells is {}, so the mutation cap can"
                                + " never take effect: every mutation costs at least one cell, so a"
                                + " batch reaches the cell cap first. Whether a lower"
                                + " maxBatchMutations binds instead depends on what each mutation"
                                + " costs in cells.",
                        maxBatchMutations,
                        maxBatchCells);
            }
            return new SpannerWriterOptions(this);
        }
    }
}
