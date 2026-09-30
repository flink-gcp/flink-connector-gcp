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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.BulkWriter;
import io.github.flink.gcp.connector.base.options.OptionChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the sink's writer: the client library's throttling, its two layers of retries,
 * and the writer's own bounds on unacknowledged writes.
 *
 * <p>Set via {@link FirestoreSinkBuilder#writerOptions(FirestoreWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all. An unset
 * rate leaves the client library's own default in place rather than restating it here, so a client
 * upgrade that retunes it is inherited.
 *
 * <h2>Throttling</h2>
 *
 * <p>The writer sends through the client library's {@code BulkWriter}, which throttles itself: it
 * starts at {@link Builder#initialOpsPerSecond(int)} operations per second and raises the rate by
 * half every five minutes up to {@link Builder#maxOpsPerSecond(int)}. That is Firestore's own
 * 500/50/5 ramp-up guidance for new traffic, and each writer subtask ramps separately, so a sink
 * with parallelism {@code p} starts at {@code p} times the initial rate. Disabling throttling
 * removes the ramp; a database without the warm-up it paces may then answer a burst with {@code
 * RESOURCE_EXHAUSTED}, which the retries below absorb.
 *
 * <h2>Retries</h2>
 *
 * <p>Retrying is the client library's, in two layers. The {@code BulkWriter} re-sends a write
 * Firestore refused with {@code RESOURCE_EXHAUSTED}, {@code UNAVAILABLE} or {@code ABORTED} with
 * its own jittered exponential backoff, one second growing by half up to a minute — except after
 * {@code RESOURCE_EXHAUSTED}, which waits the full minute from the first retry. {@link
 * Builder#writeMaxAttempts(int)} is the one part of that loop the library lets a caller decide, and
 * it bounds how long a throttled write can take: at the default, about ten minutes of backoff. That
 * backoff has no knobs, because the library exposes none.
 *
 * <p>Underneath, the transport retries each {@code BatchWrite} call. The {@code retry*} knobs set
 * that layer's settings, as the Pub/Sub sink's knobs of the same names set its publish RPC's, and
 * an unset knob keeps the library's value for {@code BatchWrite}: a 60-second total timeout, 100 ms
 * growing by 1.3 times up to a minute between attempts, and five attempts. The client library
 * applies these settings to every call a client makes, and the sink's client makes no call but
 * {@code BatchWrite}.
 *
 * <h2>In-flight bounds</h2>
 *
 * <p>The writer bounds the writes it has handed to the client and not yet seen answered, by count
 * and by estimated size, and yields to the Flink mailbox at either bound. The client offers no
 * backpressure of its own: past 500 pending writes it queues further ones without limit.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class FirestoreWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The default {@link Builder#writeMaxAttempts(int)}: the first attempt and the client library's
     * own ten retries ({@code BulkWriter.MAX_RETRY_ATTEMPTS}).
     */
    public static final int DEFAULT_WRITE_MAX_ATTEMPTS = BulkWriter.MAX_RETRY_ATTEMPTS + 1;

    /**
     * The default {@link Builder#maxInFlightWrites(int)}: half the client library's pending
     * ceiling. A failed write holds one of those slots until the writer replaces its {@code
     * BulkWriter}, which restarts the throttle's ramp-up, so the headroom is what lets a burst of
     * refusals — every create a restart replays, say — pass without a replacement per refusal.
     */
    public static final int DEFAULT_MAX_IN_FLIGHT_WRITES = 250;

    /**
     * The largest {@link Builder#maxInFlightWrites(int)}: the client library's pending-operation
     * ceiling ({@code BulkWriter}, google-cloud-firestore 3.46.0), past which it queues further
     * writes without bound instead of sending them.
     */
    public static final int MAX_IN_FLIGHT_WRITES_LIMIT = 500;

    /** The default {@link Builder#maxInFlightBytes(long)}: 64 MiB. */
    public static final long DEFAULT_MAX_IN_FLIGHT_BYTES = 64L * 1024 * 1024;

    /**
     * The default {@link Builder#maxConsecutiveRejections(int)}: enough confirmed rejections in a
     * row to say the stream's data is broken rather than anomalous, at an isolation cost of about a
     * hundred solo requests before the job fails.
     */
    public static final int DEFAULT_MAX_CONSECUTIVE_REJECTIONS = 100;

    /** {@link Builder#maxConsecutiveRejections(int)} value under which the bound never fires. */
    public static final int UNBOUNDED = -1;

    /**
     * The lowest rate the throttle accepts: the client library's batch size ({@code
     * BulkWriter.MAX_BATCH_SIZE}). The library opens its first batch at that size before it lowers
     * the batch size to a slower rate, and a full batch cannot pass a throttle slower than itself:
     * the library retries its send with no delay, spinning its thread, until the ramp-up lifts the
     * rate to the batch size, or forever when the ceiling is below it (google-cloud-firestore
     * 3.46.0).
     */
    static final int MIN_OPS_PER_SECOND = BulkWriter.MAX_BATCH_SIZE;

    private static final FirestoreWriterOptions DEFAULTS = builder().build();

    private final boolean throttlingEnabled;
    @Nullable private final Integer initialOpsPerSecond;
    @Nullable private final Integer maxOpsPerSecond;
    private final int writeMaxAttempts;
    @Nullable private final Duration retryTotalTimeout;
    @Nullable private final Duration retryInitialDelay;
    @Nullable private final Double retryDelayMultiplier;
    @Nullable private final Duration retryMaxDelay;
    @Nullable private final Duration retryInitialRpcTimeout;
    @Nullable private final Double retryRpcTimeoutMultiplier;
    @Nullable private final Duration retryMaxRpcTimeout;
    @Nullable private final Integer retryMaxAttempts;
    private final int maxInFlightWrites;
    private final long maxInFlightBytes;
    private final int maxConsecutiveRejections;

    private FirestoreWriterOptions(Builder builder) {
        this.throttlingEnabled = builder.throttlingEnabled;
        this.initialOpsPerSecond = builder.initialOpsPerSecond;
        this.maxOpsPerSecond = builder.maxOpsPerSecond;
        this.writeMaxAttempts = builder.writeMaxAttempts;
        this.retryTotalTimeout = builder.retryTotalTimeout;
        this.retryInitialDelay = builder.retryInitialDelay;
        this.retryDelayMultiplier = builder.retryDelayMultiplier;
        this.retryMaxDelay = builder.retryMaxDelay;
        this.retryInitialRpcTimeout = builder.retryInitialRpcTimeout;
        this.retryRpcTimeoutMultiplier = builder.retryRpcTimeoutMultiplier;
        this.retryMaxRpcTimeout = builder.retryMaxRpcTimeout;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.maxInFlightWrites = builder.maxInFlightWrites;
        this.maxInFlightBytes = builder.maxInFlightBytes;
        this.maxConsecutiveRejections = builder.maxConsecutiveRejections;
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
     * Returns the default options: the client library's throttling at its own rates, its own retry
     * budgets and transport settings, at most {@value #DEFAULT_MAX_IN_FLIGHT_WRITES} unacknowledged
     * writes and 64 MiB of them, and a job failure after {@value
     * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS} consecutive confirmed rejections under a dropping
     * policy.
     *
     * @return the default options
     */
    public static FirestoreWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns whether the client library throttles the writes it sends. */
    public boolean isThrottlingEnabled() {
        return throttlingEnabled;
    }

    /** Returns the initial throttling rate, or {@code null} to use the client library's default. */
    @Nullable
    public Integer getInitialOpsPerSecond() {
        return initialOpsPerSecond;
    }

    /** Returns the throttling rate ceiling, or {@code null} for the client library's default. */
    @Nullable
    public Integer getMaxOpsPerSecond() {
        return maxOpsPerSecond;
    }

    /** Returns how many attempts the {@code BulkWriter} gives a write, the first one included. */
    public int getWriteMaxAttempts() {
        return writeMaxAttempts;
    }

    /**
     * Returns the total timeout of a {@code BatchWrite} call, or {@code null} for the library's.
     */
    @Nullable
    public Duration getRetryTotalTimeout() {
        return retryTotalTimeout;
    }

    /** Returns the delay before the first call retry, or {@code null} for the library's. */
    @Nullable
    public Duration getRetryInitialDelay() {
        return retryInitialDelay;
    }

    /** Returns the call-retry delay multiplier, or {@code null} for the library's. */
    @Nullable
    public Double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    /** Returns the cap on the delay between call retries, or {@code null} for the library's. */
    @Nullable
    public Duration getRetryMaxDelay() {
        return retryMaxDelay;
    }

    /** Returns the timeout of the first call attempt, or {@code null} for the library's. */
    @Nullable
    public Duration getRetryInitialRpcTimeout() {
        return retryInitialRpcTimeout;
    }

    /** Returns the per-attempt timeout multiplier, or {@code null} for the library's. */
    @Nullable
    public Double getRetryRpcTimeoutMultiplier() {
        return retryRpcTimeoutMultiplier;
    }

    /** Returns the cap on a call attempt's timeout, or {@code null} for the library's. */
    @Nullable
    public Duration getRetryMaxRpcTimeout() {
        return retryMaxRpcTimeout;
    }

    /** Returns the cap on a call's attempts, or {@code null} for the library's. */
    @Nullable
    public Integer getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    /** Returns the writer's cap on unacknowledged writes. */
    public int getMaxInFlightWrites() {
        return maxInFlightWrites;
    }

    /** Returns the writer's cap on the estimated size of unacknowledged writes. */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FirestoreWriterOptions that = (FirestoreWriterOptions) o;
        return throttlingEnabled == that.throttlingEnabled
                && writeMaxAttempts == that.writeMaxAttempts
                && maxInFlightWrites == that.maxInFlightWrites
                && maxInFlightBytes == that.maxInFlightBytes
                && maxConsecutiveRejections == that.maxConsecutiveRejections
                && Objects.equals(initialOpsPerSecond, that.initialOpsPerSecond)
                && Objects.equals(maxOpsPerSecond, that.maxOpsPerSecond)
                && Objects.equals(retryTotalTimeout, that.retryTotalTimeout)
                && Objects.equals(retryInitialDelay, that.retryInitialDelay)
                && Objects.equals(retryDelayMultiplier, that.retryDelayMultiplier)
                && Objects.equals(retryMaxDelay, that.retryMaxDelay)
                && Objects.equals(retryInitialRpcTimeout, that.retryInitialRpcTimeout)
                && Objects.equals(retryRpcTimeoutMultiplier, that.retryRpcTimeoutMultiplier)
                && Objects.equals(retryMaxRpcTimeout, that.retryMaxRpcTimeout)
                && Objects.equals(retryMaxAttempts, that.retryMaxAttempts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                throttlingEnabled,
                initialOpsPerSecond,
                maxOpsPerSecond,
                writeMaxAttempts,
                retryTotalTimeout,
                retryInitialDelay,
                retryDelayMultiplier,
                retryMaxDelay,
                retryInitialRpcTimeout,
                retryRpcTimeoutMultiplier,
                retryMaxRpcTimeout,
                retryMaxAttempts,
                maxInFlightWrites,
                maxInFlightBytes,
                maxConsecutiveRejections);
    }

    @Override
    public String toString() {
        return "FirestoreWriterOptions{throttlingEnabled="
                + throttlingEnabled
                + ", initialOpsPerSecond="
                + initialOpsPerSecond
                + ", maxOpsPerSecond="
                + maxOpsPerSecond
                + ", writeMaxAttempts="
                + writeMaxAttempts
                + ", retryTotalTimeout="
                + retryTotalTimeout
                + ", retryInitialDelay="
                + retryInitialDelay
                + ", retryDelayMultiplier="
                + retryDelayMultiplier
                + ", retryMaxDelay="
                + retryMaxDelay
                + ", retryInitialRpcTimeout="
                + retryInitialRpcTimeout
                + ", retryRpcTimeoutMultiplier="
                + retryRpcTimeoutMultiplier
                + ", retryMaxRpcTimeout="
                + retryMaxRpcTimeout
                + ", retryMaxAttempts="
                + retryMaxAttempts
                + ", maxInFlightWrites="
                + maxInFlightWrites
                + ", maxInFlightBytes="
                + maxInFlightBytes
                + ", maxConsecutiveRejections="
                + maxConsecutiveRejections
                + "}";
    }

    /** Builder for {@link FirestoreWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        private boolean throttlingEnabled = true;
        @Nullable private Integer initialOpsPerSecond;
        @Nullable private Integer maxOpsPerSecond;
        private int writeMaxAttempts = DEFAULT_WRITE_MAX_ATTEMPTS;
        @Nullable private Duration retryTotalTimeout;
        @Nullable private Duration retryInitialDelay;
        @Nullable private Double retryDelayMultiplier;
        @Nullable private Duration retryMaxDelay;
        @Nullable private Duration retryInitialRpcTimeout;
        @Nullable private Double retryRpcTimeoutMultiplier;
        @Nullable private Duration retryMaxRpcTimeout;
        @Nullable private Integer retryMaxAttempts;
        private int maxInFlightWrites = DEFAULT_MAX_IN_FLIGHT_WRITES;
        private long maxInFlightBytes = DEFAULT_MAX_IN_FLIGHT_BYTES;
        private int maxConsecutiveRejections = DEFAULT_MAX_CONSECUTIVE_REJECTIONS;

        private Builder() {}

        /**
         * Sets whether the client library throttles the writes it sends. Defaults to {@code true}.
         * The two rates below can only be set while throttling is on.
         *
         * @param throttlingEnabled whether to throttle
         * @return this builder
         */
        public Builder throttlingEnabled(boolean throttlingEnabled) {
            this.throttlingEnabled = throttlingEnabled;
            return this;
        }

        /**
         * Sets the rate, in operations per second per writer subtask, the throttle starts at.
         * Defaults to the client library's own starting rate (500, Firestore's ramp-up guidance).
         *
         * @param initialOpsPerSecond the starting rate, at least 20 — the client library's batch
         *     size, below which its first full batch cannot pass the throttle
         * @return this builder
         */
        public Builder initialOpsPerSecond(int initialOpsPerSecond) {
            checkRate(initialOpsPerSecond, "initialOpsPerSecond");
            this.initialOpsPerSecond = initialOpsPerSecond;
            return this;
        }

        /**
         * Sets the rate, in operations per second per writer subtask, the throttle ramps up to.
         * Defaults to no ceiling: the rate keeps rising by half every five minutes.
         *
         * @param maxOpsPerSecond the rate ceiling, at least 20 and at least the initial rate
         * @return this builder
         */
        public Builder maxOpsPerSecond(int maxOpsPerSecond) {
            checkRate(maxOpsPerSecond, "maxOpsPerSecond");
            this.maxOpsPerSecond = maxOpsPerSecond;
            return this;
        }

        /**
         * Sets how many attempts the {@code BulkWriter} gives a write, the first one included, when
         * Firestore refuses it with a status the client library retries. Defaults to {@value
         * #DEFAULT_WRITE_MAX_ATTEMPTS}; {@code 1} disables these retries. Each attempt is one
         * {@code BatchWrite} call, which the transport retries on its own within the {@code retry*}
         * settings below.
         *
         * @param writeMaxAttempts the attempt budget, positive
         * @return this builder
         */
        public Builder writeMaxAttempts(int writeMaxAttempts) {
            Preconditions.checkArgument(
                    writeMaxAttempts > 0,
                    "writeMaxAttempts must be positive, was %s",
                    writeMaxAttempts);
            this.writeMaxAttempts = writeMaxAttempts;
            return this;
        }

        /**
         * Sets the total time budget of a {@code BatchWrite} call including the transport's
         * retries. Optional; defaults to the library's 60 seconds.
         *
         * <p><b>{@code Duration.ZERO} is settable</b> and means what gax means by it: retries are
         * bounded by the attempt count instead of by time. A setting this connector forwards to the
         * library stays settable as the library defines it; a positive sub-millisecond value is
         * refused instead, because gax reads this with {@code toMillis()} and it would silently
         * become that zero (ADR-0068).
         *
         * @param retryTotalTimeout the total timeout, at least 1 ms or {@code Duration.ZERO}
         * @return this builder
         */
        public Builder retryTotalTimeout(Duration retryTotalTimeout) {
            this.retryTotalTimeout =
                    OptionChecks.checkAtLeastOneMilliOrZero(retryTotalTimeout, "retryTotalTimeout");
            return this;
        }

        /**
         * Sets the delay before the transport's first retry of a {@code BatchWrite} call. Optional;
         * defaults to the library's 100 ms.
         *
         * <p><b>{@code Duration.ZERO} is settable</b> and means what gax means by it: no delay
         * before the first retry. A positive sub-millisecond value is refused, because gax reads
         * this with {@code toMillis()} and it would silently become that zero (ADR-0068).
         *
         * <p>It must not exceed {@link #retryMaxDelay(Duration)}, set or the library's 60 seconds;
         * the sink's builder refuses the pair otherwise.
         *
         * @param retryInitialDelay the initial retry delay, at least 1 ms or {@code Duration.ZERO}
         * @return this builder
         */
        public Builder retryInitialDelay(Duration retryInitialDelay) {
            this.retryInitialDelay =
                    OptionChecks.checkAtLeastOneMilliOrZero(retryInitialDelay, "retryInitialDelay");
            return this;
        }

        /**
         * Sets the factor the transport's retry delay grows by per attempt. Optional; defaults to
         * the library's 1.3.
         *
         * @param retryDelayMultiplier the delay multiplier, at least 1.0
         * @return this builder
         */
        public Builder retryDelayMultiplier(double retryDelayMultiplier) {
            Preconditions.checkArgument(
                    retryDelayMultiplier >= 1.0,
                    "retryDelayMultiplier must be at least 1.0, was %s",
                    retryDelayMultiplier);
            this.retryDelayMultiplier = retryDelayMultiplier;
            return this;
        }

        /**
         * Caps the delay between the transport's retries of a {@code BatchWrite} call. Optional;
         * defaults to the library's 60 seconds.
         *
         * <p><b>{@code Duration.ZERO} is settable</b> and means what gax means by it: a cap of
         * zero, so that no retry waits. A positive sub-millisecond value is refused, because gax
         * reads this with {@code toMillis()} and it would silently become that zero (ADR-0068).
         *
         * <p>It must not be shorter than {@link #retryInitialDelay(Duration)}, set or the library's
         * 100 ms, so a cap of zero needs {@code retryInitialDelay(Duration.ZERO)} beside it; the
         * sink's builder refuses the pair otherwise.
         *
         * @param retryMaxDelay the maximum retry delay, at least 1 ms or {@code Duration.ZERO}
         * @return this builder
         */
        public Builder retryMaxDelay(Duration retryMaxDelay) {
            this.retryMaxDelay =
                    OptionChecks.checkAtLeastOneMilliOrZero(retryMaxDelay, "retryMaxDelay");
            return this;
        }

        /**
         * Sets the timeout of the first attempt of a {@code BatchWrite} call. Optional; defaults to
         * the library's 60 seconds.
         *
         * <p><b>{@code Duration.ZERO} is settable</b> and means what gax means by it: the call runs
         * indefinitely, until the connection itself ends. A positive sub-millisecond value is
         * refused, because gax reads this with {@code toMillis()} and it would silently become that
         * zero (ADR-0068).
         *
         * <p>It must not exceed {@link #retryMaxRpcTimeout(Duration)}, set or the library's 60
         * seconds; the sink's builder refuses the pair otherwise.
         *
         * @param retryInitialRpcTimeout the initial per-attempt timeout, at least 1 ms or {@code
         *     Duration.ZERO}
         * @return this builder
         */
        public Builder retryInitialRpcTimeout(Duration retryInitialRpcTimeout) {
            this.retryInitialRpcTimeout =
                    OptionChecks.checkAtLeastOneMilliOrZero(
                            retryInitialRpcTimeout, "retryInitialRpcTimeout");
            return this;
        }

        /**
         * Sets the factor a {@code BatchWrite} attempt's timeout grows by per attempt. Optional;
         * defaults to the library's 1.0.
         *
         * @param retryRpcTimeoutMultiplier the timeout multiplier, at least 1.0
         * @return this builder
         */
        public Builder retryRpcTimeoutMultiplier(double retryRpcTimeoutMultiplier) {
            Preconditions.checkArgument(
                    retryRpcTimeoutMultiplier >= 1.0,
                    "retryRpcTimeoutMultiplier must be at least 1.0, was %s",
                    retryRpcTimeoutMultiplier);
            this.retryRpcTimeoutMultiplier = retryRpcTimeoutMultiplier;
            return this;
        }

        /**
         * Caps the timeout of a {@code BatchWrite} attempt. Optional; defaults to the library's 60
         * seconds.
         *
         * <p><b>{@code Duration.ZERO} is settable</b> and means what gax means by it: a cap of
         * zero, which lets every call run indefinitely. A positive sub-millisecond value is
         * refused, because gax reads this with {@code toMillis()} and it would silently become that
         * zero (ADR-0068).
         *
         * <p>It must not be shorter than {@link #retryInitialRpcTimeout(Duration)}, set or the
         * library's 60 seconds, so a cap of zero needs {@code
         * retryInitialRpcTimeout(Duration.ZERO)} beside it; the sink's builder refuses the pair
         * otherwise.
         *
         * @param retryMaxRpcTimeout the maximum per-attempt timeout, at least 1 ms or {@code
         *     Duration.ZERO}
         * @return this builder
         */
        public Builder retryMaxRpcTimeout(Duration retryMaxRpcTimeout) {
            this.retryMaxRpcTimeout =
                    OptionChecks.checkAtLeastOneMilliOrZero(
                            retryMaxRpcTimeout, "retryMaxRpcTimeout");
            return this;
        }

        /**
         * Caps the transport's attempts at a {@code BatchWrite} call, the first one included.
         * Optional; defaults to the library's five. {@code 0} means what gax means by it: attempts
         * are bounded only by {@link #retryTotalTimeout(Duration)}.
         *
         * @param retryMaxAttempts the maximum attempts, non-negative
         * @return this builder
         */
        public Builder retryMaxAttempts(int retryMaxAttempts) {
            Preconditions.checkArgument(
                    retryMaxAttempts >= 0,
                    "retryMaxAttempts must not be negative, was %s",
                    retryMaxAttempts);
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        /**
         * Caps the writes the writer keeps unacknowledged. Defaults to {@value
         * #DEFAULT_MAX_IN_FLIGHT_WRITES}.
         *
         * @param maxInFlightWrites the in-flight cap, positive and at most 500 — the client
         *     library's pending-operation ceiling, past which it queues writes without bound
         * @return this builder
         */
        public Builder maxInFlightWrites(int maxInFlightWrites) {
            Preconditions.checkArgument(
                    maxInFlightWrites > 0,
                    "maxInFlightWrites must be positive, was %s",
                    maxInFlightWrites);
            Preconditions.checkArgument(
                    maxInFlightWrites <= MAX_IN_FLIGHT_WRITES_LIMIT,
                    "maxInFlightWrites must be at most %s, was %s: past that many pending writes"
                            + " the Firestore client library queues further ones without bound"
                            + " instead of sending them.",
                    MAX_IN_FLIGHT_WRITES_LIMIT,
                    maxInFlightWrites);
            this.maxInFlightWrites = maxInFlightWrites;
            return this;
        }

        /**
         * Caps the estimated size of the writes the writer keeps unacknowledged. Defaults to 64
         * MiB. This is the bound that actually bounds memory — a single document may be up to 1
         * MiB, so a count alone does not.
         *
         * @param maxInFlightBytes the in-flight byte cap, positive
         * @return this builder
         */
        public Builder maxInFlightBytes(long maxInFlightBytes) {
            Preconditions.checkArgument(
                    maxInFlightBytes > 0,
                    "maxInFlightBytes must be positive, was %s",
                    maxInFlightBytes);
            this.maxInFlightBytes = maxInFlightBytes;
            return this;
        }

        /**
         * Sets how many <em>consecutive</em> confirmed rejections fail the job. Defaults to {@value
         * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS}; {@link #UNBOUNDED} (-1) never fails it.
         *
         * <p>This bound only matters beside a {@code failedWriteHandler} that does not fail the
         * job, dropping or dead-lettering — under the default {@code failJob()} the first rejection
         * already fails the job. Beside one, it is what stops a stream whose writes Firestore
         * refuses wholesale from being shed one record at a time behind a green job: every refused
         * write is still routed, and the bound fails the job once that many arrive with none
         * applied in between. Two refusals do not count toward it, because they are what a
         * restart's replay answers rather than evidence of broken data: {@code ALREADY_EXISTS} for
         * a create, and {@code FAILED_PRECONDITION} for a conditional write. Records the serializer
         * rejects do not count either; the bound is about Firestore's refusals.
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

        private static void checkRate(int rate, String name) {
            Preconditions.checkArgument(
                    rate >= MIN_OPS_PER_SECOND,
                    "%s must be at least %s, was %s: the Firestore client library opens its first"
                            + " batch at %s writes, and a batch larger than the throttle's rate"
                            + " is never sent.",
                    name,
                    MIN_OPS_PER_SECOND,
                    rate,
                    MIN_OPS_PER_SECOND);
        }

        /**
         * Builds the options.
         *
         * @return the options
         * @throws IllegalStateException if a rate is set with throttling disabled, or the initial
         *     rate exceeds the ceiling — combinations the client library refuses when the writer
         *     opens
         */
        public FirestoreWriterOptions build() {
            Preconditions.checkState(
                    throttlingEnabled || (initialOpsPerSecond == null && maxOpsPerSecond == null),
                    "initialOpsPerSecond and maxOpsPerSecond set the throttle's rates, so they"
                            + " cannot be combined with throttlingEnabled(false).");
            Preconditions.checkState(
                    initialOpsPerSecond == null
                            || maxOpsPerSecond == null
                            || initialOpsPerSecond <= maxOpsPerSecond,
                    "initialOpsPerSecond (%s) must not exceed maxOpsPerSecond (%s).",
                    initialOpsPerSecond,
                    maxOpsPerSecond);
            return new FirestoreWriterOptions(this);
        }
    }
}
