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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Options specific to {@link WriteMethod#STORAGE_API_EXACTLY_ONCE}: how large append requests may
 * grow, how the connector-driven recovery schedule backs off, and how the SDK retries retriable
 * append failures in-stream.
 *
 * <p>Two groups of knobs configure two distinct layers:
 *
 * <ul>
 *   <li><b>{@code recovery*} and {@code maxAppendRequestBytes}</b> — the connector's own bounded
 *       recovery budget: stream creation, transient append failures that surfaced past the SDK's
 *       retries, and the restore probe (same vocabulary as {@link DefaultStreamOptions}).
 *   <li><b>{@code retry*} and {@code maxRetryDuration}</b> — the SDK's in-stream retry of retriable
 *       append failures (for example {@code ABORTED}, {@code UNAVAILABLE}, {@code CANCELLED},
 *       {@code INTERNAL}, {@code DEADLINE_EXCEEDED} and quota {@code RESOURCE_EXHAUSTED}), so
 *       transient errors are normally absorbed before they reach the writer, whose own budget sits
 *       above them. Handed to the stream writer as {@code RetrySettings} / {@code
 *       setMaxRetryDuration}, spelled the SDK's way. A retry of an offset append that already
 *       landed answers {@code ALREADY_EXISTS}, which the writer treats as success.
 * </ul>
 *
 * <p>Unlike the default-stream path these appenders never enter the SDK's connection pool — each
 * buffered stream gets a dedicated writer — so there is no first-writer-wins caveat here and no
 * pool-sizing knob.
 *
 * <p>Set via {@link BigQuerySinkBuilder#bufferedStreamOptions(BufferedStreamOptions)}; required
 * when building a {@code STORAGE_API_EXACTLY_ONCE} sink and rejected for every other write method.
 * All knobs are defaulted, so {@code BufferedStreamOptions.builder().build()} is a valid
 * configuration.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class BufferedStreamOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Default for {@link Builder#maxAppendRequestBytes(long)}: 512 KiB of serialized rows per
     * append request, well under the 10 MB request limit while amortizing per-request overhead.
     */
    public static final long DEFAULT_MAX_APPEND_REQUEST_BYTES = 512 * 1024;

    /** Default for {@link Builder#recoveryInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_RECOVERY_INITIAL_BACKOFF = Duration.ofMillis(500);

    /** Default for {@link Builder#recoveryMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_RECOVERY_MAX_BACKOFF = Duration.ofSeconds(10);

    /** Default for {@link Builder#recoveryMaxAttempts(int)}. */
    public static final int DEFAULT_RECOVERY_MAX_ATTEMPTS = 10;

    /** Default for {@link Builder#retryInitialDelay(Duration)}. */
    public static final Duration DEFAULT_RETRY_INITIAL_DELAY = Duration.ofMillis(500);

    /** Default for {@link Builder#retryDelayMultiplier(double)}. */
    public static final double DEFAULT_RETRY_DELAY_MULTIPLIER = 2.0;

    /** Default for {@link Builder#retryMaxDelay(Duration)}. */
    public static final Duration DEFAULT_RETRY_MAX_DELAY = Duration.ofSeconds(30);

    /** Default for {@link Builder#retryMaxAttempts(int)}. */
    public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 5;

    /**
     * Default for {@link Builder#maxRetryDuration(Duration)}: the SDK's own default, read from
     * {@code StreamWriter.Builder} in google-cloud-bigquerystorage 3.30.0. Pinned here rather than
     * inherited, so a later SDK default does not silently change this path's behavior — but that
     * also means this constant no longer tracks the SDK, which is why it names the version it was
     * taken from.
     */
    public static final Duration DEFAULT_MAX_RETRY_DURATION = Duration.ofMinutes(5);

    private final long maxAppendRequestBytes;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;
    private final Duration retryInitialDelay;
    private final double retryDelayMultiplier;
    private final Duration retryMaxDelay;
    private final int retryMaxAttempts;
    private final Duration maxRetryDuration;

    private BufferedStreamOptions(Builder builder) {
        this.retryInitialDelay = builder.retryInitialDelay;
        this.retryDelayMultiplier = builder.retryDelayMultiplier;
        this.retryMaxDelay = builder.retryMaxDelay;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.maxRetryDuration = builder.maxRetryDuration;
        this.maxAppendRequestBytes = builder.maxAppendRequestBytes;
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

    /** Returns the largest serialized-row payload sent in one append request, in bytes. */
    public long getMaxAppendRequestBytes() {
        return maxAppendRequestBytes;
    }

    /** Returns the first backoff of the connector-driven recovery schedule. */
    public Duration getRecoveryInitialBackoff() {
        return recoveryInitialBackoff;
    }

    /** Returns the backoff cap of the connector-driven recovery schedule. */
    public Duration getRecoveryMaxBackoff() {
        return recoveryMaxBackoff;
    }

    /** Returns the maximum number of attempts of the connector-driven recovery schedule. */
    public int getRecoveryMaxAttempts() {
        return recoveryMaxAttempts;
    }

    /** Returns the first delay of the SDK's in-stream retry schedule. */
    public Duration getRetryInitialDelay() {
        return retryInitialDelay;
    }

    /** Returns the delay multiplier of the SDK's in-stream retry schedule. */
    public double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    /** Returns the delay cap of the SDK's in-stream retry schedule. */
    public Duration getRetryMaxDelay() {
        return retryMaxDelay;
    }

    /** Returns the maximum number of attempts of the SDK's in-stream retry schedule. */
    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    /** Returns the SDK's overall ceiling on retrying one retriable in-stream failure. */
    public Duration getMaxRetryDuration() {
        return maxRetryDuration;
    }

    /**
     * Returns the connector-driven recovery schedule the {@code recovery*} knobs describe, shared
     * by the writer and the committer. Jittered: the writer, the committer and every parallel
     * subtask of both back off against the same table.
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
        BufferedStreamOptions that = (BufferedStreamOptions) o;
        return maxAppendRequestBytes == that.maxAppendRequestBytes
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff)
                && retryMaxAttempts == that.retryMaxAttempts
                && Double.compare(retryDelayMultiplier, that.retryDelayMultiplier) == 0
                && retryInitialDelay.equals(that.retryInitialDelay)
                && retryMaxDelay.equals(that.retryMaxDelay)
                && maxRetryDuration.equals(that.maxRetryDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxAppendRequestBytes,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts,
                retryInitialDelay,
                retryDelayMultiplier,
                retryMaxDelay,
                retryMaxAttempts,
                maxRetryDuration);
    }

    @Override
    public String toString() {
        return "BufferedStreamOptions{maxAppendRequestBytes="
                + maxAppendRequestBytes
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + ", retryInitialDelay="
                + retryInitialDelay
                + ", retryDelayMultiplier="
                + retryDelayMultiplier
                + ", retryMaxDelay="
                + retryMaxDelay
                + ", retryMaxAttempts="
                + retryMaxAttempts
                + ", maxRetryDuration="
                + maxRetryDuration
                + "}";
    }

    /** Builder for {@link BufferedStreamOptions}. */
    @PublicEvolving
    public static final class Builder {

        private long maxAppendRequestBytes = DEFAULT_MAX_APPEND_REQUEST_BYTES;
        private Duration recoveryInitialBackoff = DEFAULT_RECOVERY_INITIAL_BACKOFF;
        private Duration recoveryMaxBackoff = DEFAULT_RECOVERY_MAX_BACKOFF;
        private int recoveryMaxAttempts = DEFAULT_RECOVERY_MAX_ATTEMPTS;
        private Duration retryInitialDelay = DEFAULT_RETRY_INITIAL_DELAY;
        private double retryDelayMultiplier = DEFAULT_RETRY_DELAY_MULTIPLIER;
        private Duration retryMaxDelay = DEFAULT_RETRY_MAX_DELAY;
        private int retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
        private Duration maxRetryDuration = DEFAULT_MAX_RETRY_DURATION;

        private Builder() {}

        /**
         * Sets the largest serialized-row payload sent in one append request. Larger rows are still
         * sent (one row per request); the Storage Write API rejects requests over 10 MB. Defaults
         * to {@link #DEFAULT_MAX_APPEND_REQUEST_BYTES}.
         *
         * @param maxAppendRequestBytes the request payload cap in bytes
         * @return this builder
         */
        public Builder maxAppendRequestBytes(long maxAppendRequestBytes) {
            Preconditions.checkArgument(
                    maxAppendRequestBytes > 0,
                    "maxAppendRequestBytes must be positive: %s",
                    maxAppendRequestBytes);
            this.maxAppendRequestBytes = maxAppendRequestBytes;
            return this;
        }

        /**
         * Sets the first backoff of the connector-driven recovery schedule (stream creation after
         * table auto-creation, transient append failures, the restore probe). Defaults to {@link
         * #DEFAULT_RECOVERY_INITIAL_BACKOFF}.
         *
         * @param recoveryInitialBackoff the first backoff
         * @return this builder
         */
        public Builder recoveryInitialBackoff(Duration recoveryInitialBackoff) {
            OptionChecks.checkPositive(recoveryInitialBackoff, "recoveryInitialBackoff");
            this.recoveryInitialBackoff = recoveryInitialBackoff;
            return this;
        }

        /**
         * Sets the backoff cap of the connector-driven recovery schedule. Must be at least the
         * initial backoff. Defaults to {@link #DEFAULT_RECOVERY_MAX_BACKOFF}.
         *
         * @param recoveryMaxBackoff the backoff cap
         * @return this builder
         */
        public Builder recoveryMaxBackoff(Duration recoveryMaxBackoff) {
            OptionChecks.checkPositive(recoveryMaxBackoff, "recoveryMaxBackoff");
            this.recoveryMaxBackoff = recoveryMaxBackoff;
            return this;
        }

        /**
         * Sets the maximum number of attempts of the connector-driven recovery schedule. Defaults
         * to {@link #DEFAULT_RECOVERY_MAX_ATTEMPTS}.
         *
         * @param recoveryMaxAttempts the attempt cap
         * @return this builder
         */
        public Builder recoveryMaxAttempts(int recoveryMaxAttempts) {
            Preconditions.checkArgument(
                    recoveryMaxAttempts > 0,
                    "recoveryMaxAttempts must be positive: %s",
                    recoveryMaxAttempts);
            this.recoveryMaxAttempts = recoveryMaxAttempts;
            return this;
        }

        /**
         * Sets the first delay of the SDK's in-stream retry of retriable append failures. Defaults
         * to {@link #DEFAULT_RETRY_INITIAL_DELAY}.
         *
         * @param retryInitialDelay the first retry delay
         * @return this builder
         */
        public Builder retryInitialDelay(Duration retryInitialDelay) {
            this.retryInitialDelay = checkAtLeastOneMilli(retryInitialDelay, "retryInitialDelay");
            return this;
        }

        /**
         * Sets the delay multiplier of the SDK's in-stream retry schedule. Must be at least 1.
         * Defaults to {@link #DEFAULT_RETRY_DELAY_MULTIPLIER}.
         *
         * @param retryDelayMultiplier the delay multiplier
         * @return this builder
         */
        public Builder retryDelayMultiplier(double retryDelayMultiplier) {
            Preconditions.checkArgument(
                    retryDelayMultiplier >= 1.0,
                    "retryDelayMultiplier must be >= 1: %s",
                    retryDelayMultiplier);
            this.retryDelayMultiplier = retryDelayMultiplier;
            return this;
        }

        /**
         * Caps the delay of the SDK's in-stream retry schedule. Must be at least the initial delay.
         * Defaults to {@link #DEFAULT_RETRY_MAX_DELAY}.
         *
         * @param retryMaxDelay the delay cap
         * @return this builder
         */
        public Builder retryMaxDelay(Duration retryMaxDelay) {
            this.retryMaxDelay = checkAtLeastOneMilli(retryMaxDelay, "retryMaxDelay");
            return this;
        }

        /**
         * Sets the maximum number of attempts of the SDK's in-stream retry schedule. Defaults to
         * {@link #DEFAULT_RETRY_MAX_ATTEMPTS}.
         *
         * @param retryMaxAttempts the attempt cap
         * @return this builder
         */
        public Builder retryMaxAttempts(int retryMaxAttempts) {
            Preconditions.checkArgument(
                    retryMaxAttempts > 0,
                    "retryMaxAttempts must be positive: %s",
                    retryMaxAttempts);
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        /**
         * Sets the SDK's overall ceiling on retrying one retriable in-stream failure, across all
         * attempts. Defaults to {@link #DEFAULT_MAX_RETRY_DURATION}, the SDK's own default.
         *
         * @param maxRetryDuration the overall retry ceiling
         * @return this builder
         */
        public Builder maxRetryDuration(Duration maxRetryDuration) {
            this.maxRetryDuration = checkAtLeastOneMilli(maxRetryDuration, "maxRetryDuration");
            return this;
        }

        private static Duration checkAtLeastOneMilli(Duration value, String name) {
            Preconditions.checkNotNull(value, name + " must not be null");
            Preconditions.checkArgument(
                    value.toMillis() > 0, name + " must be at least 1 ms: %s", value);
            return value;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public BufferedStreamOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be >= recoveryInitialBackoff: %s < %s",
                    recoveryMaxBackoff,
                    recoveryInitialBackoff);
            Preconditions.checkState(
                    retryMaxDelay.compareTo(retryInitialDelay) >= 0,
                    "retryMaxDelay must be >= retryInitialDelay: %s < %s",
                    retryMaxDelay,
                    retryInitialDelay);
            return new BufferedStreamOptions(this);
        }
    }
}
