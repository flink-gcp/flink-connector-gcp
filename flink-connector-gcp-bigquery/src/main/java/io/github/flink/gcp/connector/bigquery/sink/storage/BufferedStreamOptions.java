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

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Options specific to {@link WriteMethod#STORAGE_API_EXACTLY_ONCE}: how large append requests may
 * grow and how the connector-driven recovery schedule (stream creation, transient append failures,
 * the restore probe) backs off.
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

    private final long maxAppendRequestBytes;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;

    private BufferedStreamOptions(Builder builder) {
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
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxAppendRequestBytes,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts);
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
                + "}";
    }

    /** Builder for {@link BufferedStreamOptions}. */
    @PublicEvolving
    public static final class Builder {

        private long maxAppendRequestBytes = DEFAULT_MAX_APPEND_REQUEST_BYTES;
        private Duration recoveryInitialBackoff = DEFAULT_RECOVERY_INITIAL_BACKOFF;
        private Duration recoveryMaxBackoff = DEFAULT_RECOVERY_MAX_BACKOFF;
        private int recoveryMaxAttempts = DEFAULT_RECOVERY_MAX_ATTEMPTS;

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
            Preconditions.checkNotNull(
                    recoveryInitialBackoff, "recoveryInitialBackoff must not be null");
            Preconditions.checkArgument(
                    !recoveryInitialBackoff.isNegative() && !recoveryInitialBackoff.isZero(),
                    "recoveryInitialBackoff must be positive: %s",
                    recoveryInitialBackoff);
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
            Preconditions.checkNotNull(recoveryMaxBackoff, "recoveryMaxBackoff must not be null");
            Preconditions.checkArgument(
                    !recoveryMaxBackoff.isNegative() && !recoveryMaxBackoff.isZero(),
                    "recoveryMaxBackoff must be positive: %s",
                    recoveryMaxBackoff);
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
            return new BufferedStreamOptions(this);
        }
    }
}
