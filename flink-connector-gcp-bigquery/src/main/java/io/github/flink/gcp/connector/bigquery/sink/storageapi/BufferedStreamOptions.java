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

package io.github.flink.gcp.connector.bigquery.sink.storageapi;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Options specific to {@link WriteMethod#STORAGE_API_EXACTLY_ONCE}: how large append requests may
 * grow and how connector-driven retries (stream creation, transient append failures, the restore
 * probe) back off.
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

    /** Default for {@link Builder#retryInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_RETRY_INITIAL_BACKOFF = Duration.ofMillis(500);

    /** Default for {@link Builder#retryMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_RETRY_MAX_BACKOFF = Duration.ofSeconds(10);

    /** Default for {@link Builder#retryMaxAttempts(int)}. */
    public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 10;

    private final long maxAppendRequestBytes;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final int retryMaxAttempts;

    private BufferedStreamOptions(Builder builder) {
        this.maxAppendRequestBytes = builder.maxAppendRequestBytes;
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

    /** Returns the largest serialized-row payload sent in one append request, in bytes. */
    public long getMaxAppendRequestBytes() {
        return maxAppendRequestBytes;
    }

    /** Returns the first backoff of the connector-driven retry schedule. */
    public Duration getRetryInitialBackoff() {
        return retryInitialBackoff;
    }

    /** Returns the backoff cap of the connector-driven retry schedule. */
    public Duration getRetryMaxBackoff() {
        return retryMaxBackoff;
    }

    /** Returns the maximum number of attempts of the connector-driven retry schedule. */
    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
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
                && retryMaxAttempts == that.retryMaxAttempts
                && retryInitialBackoff.equals(that.retryInitialBackoff)
                && retryMaxBackoff.equals(that.retryMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxAppendRequestBytes, retryInitialBackoff, retryMaxBackoff, retryMaxAttempts);
    }

    @Override
    public String toString() {
        return "BufferedStreamOptions{maxAppendRequestBytes="
                + maxAppendRequestBytes
                + ", retryInitialBackoff="
                + retryInitialBackoff
                + ", retryMaxBackoff="
                + retryMaxBackoff
                + ", retryMaxAttempts="
                + retryMaxAttempts
                + "}";
    }

    /** Builder for {@link BufferedStreamOptions}. */
    @PublicEvolving
    public static final class Builder {

        private long maxAppendRequestBytes = DEFAULT_MAX_APPEND_REQUEST_BYTES;
        private Duration retryInitialBackoff = DEFAULT_RETRY_INITIAL_BACKOFF;
        private Duration retryMaxBackoff = DEFAULT_RETRY_MAX_BACKOFF;
        private int retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;

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
         * Sets the first backoff of the connector-driven retry schedule (stream creation after
         * table auto-creation, transient append failures, the restore probe). Defaults to {@link
         * #DEFAULT_RETRY_INITIAL_BACKOFF}.
         *
         * @param retryInitialBackoff the first backoff
         * @return this builder
         */
        public Builder retryInitialBackoff(Duration retryInitialBackoff) {
            Preconditions.checkNotNull(retryInitialBackoff, "retryInitialBackoff must not be null");
            Preconditions.checkArgument(
                    !retryInitialBackoff.isNegative() && !retryInitialBackoff.isZero(),
                    "retryInitialBackoff must be positive: %s",
                    retryInitialBackoff);
            this.retryInitialBackoff = retryInitialBackoff;
            return this;
        }

        /**
         * Sets the backoff cap of the connector-driven retry schedule. Must be at least the initial
         * backoff. Defaults to {@link #DEFAULT_RETRY_MAX_BACKOFF}.
         *
         * @param retryMaxBackoff the backoff cap
         * @return this builder
         */
        public Builder retryMaxBackoff(Duration retryMaxBackoff) {
            Preconditions.checkNotNull(retryMaxBackoff, "retryMaxBackoff must not be null");
            Preconditions.checkArgument(
                    !retryMaxBackoff.isNegative() && !retryMaxBackoff.isZero(),
                    "retryMaxBackoff must be positive: %s",
                    retryMaxBackoff);
            this.retryMaxBackoff = retryMaxBackoff;
            return this;
        }

        /**
         * Sets the maximum number of attempts of the connector-driven retry schedule. Defaults to
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
         * Builds the options.
         *
         * @return the options
         */
        public BufferedStreamOptions build() {
            Preconditions.checkState(
                    retryMaxBackoff.compareTo(retryInitialBackoff) >= 0,
                    "retryMaxBackoff must be >= retryInitialBackoff: %s < %s",
                    retryMaxBackoff,
                    retryInitialBackoff);
            return new BufferedStreamOptions(this);
        }
    }
}
