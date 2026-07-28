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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Options specific to {@link WriteMethod#STORAGE_API_AT_LEAST_ONCE}: how large append requests may
 * grow, how the connector-driven re-append budget backs off, how the SDK retries retriable append
 * failures in-stream, how the SDK's connection pool (multiplexing) is sized, and the writer's
 * housekeeping ({@code destinationIdleTimeout}, {@code flushInterval}).
 *
 * <p>Set via {@link BigQuerySinkBuilder#defaultStreamOptions(DefaultStreamOptions)}; optional for a
 * {@code STORAGE_API_AT_LEAST_ONCE} sink (all knobs are defaulted, and an unconfigured sink uses
 * the defaults) and rejected for every other write method.
 *
 * <p>Three groups of knobs configure three distinct layers:
 *
 * <ul>
 *   <li><b>{@code recovery*} and {@code maxAppendRequestBytes}</b> — the connector's own bounded
 *       recovery budget: re-appends on a rebuilt stream writer after a failure surfaced past the
 *       SDK's retries (same vocabulary as {@link BufferedStreamOptions}). The schedule pacing
 *       schema-update propagation waits is deliberately not exposed: it tracks a BigQuery service
 *       property, not a workload property.
 *   <li><b>{@code retry*} and {@code maxRetryDuration}</b> — the SDK's in-stream retry of retriable
 *       append failures on default streams, handed to the stream writer as {@code RetrySettings} /
 *       {@code setMaxRetryDuration}, spelled the SDK's way.
 *   <li><b>{@code maxInflight*} and {@code *ConnectionsPerRegion}</b> — the SDK's connection pool.
 * </ul>
 *
 * <p><b>The SDK connection pool is JVM-static per (location, credentials).</b> The first stream
 * writer built for a pool key bakes its in-flight limits, SDK retry settings and {@code
 * maxRetryDuration} into that pool permanently; later writers' values are silently dropped by the
 * SDK. All writers of one sink carry the same options, so within a job the pool is consistent — but
 * on a session cluster, or with another BigQuery Storage Write API client in the same JVM,
 * whichever builds first wins. The {@code *ConnectionsPerRegion} knobs are likewise JVM-global
 * ({@code ConnectionWorkerPool.setOptions}): the connector applies them once per JVM before
 * building its first writer, the floor is latched when a pool is constructed, and a second sink
 * configuring different pool bounds in the same JVM is ignored with a warning.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class DefaultStreamOptions implements Serializable {

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

    /** Default for {@link Builder#maxRetryDuration(Duration)}: the SDK's own default. */
    public static final Duration DEFAULT_MAX_RETRY_DURATION = Duration.ofMinutes(5);

    /**
     * Default for {@link Builder#maxInflightRequests(int)}: 100, following the official
     * multiplexing guidance, and deliberately below the SDK's own default of 1000. A pooled
     * connection counts as busy above 20% of this limit, so the SDK default would let a connection
     * queue 200 requests before the pool considers scaling up; 100 makes load-based scale-up
     * actually trigger.
     */
    public static final int DEFAULT_MAX_INFLIGHT_REQUESTS = 100;

    /** Default for {@link Builder#maxInflightBytes(long)}: the SDK's own default of 100 MiB. */
    public static final long DEFAULT_MAX_INFLIGHT_BYTES = 100L * 1024 * 1024;

    /** Default for {@link Builder#minConnectionsPerRegion(int)}: the SDK's own default. */
    public static final int DEFAULT_MIN_CONNECTIONS_PER_REGION = 2;

    /** Default for {@link Builder#maxConnectionsPerRegion(int)}: the SDK's own default. */
    public static final int DEFAULT_MAX_CONNECTIONS_PER_REGION = 20;

    /**
     * Default for {@link Builder#destinationIdleTimeout(Duration)}: one hour. Coarse on purpose —
     * eviction is memory hygiene for long-lived jobs with dynamic destinations (for example
     * date-suffixed tables), and an evicted destination that receives a record again just rebuilds
     * its stream writer once.
     */
    public static final Duration DEFAULT_DESTINATION_IDLE_TIMEOUT = Duration.ofHours(1);

    private final long maxAppendRequestBytes;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;
    private final Duration retryInitialDelay;
    private final double retryDelayMultiplier;
    private final Duration retryMaxDelay;
    private final int retryMaxAttempts;
    private final Duration maxRetryDuration;
    private final int maxInflightRequests;
    private final long maxInflightBytes;
    private final int minConnectionsPerRegion;
    private final int maxConnectionsPerRegion;
    private final Duration destinationIdleTimeout;
    @Nullable private final Duration flushInterval;

    private DefaultStreamOptions(Builder builder) {
        this.maxAppendRequestBytes = builder.maxAppendRequestBytes;
        this.recoveryInitialBackoff = builder.recoveryInitialBackoff;
        this.recoveryMaxBackoff = builder.recoveryMaxBackoff;
        this.recoveryMaxAttempts = builder.recoveryMaxAttempts;
        this.retryInitialDelay = builder.retryInitialDelay;
        this.retryDelayMultiplier = builder.retryDelayMultiplier;
        this.retryMaxDelay = builder.retryMaxDelay;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.maxRetryDuration = builder.maxRetryDuration;
        this.maxInflightRequests = builder.maxInflightRequests;
        this.maxInflightBytes = builder.maxInflightBytes;
        this.minConnectionsPerRegion = builder.minConnectionsPerRegion;
        this.maxConnectionsPerRegion = builder.maxConnectionsPerRegion;
        this.destinationIdleTimeout = builder.destinationIdleTimeout;
        this.flushInterval = builder.flushInterval;
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

    /** Returns the SDK's in-flight append request cap per pooled connection. */
    public int getMaxInflightRequests() {
        return maxInflightRequests;
    }

    /** Returns the SDK's in-flight append bytes cap per pooled connection. */
    public long getMaxInflightBytes() {
        return maxInflightBytes;
    }

    /** Returns the connection pool's starting connection count per (location, credentials). */
    public int getMinConnectionsPerRegion() {
        return minConnectionsPerRegion;
    }

    /** Returns the connection pool's connection ceiling per (location, credentials). */
    public int getMaxConnectionsPerRegion() {
        return maxConnectionsPerRegion;
    }

    /** Returns how long a destination may go without records before its writer is evicted. */
    public Duration getDestinationIdleTimeout() {
        return destinationIdleTimeout;
    }

    /** Returns the periodic flush interval, or {@code null} when periodic flushing is disabled. */
    @Nullable
    public Duration getFlushInterval() {
        return flushInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultStreamOptions that = (DefaultStreamOptions) o;
        return maxAppendRequestBytes == that.maxAppendRequestBytes
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && Double.compare(retryDelayMultiplier, that.retryDelayMultiplier) == 0
                && retryMaxAttempts == that.retryMaxAttempts
                && maxInflightRequests == that.maxInflightRequests
                && maxInflightBytes == that.maxInflightBytes
                && minConnectionsPerRegion == that.minConnectionsPerRegion
                && maxConnectionsPerRegion == that.maxConnectionsPerRegion
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff)
                && retryInitialDelay.equals(that.retryInitialDelay)
                && retryMaxDelay.equals(that.retryMaxDelay)
                && maxRetryDuration.equals(that.maxRetryDuration)
                && destinationIdleTimeout.equals(that.destinationIdleTimeout)
                && Objects.equals(flushInterval, that.flushInterval);
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
                maxRetryDuration,
                maxInflightRequests,
                maxInflightBytes,
                minConnectionsPerRegion,
                maxConnectionsPerRegion,
                destinationIdleTimeout,
                flushInterval);
    }

    @Override
    public String toString() {
        return "DefaultStreamOptions{maxAppendRequestBytes="
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
                + ", maxInflightRequests="
                + maxInflightRequests
                + ", maxInflightBytes="
                + maxInflightBytes
                + ", minConnectionsPerRegion="
                + minConnectionsPerRegion
                + ", maxConnectionsPerRegion="
                + maxConnectionsPerRegion
                + ", destinationIdleTimeout="
                + destinationIdleTimeout
                + ", flushInterval="
                + flushInterval
                + "}";
    }

    /** Builder for {@link DefaultStreamOptions}. */
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
        private int maxInflightRequests = DEFAULT_MAX_INFLIGHT_REQUESTS;
        private long maxInflightBytes = DEFAULT_MAX_INFLIGHT_BYTES;
        private int minConnectionsPerRegion = DEFAULT_MIN_CONNECTIONS_PER_REGION;
        private int maxConnectionsPerRegion = DEFAULT_MAX_CONNECTIONS_PER_REGION;
        private Duration destinationIdleTimeout = DEFAULT_DESTINATION_IDLE_TIMEOUT;
        @Nullable private Duration flushInterval;

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
         * Sets the first backoff of the connector-driven recovery schedule (re-appends after table
         * auto-creation, transient append failures past the SDK's own retries, stale-writer
         * refreshes). Defaults to {@link #DEFAULT_RECOVERY_INITIAL_BACKOFF}.
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
         * Sets the first delay of the SDK's in-stream retry of retriable append failures (for
         * example {@code ABORTED}, {@code UNAVAILABLE} and quota {@code RESOURCE_EXHAUSTED}).
         * Defaults to {@link #DEFAULT_RETRY_INITIAL_DELAY}.
         *
         * <p>The connection pool adopts the first writer's SDK retry settings per JVM; see the
         * class javadoc.
         *
         * @param retryInitialDelay the first retry delay
         * @return this builder
         */
        public Builder retryInitialDelay(Duration retryInitialDelay) {
            Preconditions.checkNotNull(retryInitialDelay, "retryInitialDelay must not be null");
            Preconditions.checkArgument(
                    !retryInitialDelay.isNegative() && !retryInitialDelay.isZero(),
                    "retryInitialDelay must be positive: %s",
                    retryInitialDelay);
            this.retryInitialDelay = retryInitialDelay;
            return this;
        }

        /**
         * Sets the delay multiplier of the SDK's in-stream retry schedule. Defaults to {@link
         * #DEFAULT_RETRY_DELAY_MULTIPLIER}.
         *
         * @param retryDelayMultiplier the multiplier, at least 1.0
         * @return this builder
         */
        public Builder retryDelayMultiplier(double retryDelayMultiplier) {
            Preconditions.checkArgument(
                    retryDelayMultiplier >= 1.0,
                    "retryDelayMultiplier must be at least 1.0: %s",
                    retryDelayMultiplier);
            this.retryDelayMultiplier = retryDelayMultiplier;
            return this;
        }

        /**
         * Sets the delay cap of the SDK's in-stream retry schedule. Must be at least the initial
         * delay. Defaults to {@link #DEFAULT_RETRY_MAX_DELAY}.
         *
         * @param retryMaxDelay the delay cap
         * @return this builder
         */
        public Builder retryMaxDelay(Duration retryMaxDelay) {
            Preconditions.checkNotNull(retryMaxDelay, "retryMaxDelay must not be null");
            Preconditions.checkArgument(
                    !retryMaxDelay.isNegative() && !retryMaxDelay.isZero(),
                    "retryMaxDelay must be positive: %s",
                    retryMaxDelay);
            this.retryMaxDelay = retryMaxDelay;
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
            Preconditions.checkNotNull(maxRetryDuration, "maxRetryDuration must not be null");
            Preconditions.checkArgument(
                    !maxRetryDuration.isNegative() && !maxRetryDuration.isZero(),
                    "maxRetryDuration must be positive: %s",
                    maxRetryDuration);
            this.maxRetryDuration = maxRetryDuration;
            return this;
        }

        /**
         * Sets the in-flight append request cap per pooled connection. The writer blocks once a
         * connection reaches it, and the pool considers a connection busy — a scale-up candidate —
         * above 20% of it. Defaults to {@link #DEFAULT_MAX_INFLIGHT_REQUESTS}, which is 100
         * following the official multiplexing guidance rather than the SDK's 1000: with the SDK
         * default, load-based pool scale-up rarely triggers.
         *
         * @param maxInflightRequests the per-connection in-flight request cap
         * @return this builder
         */
        public Builder maxInflightRequests(int maxInflightRequests) {
            Preconditions.checkArgument(
                    maxInflightRequests > 0,
                    "maxInflightRequests must be positive: %s",
                    maxInflightRequests);
            this.maxInflightRequests = maxInflightRequests;
            return this;
        }

        /**
         * Sets the in-flight append bytes cap per pooled connection. Defaults to {@link
         * #DEFAULT_MAX_INFLIGHT_BYTES}, the SDK's own default.
         *
         * @param maxInflightBytes the per-connection in-flight bytes cap
         * @return this builder
         */
        public Builder maxInflightBytes(long maxInflightBytes) {
            Preconditions.checkArgument(
                    maxInflightBytes > 0,
                    "maxInflightBytes must be positive: %s",
                    maxInflightBytes);
            this.maxInflightBytes = maxInflightBytes;
            return this;
        }

        /**
         * Sets the connection pool's starting connection count per (location, credentials). Latched
         * by the SDK when the pool is constructed, so it only takes effect if this sink builds the
         * JVM's first pooled writer for the key. Defaults to {@link
         * #DEFAULT_MIN_CONNECTIONS_PER_REGION}, the SDK's own default.
         *
         * @param minConnectionsPerRegion the starting connection count
         * @return this builder
         */
        public Builder minConnectionsPerRegion(int minConnectionsPerRegion) {
            Preconditions.checkArgument(
                    minConnectionsPerRegion > 0,
                    "minConnectionsPerRegion must be positive: %s",
                    minConnectionsPerRegion);
            this.minConnectionsPerRegion = minConnectionsPerRegion;
            return this;
        }

        /**
         * Sets the connection pool's connection ceiling per (location, credentials). Must be at
         * least {@code minConnectionsPerRegion}. Defaults to {@link
         * #DEFAULT_MAX_CONNECTIONS_PER_REGION}, the SDK's own default.
         *
         * @param maxConnectionsPerRegion the connection ceiling
         * @return this builder
         */
        public Builder maxConnectionsPerRegion(int maxConnectionsPerRegion) {
            Preconditions.checkArgument(
                    maxConnectionsPerRegion > 0,
                    "maxConnectionsPerRegion must be positive: %s",
                    maxConnectionsPerRegion);
            this.maxConnectionsPerRegion = maxConnectionsPerRegion;
            return this;
        }

        /**
         * Sets how long a destination may go without records before the writer closes and drops its
         * stream writer. Eviction is memory hygiene for long-lived jobs with dynamic destinations
         * (for example date-suffixed tables), whose per-destination state otherwise grows without
         * bound; correctness is unaffected, and a destination that receives a record again after
         * eviction rebuilds its stream writer transparently. The sweep runs at the end of each
         * successful flush, when nothing is pending or in flight. Defaults to {@link
         * #DEFAULT_DESTINATION_IDLE_TIMEOUT}; to never evict, set a very large duration.
         *
         * @param destinationIdleTimeout the idle timeout
         * @return this builder
         */
        public Builder destinationIdleTimeout(Duration destinationIdleTimeout) {
            Preconditions.checkNotNull(
                    destinationIdleTimeout, "destinationIdleTimeout must not be null");
            Preconditions.checkArgument(
                    !destinationIdleTimeout.isNegative() && !destinationIdleTimeout.isZero(),
                    "destinationIdleTimeout must be positive: %s",
                    destinationIdleTimeout);
            this.destinationIdleTimeout = destinationIdleTimeout;
            return this;
        }

        /**
         * Enables a periodic time-based flush: every interval, the writer appends all pending
         * batches and awaits every in-flight append, exactly as the checkpoint flush does. Disabled
         * by default.
         *
         * <p>This is a mitigation for streaming jobs running <em>without</em> checkpointing, where
         * Flink only flushes at end of input, so sub-threshold buffers would otherwise sit
         * unacknowledged indefinitely and be lost on failure. It bounds that window; it does not
         * replace the documented at-least-once guarantee, which requires checkpointing.
         *
         * @param flushInterval the flush interval
         * @return this builder
         */
        public Builder flushInterval(Duration flushInterval) {
            Preconditions.checkNotNull(flushInterval, "flushInterval must not be null");
            Preconditions.checkArgument(
                    !flushInterval.isNegative() && !flushInterval.isZero(),
                    "flushInterval must be positive: %s",
                    flushInterval);
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public DefaultStreamOptions build() {
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
            Preconditions.checkState(
                    maxConnectionsPerRegion >= minConnectionsPerRegion,
                    "maxConnectionsPerRegion must be >= minConnectionsPerRegion: %s < %s",
                    maxConnectionsPerRegion,
                    minConnectionsPerRegion);
            return new DefaultStreamOptions(this);
        }
    }
}
