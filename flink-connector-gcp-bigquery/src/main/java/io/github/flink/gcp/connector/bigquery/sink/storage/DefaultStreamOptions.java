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

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Options specific to {@link WriteMethod#STORAGE_API_AT_LEAST_ONCE}: how large append requests may
 * grow, how the connector-driven re-append budget backs off, how the SDK retries retriable append
 * failures in-stream, and how the SDK's connection pool (multiplexing) is sized.
 *
 * <p>Set via {@link BigQuerySinkBuilder#defaultStreamOptions(DefaultStreamOptions)}; optional for a
 * {@code STORAGE_API_AT_LEAST_ONCE} sink (all knobs are defaulted, and an unconfigured sink uses
 * the defaults) and rejected for every other write method.
 *
 * <p>Three groups of knobs configure three distinct layers:
 *
 * <ul>
 *   <li><b>{@code retry*} and {@code maxAppendRequestBytes}</b> — the connector's own bounded
 *       re-append budget, sitting above the SDK's retries (same vocabulary as {@link
 *       BufferedStreamOptions}). The schedule pacing schema-update propagation waits is
 *       deliberately not exposed: it tracks a BigQuery service property, not a workload property.
 *   <li><b>{@code sdkRetry*} and {@code sdkMaxRetryDuration}</b> — the SDK's in-stream retry of
 *       retriable append failures on default streams, handed to the stream writer as {@code
 *       RetrySettings} / {@code setMaxRetryDuration}.
 *   <li><b>{@code maxInflight*} and {@code *ConnectionsPerRegion}</b> — the SDK's connection pool.
 * </ul>
 *
 * <p><b>The SDK connection pool is JVM-static per (location, credentials).</b> The first stream
 * writer built for a pool key bakes its in-flight limits, SDK retry settings and {@code
 * sdkMaxRetryDuration} into that pool permanently; later writers' values are silently dropped by
 * the SDK. All writers of one sink carry the same options, so within a job the pool is consistent —
 * but on a session cluster, or with another BigQuery Storage Write API client in the same JVM,
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

    /** Default for {@link Builder#retryInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_RETRY_INITIAL_BACKOFF = Duration.ofMillis(500);

    /** Default for {@link Builder#retryMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_RETRY_MAX_BACKOFF = Duration.ofSeconds(10);

    /** Default for {@link Builder#retryMaxAttempts(int)}. */
    public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 10;

    /** Default for {@link Builder#sdkRetryInitialDelay(Duration)}. */
    public static final Duration DEFAULT_SDK_RETRY_INITIAL_DELAY = Duration.ofMillis(500);

    /** Default for {@link Builder#sdkRetryDelayMultiplier(double)}. */
    public static final double DEFAULT_SDK_RETRY_DELAY_MULTIPLIER = 2.0;

    /** Default for {@link Builder#sdkRetryMaxDelay(Duration)}. */
    public static final Duration DEFAULT_SDK_RETRY_MAX_DELAY = Duration.ofSeconds(30);

    /** Default for {@link Builder#sdkRetryMaxAttempts(int)}. */
    public static final int DEFAULT_SDK_RETRY_MAX_ATTEMPTS = 5;

    /** Default for {@link Builder#sdkMaxRetryDuration(Duration)}: the SDK's own default. */
    public static final Duration DEFAULT_SDK_MAX_RETRY_DURATION = Duration.ofMinutes(5);

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

    private final long maxAppendRequestBytes;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final int retryMaxAttempts;
    private final Duration sdkRetryInitialDelay;
    private final double sdkRetryDelayMultiplier;
    private final Duration sdkRetryMaxDelay;
    private final int sdkRetryMaxAttempts;
    private final Duration sdkMaxRetryDuration;
    private final int maxInflightRequests;
    private final long maxInflightBytes;
    private final int minConnectionsPerRegion;
    private final int maxConnectionsPerRegion;

    private DefaultStreamOptions(Builder builder) {
        this.maxAppendRequestBytes = builder.maxAppendRequestBytes;
        this.retryInitialBackoff = builder.retryInitialBackoff;
        this.retryMaxBackoff = builder.retryMaxBackoff;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.sdkRetryInitialDelay = builder.sdkRetryInitialDelay;
        this.sdkRetryDelayMultiplier = builder.sdkRetryDelayMultiplier;
        this.sdkRetryMaxDelay = builder.sdkRetryMaxDelay;
        this.sdkRetryMaxAttempts = builder.sdkRetryMaxAttempts;
        this.sdkMaxRetryDuration = builder.sdkMaxRetryDuration;
        this.maxInflightRequests = builder.maxInflightRequests;
        this.maxInflightBytes = builder.maxInflightBytes;
        this.minConnectionsPerRegion = builder.minConnectionsPerRegion;
        this.maxConnectionsPerRegion = builder.maxConnectionsPerRegion;
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

    /** Returns the first delay of the SDK's in-stream retry schedule. */
    public Duration getSdkRetryInitialDelay() {
        return sdkRetryInitialDelay;
    }

    /** Returns the delay multiplier of the SDK's in-stream retry schedule. */
    public double getSdkRetryDelayMultiplier() {
        return sdkRetryDelayMultiplier;
    }

    /** Returns the delay cap of the SDK's in-stream retry schedule. */
    public Duration getSdkRetryMaxDelay() {
        return sdkRetryMaxDelay;
    }

    /** Returns the maximum number of attempts of the SDK's in-stream retry schedule. */
    public int getSdkRetryMaxAttempts() {
        return sdkRetryMaxAttempts;
    }

    /** Returns the SDK's overall ceiling on retrying one retriable in-stream failure. */
    public Duration getSdkMaxRetryDuration() {
        return sdkMaxRetryDuration;
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
                && retryMaxAttempts == that.retryMaxAttempts
                && Double.compare(sdkRetryDelayMultiplier, that.sdkRetryDelayMultiplier) == 0
                && sdkRetryMaxAttempts == that.sdkRetryMaxAttempts
                && maxInflightRequests == that.maxInflightRequests
                && maxInflightBytes == that.maxInflightBytes
                && minConnectionsPerRegion == that.minConnectionsPerRegion
                && maxConnectionsPerRegion == that.maxConnectionsPerRegion
                && retryInitialBackoff.equals(that.retryInitialBackoff)
                && retryMaxBackoff.equals(that.retryMaxBackoff)
                && sdkRetryInitialDelay.equals(that.sdkRetryInitialDelay)
                && sdkRetryMaxDelay.equals(that.sdkRetryMaxDelay)
                && sdkMaxRetryDuration.equals(that.sdkMaxRetryDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxAppendRequestBytes,
                retryInitialBackoff,
                retryMaxBackoff,
                retryMaxAttempts,
                sdkRetryInitialDelay,
                sdkRetryDelayMultiplier,
                sdkRetryMaxDelay,
                sdkRetryMaxAttempts,
                sdkMaxRetryDuration,
                maxInflightRequests,
                maxInflightBytes,
                minConnectionsPerRegion,
                maxConnectionsPerRegion);
    }

    @Override
    public String toString() {
        return "DefaultStreamOptions{maxAppendRequestBytes="
                + maxAppendRequestBytes
                + ", retryInitialBackoff="
                + retryInitialBackoff
                + ", retryMaxBackoff="
                + retryMaxBackoff
                + ", retryMaxAttempts="
                + retryMaxAttempts
                + ", sdkRetryInitialDelay="
                + sdkRetryInitialDelay
                + ", sdkRetryDelayMultiplier="
                + sdkRetryDelayMultiplier
                + ", sdkRetryMaxDelay="
                + sdkRetryMaxDelay
                + ", sdkRetryMaxAttempts="
                + sdkRetryMaxAttempts
                + ", sdkMaxRetryDuration="
                + sdkMaxRetryDuration
                + ", maxInflightRequests="
                + maxInflightRequests
                + ", maxInflightBytes="
                + maxInflightBytes
                + ", minConnectionsPerRegion="
                + minConnectionsPerRegion
                + ", maxConnectionsPerRegion="
                + maxConnectionsPerRegion
                + "}";
    }

    /** Builder for {@link DefaultStreamOptions}. */
    @PublicEvolving
    public static final class Builder {

        private long maxAppendRequestBytes = DEFAULT_MAX_APPEND_REQUEST_BYTES;
        private Duration retryInitialBackoff = DEFAULT_RETRY_INITIAL_BACKOFF;
        private Duration retryMaxBackoff = DEFAULT_RETRY_MAX_BACKOFF;
        private int retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
        private Duration sdkRetryInitialDelay = DEFAULT_SDK_RETRY_INITIAL_DELAY;
        private double sdkRetryDelayMultiplier = DEFAULT_SDK_RETRY_DELAY_MULTIPLIER;
        private Duration sdkRetryMaxDelay = DEFAULT_SDK_RETRY_MAX_DELAY;
        private int sdkRetryMaxAttempts = DEFAULT_SDK_RETRY_MAX_ATTEMPTS;
        private Duration sdkMaxRetryDuration = DEFAULT_SDK_MAX_RETRY_DURATION;
        private int maxInflightRequests = DEFAULT_MAX_INFLIGHT_REQUESTS;
        private long maxInflightBytes = DEFAULT_MAX_INFLIGHT_BYTES;
        private int minConnectionsPerRegion = DEFAULT_MIN_CONNECTIONS_PER_REGION;
        private int maxConnectionsPerRegion = DEFAULT_MAX_CONNECTIONS_PER_REGION;

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
         * Sets the first backoff of the connector-driven retry schedule (append recovery after
         * table auto-creation, transient append failures past the SDK's own retries, stale-writer
         * refreshes). Defaults to {@link #DEFAULT_RETRY_INITIAL_BACKOFF}.
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
         * Sets the first delay of the SDK's in-stream retry of retriable append failures (for
         * example {@code ABORTED}, {@code UNAVAILABLE} and quota {@code RESOURCE_EXHAUSTED}).
         * Defaults to {@link #DEFAULT_SDK_RETRY_INITIAL_DELAY}.
         *
         * <p>The connection pool adopts the first writer's SDK retry settings per JVM; see the
         * class javadoc.
         *
         * @param sdkRetryInitialDelay the first retry delay
         * @return this builder
         */
        public Builder sdkRetryInitialDelay(Duration sdkRetryInitialDelay) {
            Preconditions.checkNotNull(
                    sdkRetryInitialDelay, "sdkRetryInitialDelay must not be null");
            Preconditions.checkArgument(
                    !sdkRetryInitialDelay.isNegative() && !sdkRetryInitialDelay.isZero(),
                    "sdkRetryInitialDelay must be positive: %s",
                    sdkRetryInitialDelay);
            this.sdkRetryInitialDelay = sdkRetryInitialDelay;
            return this;
        }

        /**
         * Sets the delay multiplier of the SDK's in-stream retry schedule. Defaults to {@link
         * #DEFAULT_SDK_RETRY_DELAY_MULTIPLIER}.
         *
         * @param sdkRetryDelayMultiplier the multiplier, at least 1.0
         * @return this builder
         */
        public Builder sdkRetryDelayMultiplier(double sdkRetryDelayMultiplier) {
            Preconditions.checkArgument(
                    sdkRetryDelayMultiplier >= 1.0,
                    "sdkRetryDelayMultiplier must be at least 1.0: %s",
                    sdkRetryDelayMultiplier);
            this.sdkRetryDelayMultiplier = sdkRetryDelayMultiplier;
            return this;
        }

        /**
         * Sets the delay cap of the SDK's in-stream retry schedule. Must be at least the initial
         * delay. Defaults to {@link #DEFAULT_SDK_RETRY_MAX_DELAY}.
         *
         * @param sdkRetryMaxDelay the delay cap
         * @return this builder
         */
        public Builder sdkRetryMaxDelay(Duration sdkRetryMaxDelay) {
            Preconditions.checkNotNull(sdkRetryMaxDelay, "sdkRetryMaxDelay must not be null");
            Preconditions.checkArgument(
                    !sdkRetryMaxDelay.isNegative() && !sdkRetryMaxDelay.isZero(),
                    "sdkRetryMaxDelay must be positive: %s",
                    sdkRetryMaxDelay);
            this.sdkRetryMaxDelay = sdkRetryMaxDelay;
            return this;
        }

        /**
         * Sets the maximum number of attempts of the SDK's in-stream retry schedule. Defaults to
         * {@link #DEFAULT_SDK_RETRY_MAX_ATTEMPTS}.
         *
         * @param sdkRetryMaxAttempts the attempt cap
         * @return this builder
         */
        public Builder sdkRetryMaxAttempts(int sdkRetryMaxAttempts) {
            Preconditions.checkArgument(
                    sdkRetryMaxAttempts > 0,
                    "sdkRetryMaxAttempts must be positive: %s",
                    sdkRetryMaxAttempts);
            this.sdkRetryMaxAttempts = sdkRetryMaxAttempts;
            return this;
        }

        /**
         * Sets the SDK's overall ceiling on retrying one retriable in-stream failure, across all
         * attempts. Defaults to {@link #DEFAULT_SDK_MAX_RETRY_DURATION}, the SDK's own default.
         *
         * @param sdkMaxRetryDuration the overall retry ceiling
         * @return this builder
         */
        public Builder sdkMaxRetryDuration(Duration sdkMaxRetryDuration) {
            Preconditions.checkNotNull(sdkMaxRetryDuration, "sdkMaxRetryDuration must not be null");
            Preconditions.checkArgument(
                    !sdkMaxRetryDuration.isNegative() && !sdkMaxRetryDuration.isZero(),
                    "sdkMaxRetryDuration must be positive: %s",
                    sdkMaxRetryDuration);
            this.sdkMaxRetryDuration = sdkMaxRetryDuration;
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
         * Builds the options.
         *
         * @return the options
         */
        public DefaultStreamOptions build() {
            Preconditions.checkState(
                    retryMaxBackoff.compareTo(retryInitialBackoff) >= 0,
                    "retryMaxBackoff must be >= retryInitialBackoff: %s < %s",
                    retryMaxBackoff,
                    retryInitialBackoff);
            Preconditions.checkState(
                    sdkRetryMaxDelay.compareTo(sdkRetryInitialDelay) >= 0,
                    "sdkRetryMaxDelay must be >= sdkRetryInitialDelay: %s < %s",
                    sdkRetryMaxDelay,
                    sdkRetryInitialDelay);
            Preconditions.checkState(
                    maxConnectionsPerRegion >= minConnectionsPerRegion,
                    "maxConnectionsPerRegion must be >= minConnectionsPerRegion: %s < %s",
                    maxConnectionsPerRegion,
                    minConnectionsPerRegion);
            return new DefaultStreamOptions(this);
        }
    }
}
