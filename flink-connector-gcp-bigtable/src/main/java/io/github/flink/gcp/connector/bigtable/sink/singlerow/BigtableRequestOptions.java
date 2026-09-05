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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the single-row request runtime: the deadline of each request, how many may be
 * outstanding at once, and how the runtime holds instance clients.
 *
 * <p>Every knob is defaulted, so {@code builder().build()} is equivalent to setting nothing. There
 * is no shared default instance to reach for: build one where it is needed.
 *
 * <h2>Why these are not {@code BigtableWriterOptions}</h2>
 *
 * <p>The batching sink's options are batch thresholds and in-flight byte bounds, and a single-row
 * request has neither: it is one RPC for one row whose answer is a value, so what it needs is a
 * deadline and a count. Sharing the type would leave most of its setters inert here.
 *
 * <h2>Why there are no retry knobs</h2>
 *
 * <p>{@code CheckAndMutateRow} and {@code ReadModifyWriteRow} are not idempotent, and the client
 * itself ships them with an empty retryable-code set. A retry of an ambiguous failure could apply
 * an increment twice, so the runtime adds no retry loop and exposes nothing to tune one. {@link
 * Builder#requestTimeout(Duration)} is the single attempt's whole deadline.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class BigtableRequestOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Default for {@link Builder#maxInFlightRequests(int)}: 100, the same number as Flink's default
     * async-operator capacity, so the sink surface and the async surface share one bound unless
     * told otherwise.
     */
    public static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 100;

    /**
     * Default for {@link Builder#requestTimeout(Duration)}: 20 seconds, the client's own total
     * timeout for both RPCs, pinned by a test so a client upgrade moving it fails a build rather
     * than changing the meaning of the default.
     */
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(20);

    /**
     * Default for {@link Builder#destinationIdleTimeout(Duration)}: one hour, as the batching
     * sink's. Eviction is memory hygiene for long-lived jobs with per-record destinations.
     */
    public static final Duration DEFAULT_DESTINATION_IDLE_TIMEOUT = Duration.ofHours(1);

    /** Default maximum number of open-or-closing instance clients held by one subtask. */
    public static final int DEFAULT_MAX_ACTIVE_INSTANCES = 16;

    private final int maxInFlightRequests;
    private final Duration requestTimeout;
    private final Duration destinationIdleTimeout;
    private final int maxActiveInstances;
    private final boolean perDestinationMetrics;

    private BigtableRequestOptions(Builder builder) {
        this.maxInFlightRequests = builder.maxInFlightRequests;
        this.requestTimeout = builder.requestTimeout;
        this.destinationIdleTimeout = builder.destinationIdleTimeout;
        this.maxActiveInstances = builder.maxActiveInstances;
        this.perDestinationMetrics = builder.perDestinationMetrics;
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
     * Returns the cap on requests the sink surface keeps outstanding.
     *
     * @return the in-flight cap
     */
    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    /**
     * Returns the deadline of one request, applied to the client as its whole single attempt.
     *
     * @return the request timeout
     */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Returns how long a table may go without requests before the runtime drops its state.
     *
     * @return the idle timeout
     */
    public Duration getDestinationIdleTimeout() {
        return destinationIdleTimeout;
    }

    /**
     * Returns the maximum number of Bigtable instance clients held by one subtask.
     *
     * @return the instance-client cap
     */
    public int getMaxActiveInstances() {
        return maxActiveInstances;
    }

    /**
     * Returns whether per-table counters are registered beside the runtime's totals.
     *
     * @return whether per-destination counters are registered
     */
    public boolean isPerDestinationMetrics() {
        return perDestinationMetrics;
    }

    /**
     * Compares every option by value.
     *
     * @param o the object to compare with
     * @return whether every option is equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableRequestOptions that = (BigtableRequestOptions) o;
        return maxInFlightRequests == that.maxInFlightRequests
                && maxActiveInstances == that.maxActiveInstances
                && perDestinationMetrics == that.perDestinationMetrics
                && requestTimeout.equals(that.requestTimeout)
                && destinationIdleTimeout.equals(that.destinationIdleTimeout);
    }

    /**
     * Hashes every option.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                maxInFlightRequests,
                requestTimeout,
                destinationIdleTimeout,
                maxActiveInstances,
                perDestinationMetrics);
    }

    /**
     * Renders every option; nothing here is user data.
     *
     * @return the rendering
     */
    @Override
    public String toString() {
        return "BigtableRequestOptions{maxInFlightRequests="
                + maxInFlightRequests
                + ", requestTimeout="
                + requestTimeout
                + ", destinationIdleTimeout="
                + destinationIdleTimeout
                + ", maxActiveInstances="
                + maxActiveInstances
                + ", perDestinationMetrics="
                + perDestinationMetrics
                + "}";
    }

    /** Builder for {@link BigtableRequestOptions}. */
    @PublicEvolving
    public static final class Builder {

        private int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        private Duration destinationIdleTimeout = DEFAULT_DESTINATION_IDLE_TIMEOUT;
        private int maxActiveInstances = DEFAULT_MAX_ACTIVE_INSTANCES;
        private boolean perDestinationMetrics;

        private Builder() {}

        /**
         * Caps the requests the sink surface keeps outstanding — accepted by the client and not yet
         * answered. A write at the cap yields to the task mailbox until completions bring the count
         * down. Defaults to {@value #DEFAULT_MAX_IN_FLIGHT_REQUESTS}.
         *
         * <p>The conditional async helper passes this value to {@code AsyncDataStream} as its
         * operator capacity. Flink enforces that bound; the async request function has no separate
         * admission gate.
         *
         * @param maxInFlightRequests the in-flight cap, positive
         * @return this builder
         */
        public Builder maxInFlightRequests(int maxInFlightRequests) {
            Preconditions.checkArgument(
                    maxInFlightRequests > 0, "maxInFlightRequests must be positive");
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        /**
         * Sets the deadline of one request. The runtime applies it to the client as the whole of a
         * single attempt: no retries, and the timeout is the total. A request past it fails with
         * {@code DEADLINE_EXCEEDED}, which the runtime treats as ambiguous — the service may have
         * applied it — and counts under {@code requestsTimedOut}. Defaults to {@link
         * #DEFAULT_REQUEST_TIMEOUT}.
         *
         * <p>On the async-operator surface, Flink's own operator timeout should be longer than
         * this, so that it is the client's deadline, with its Bigtable-named message, that fires
         * first.
         *
         * @param requestTimeout the deadline, at least 1 ms and at most {@code
         *     Duration.ofNanos(Long.MAX_VALUE)}
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            OptionChecks.checkAtLeastOneMilli(requestTimeout, "requestTimeout");
            // Converted to nanoseconds by the client's settings builder; the ceiling keeps that
            // conversion from throwing on a TaskManager (ADR-0068).
            this.requestTimeout =
                    OptionChecks.checkExpressibleInNanos(requestTimeout, "requestTimeout");
            return this;
        }

        /**
         * Sets how long a table may go without requests before the runtime drops its per-table
         * state. Eviction is memory hygiene for long-lived jobs with per-record destinations (for
         * example date-suffixed tables); a table that receives a request again after eviction is
         * rebuilt transparently. On the sink surface the sweep runs at the end of each successful
         * flush, when nothing is in flight; on the async surface, which has no flush, it runs as
         * records arrive, at most once per idle timeout, and skips a table with a request in
         * flight. Defaults to {@link #DEFAULT_DESTINATION_IDLE_TIMEOUT}; to never evict, set a very
         * large duration — up to {@code Duration.ofNanos(Long.MAX_VALUE)}, about 292 years.
         *
         * <p>The runtime holds one client per (project, instance), shared by that instance's
         * tables. Evicting one table releases its ownership of that client; the client closes when
         * the instance's last table is evicted.
         *
         * @param destinationIdleTimeout the idle timeout, positive and at most {@code
         *     Duration.ofNanos(Long.MAX_VALUE)}
         * @return this builder
         */
        public Builder destinationIdleTimeout(Duration destinationIdleTimeout) {
            OptionChecks.checkPositive(destinationIdleTimeout, "destinationIdleTimeout");
            // The knob's own documentation offers a very large duration as "never evict"; the
            // ceiling keeps that instruction from throwing from a constructor on a TaskManager
            // (ADR-0068).
            this.destinationIdleTimeout =
                    OptionChecks.checkExpressibleInNanos(
                            destinationIdleTimeout, "destinationIdleTimeout");
            return this;
        }

        /**
         * Caps the open-or-closing Bigtable instance clients held by one subtask. When a new
         * instance would exceed the cap, the sink surface drains outstanding requests and evicts
         * the least recently used instance; the async surface, which cannot wait, evicts an
         * instance with nothing in flight or fails the record naming this option. Client close runs
         * off the task thread but keeps its slot until physical shutdown finishes. Defaults to
         * {@value #DEFAULT_MAX_ACTIVE_INSTANCES}.
         *
         * @param maxActiveInstances the instance-client cap, positive
         * @return this builder
         */
        public Builder maxActiveInstances(int maxActiveInstances) {
            Preconditions.checkArgument(
                    maxActiveInstances > 0, "maxActiveInstances must be positive");
            this.maxActiveInstances = maxActiveInstances;
            return this;
        }

        /**
         * Registers per-table {@code recordsSend} and {@code sendErrors} counters beside the
         * runtime's totals. Defaults to {@code false}.
         *
         * <p>Off by default because Flink cannot unregister a metric: with per-record destinations
         * the table set is unbounded, so every table the job ever writes to keeps a row in the
         * metric registry for the lifetime of the task. Switch it on for a job whose tables are few
         * and known.
         *
         * @param perDestinationMetrics whether to register per-table counters
         * @return this builder
         */
        public Builder perDestinationMetrics(boolean perDestinationMetrics) {
            this.perDestinationMetrics = perDestinationMetrics;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public BigtableRequestOptions build() {
            return new BigtableRequestOptions(this);
        }
    }
}
