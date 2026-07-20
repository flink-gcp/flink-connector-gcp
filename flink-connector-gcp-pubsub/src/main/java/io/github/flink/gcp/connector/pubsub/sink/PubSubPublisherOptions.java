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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the sink's Pub/Sub publishers and its writer: SDK batching, flow control and
 * publish-retry settings, message ordering, the writer's in-flight cap, and the backoff budget of
 * the topic auto-creation recovery.
 *
 * <p>Set via {@link PubSubSinkBuilder#publisherOptions(PubSubPublisherOptions)}; optional — every
 * knob left unset keeps the SDK's (or the sink's) default behavior, so {@link #defaults()} is
 * equivalent to not setting options at all.
 *
 * <p>Flow-control limits use the SDK's {@code LimitExceededBehavior.Block}: a publish exceeding a
 * limit blocks the task thread until in-flight publishes complete (permits are released on SDK
 * threads, so this is plain backpressure with no deadlock). The behavior itself is deliberately not
 * exposed: failing the job on transient pressure ({@code ThrowException}) or configuring limits
 * that are not enforced ({@code Ignore}) are not useful sink behaviors. Note the writer's own
 * {@link Builder#maxInFlightMessages(int)} cap is the mailbox-friendly primary bound; a
 * flow-control element limit above it never triggers. Avoid combining flow-control limits with
 * {@link Builder#enableMessageOrdering(boolean)}: the SDK publisher (1.152.0) leaks a flow-control
 * permit for every publish rejected or cancelled on a paused ordering key, which under {@code
 * Block} can eventually hang publishing (see the module README).
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class PubSubPublisherOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final PubSubPublisherOptions DEFAULTS = builder().build();

    @Nullable private final Long batchElementCountThreshold;
    @Nullable private final Long batchRequestByteThreshold;
    @Nullable private final Duration batchDelayThreshold;
    @Nullable private final Long flowControlMaxOutstandingElementCount;
    @Nullable private final Long flowControlMaxOutstandingRequestBytes;
    @Nullable private final Duration retryTotalTimeout;
    @Nullable private final Duration retryInitialDelay;
    @Nullable private final Double retryDelayMultiplier;
    @Nullable private final Duration retryMaxDelay;
    @Nullable private final Duration retryInitialRpcTimeout;
    @Nullable private final Double retryRpcTimeoutMultiplier;
    @Nullable private final Duration retryMaxRpcTimeout;
    @Nullable private final Integer retryMaxAttempts;
    private final boolean enableMessageOrdering;
    private final int maxInFlightMessages;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;

    private PubSubPublisherOptions(Builder builder) {
        this.batchElementCountThreshold = builder.batchElementCountThreshold;
        this.batchRequestByteThreshold = builder.batchRequestByteThreshold;
        this.batchDelayThreshold = builder.batchDelayThreshold;
        this.flowControlMaxOutstandingElementCount = builder.flowControlMaxOutstandingElementCount;
        this.flowControlMaxOutstandingRequestBytes = builder.flowControlMaxOutstandingRequestBytes;
        this.retryTotalTimeout = builder.retryTotalTimeout;
        this.retryInitialDelay = builder.retryInitialDelay;
        this.retryDelayMultiplier = builder.retryDelayMultiplier;
        this.retryMaxDelay = builder.retryMaxDelay;
        this.retryInitialRpcTimeout = builder.retryInitialRpcTimeout;
        this.retryRpcTimeoutMultiplier = builder.retryRpcTimeoutMultiplier;
        this.retryMaxRpcTimeout = builder.retryMaxRpcTimeout;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.enableMessageOrdering = builder.enableMessageOrdering;
        this.maxInFlightMessages = builder.maxInFlightMessages;
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
     * Returns the default options: SDK-default batching, flow control and retries, ordering
     * disabled, an in-flight cap of 1000, and a topic auto-creation recovery budget of 500 ms
     * doubling to 10 s over 10 attempts.
     *
     * @return the default options
     */
    public static PubSubPublisherOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the batch element-count threshold, or {@code null} for the SDK default. */
    @Nullable
    public Long getBatchElementCountThreshold() {
        return batchElementCountThreshold;
    }

    /** Returns the batch request-byte threshold, or {@code null} for the SDK default. */
    @Nullable
    public Long getBatchRequestByteThreshold() {
        return batchRequestByteThreshold;
    }

    /** Returns the batch delay threshold, or {@code null} for the SDK default. */
    @Nullable
    public Duration getBatchDelayThreshold() {
        return batchDelayThreshold;
    }

    /** Returns the flow-control outstanding-element limit, or {@code null} for no limit. */
    @Nullable
    public Long getFlowControlMaxOutstandingElementCount() {
        return flowControlMaxOutstandingElementCount;
    }

    /** Returns the flow-control outstanding-byte limit, or {@code null} for no limit. */
    @Nullable
    public Long getFlowControlMaxOutstandingRequestBytes() {
        return flowControlMaxOutstandingRequestBytes;
    }

    /** Returns the publish-retry total timeout, or {@code null} for the SDK default. */
    @Nullable
    public Duration getRetryTotalTimeout() {
        return retryTotalTimeout;
    }

    /** Returns the initial publish-retry delay, or {@code null} for the SDK default. */
    @Nullable
    public Duration getRetryInitialDelay() {
        return retryInitialDelay;
    }

    /** Returns the publish-retry delay multiplier, or {@code null} for the SDK default. */
    @Nullable
    public Double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    /** Returns the maximum publish-retry delay, or {@code null} for the SDK default. */
    @Nullable
    public Duration getRetryMaxDelay() {
        return retryMaxDelay;
    }

    /** Returns the initial per-RPC timeout, or {@code null} for the SDK default. */
    @Nullable
    public Duration getRetryInitialRpcTimeout() {
        return retryInitialRpcTimeout;
    }

    /** Returns the per-RPC timeout multiplier, or {@code null} for the SDK default. */
    @Nullable
    public Double getRetryRpcTimeoutMultiplier() {
        return retryRpcTimeoutMultiplier;
    }

    /** Returns the maximum per-RPC timeout, or {@code null} for the SDK default. */
    @Nullable
    public Duration getRetryMaxRpcTimeout() {
        return retryMaxRpcTimeout;
    }

    /**
     * Returns the maximum publish attempts, or {@code null} for the SDK default (bounded only by
     * the total timeout).
     */
    @Nullable
    public Integer getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    /** Returns whether publishers honor message ordering keys. */
    public boolean isEnableMessageOrdering() {
        return enableMessageOrdering;
    }

    /** Returns the writer's cap on unacknowledged publishes. */
    public int getMaxInFlightMessages() {
        return maxInFlightMessages;
    }

    /** Returns the first backoff of the topic auto-creation recovery. */
    public Duration getRecoveryInitialBackoff() {
        return recoveryInitialBackoff;
    }

    /** Returns the backoff cap of the topic auto-creation recovery. */
    public Duration getRecoveryMaxBackoff() {
        return recoveryMaxBackoff;
    }

    /** Returns the maximum republish attempts of the topic auto-creation recovery. */
    public int getRecoveryMaxAttempts() {
        return recoveryMaxAttempts;
    }

    /** Returns whether any batching or flow-control knob deviates from the SDK default. */
    public boolean hasBatchingOverrides() {
        return batchElementCountThreshold != null
                || batchRequestByteThreshold != null
                || batchDelayThreshold != null
                || flowControlMaxOutstandingElementCount != null
                || flowControlMaxOutstandingRequestBytes != null;
    }

    /** Returns whether any publish-retry knob deviates from the SDK default. */
    public boolean hasRetryOverrides() {
        return retryTotalTimeout != null
                || retryInitialDelay != null
                || retryDelayMultiplier != null
                || retryMaxDelay != null
                || retryInitialRpcTimeout != null
                || retryRpcTimeoutMultiplier != null
                || retryMaxRpcTimeout != null
                || retryMaxAttempts != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubPublisherOptions that = (PubSubPublisherOptions) o;
        return enableMessageOrdering == that.enableMessageOrdering
                && maxInFlightMessages == that.maxInFlightMessages
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && Objects.equals(batchElementCountThreshold, that.batchElementCountThreshold)
                && Objects.equals(batchRequestByteThreshold, that.batchRequestByteThreshold)
                && Objects.equals(batchDelayThreshold, that.batchDelayThreshold)
                && Objects.equals(
                        flowControlMaxOutstandingElementCount,
                        that.flowControlMaxOutstandingElementCount)
                && Objects.equals(
                        flowControlMaxOutstandingRequestBytes,
                        that.flowControlMaxOutstandingRequestBytes)
                && Objects.equals(retryTotalTimeout, that.retryTotalTimeout)
                && Objects.equals(retryInitialDelay, that.retryInitialDelay)
                && Objects.equals(retryDelayMultiplier, that.retryDelayMultiplier)
                && Objects.equals(retryMaxDelay, that.retryMaxDelay)
                && Objects.equals(retryInitialRpcTimeout, that.retryInitialRpcTimeout)
                && Objects.equals(retryRpcTimeoutMultiplier, that.retryRpcTimeoutMultiplier)
                && Objects.equals(retryMaxRpcTimeout, that.retryMaxRpcTimeout)
                && Objects.equals(retryMaxAttempts, that.retryMaxAttempts)
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchElementCountThreshold,
                batchRequestByteThreshold,
                batchDelayThreshold,
                flowControlMaxOutstandingElementCount,
                flowControlMaxOutstandingRequestBytes,
                retryTotalTimeout,
                retryInitialDelay,
                retryDelayMultiplier,
                retryMaxDelay,
                retryInitialRpcTimeout,
                retryRpcTimeoutMultiplier,
                retryMaxRpcTimeout,
                retryMaxAttempts,
                enableMessageOrdering,
                maxInFlightMessages,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts);
    }

    @Override
    public String toString() {
        return "PubSubPublisherOptions{batchElementCountThreshold="
                + batchElementCountThreshold
                + ", batchRequestByteThreshold="
                + batchRequestByteThreshold
                + ", batchDelayThreshold="
                + batchDelayThreshold
                + ", flowControlMaxOutstandingElementCount="
                + flowControlMaxOutstandingElementCount
                + ", flowControlMaxOutstandingRequestBytes="
                + flowControlMaxOutstandingRequestBytes
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
                + ", enableMessageOrdering="
                + enableMessageOrdering
                + ", maxInFlightMessages="
                + maxInFlightMessages
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + "}";
    }

    /** Builder for {@link PubSubPublisherOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Long batchElementCountThreshold;
        @Nullable private Long batchRequestByteThreshold;
        @Nullable private Duration batchDelayThreshold;
        @Nullable private Long flowControlMaxOutstandingElementCount;
        @Nullable private Long flowControlMaxOutstandingRequestBytes;
        @Nullable private Duration retryTotalTimeout;
        @Nullable private Duration retryInitialDelay;
        @Nullable private Double retryDelayMultiplier;
        @Nullable private Duration retryMaxDelay;
        @Nullable private Duration retryInitialRpcTimeout;
        @Nullable private Double retryRpcTimeoutMultiplier;
        @Nullable private Duration retryMaxRpcTimeout;
        @Nullable private Integer retryMaxAttempts;
        private boolean enableMessageOrdering;
        private int maxInFlightMessages = 1000;
        private Duration recoveryInitialBackoff = Duration.ofMillis(500);
        private Duration recoveryMaxBackoff = Duration.ofSeconds(10);
        private int recoveryMaxAttempts = 10;

        private Builder() {}

        /**
         * Sets how many messages a publisher batches into one publish request. Optional; defaults
         * to the SDK's threshold.
         *
         * @param batchElementCountThreshold the element-count threshold, positive
         * @return this builder
         */
        public Builder batchElementCountThreshold(long batchElementCountThreshold) {
            Preconditions.checkArgument(
                    batchElementCountThreshold > 0, "batchElementCountThreshold must be positive");
            this.batchElementCountThreshold = batchElementCountThreshold;
            return this;
        }

        /**
         * Sets how many bytes a publisher batches into one publish request. Optional; defaults to
         * the SDK's threshold.
         *
         * @param batchRequestByteThreshold the request-byte threshold, positive
         * @return this builder
         */
        public Builder batchRequestByteThreshold(long batchRequestByteThreshold) {
            Preconditions.checkArgument(
                    batchRequestByteThreshold > 0, "batchRequestByteThreshold must be positive");
            this.batchRequestByteThreshold = batchRequestByteThreshold;
            return this;
        }

        /**
         * Sets how long a publisher waits for a batch to fill before sending it. Optional; defaults
         * to the SDK's threshold.
         *
         * @param batchDelayThreshold the delay threshold, positive
         * @return this builder
         */
        public Builder batchDelayThreshold(Duration batchDelayThreshold) {
            this.batchDelayThreshold = checkPositive(batchDelayThreshold, "batchDelayThreshold");
            return this;
        }

        /**
         * Caps the messages a publisher may hold unacknowledged; a publish beyond the cap blocks
         * the task thread until in-flight publishes complete. Optional; defaults to no limit. Note
         * the writer's own {@link #maxInFlightMessages(int)} is the primary cap — a higher
         * flow-control limit never triggers.
         *
         * @param flowControlMaxOutstandingElementCount the outstanding-element limit, positive
         * @return this builder
         */
        public Builder flowControlMaxOutstandingElementCount(
                long flowControlMaxOutstandingElementCount) {
            Preconditions.checkArgument(
                    flowControlMaxOutstandingElementCount > 0,
                    "flowControlMaxOutstandingElementCount must be positive");
            this.flowControlMaxOutstandingElementCount = flowControlMaxOutstandingElementCount;
            return this;
        }

        /**
         * Caps the bytes a publisher may hold unacknowledged; a publish beyond the cap blocks the
         * task thread until in-flight publishes complete. Optional; defaults to no limit. This is
         * the byte-level bound the writer's element-count cap cannot provide.
         *
         * @param flowControlMaxOutstandingRequestBytes the outstanding-byte limit, positive
         * @return this builder
         */
        public Builder flowControlMaxOutstandingRequestBytes(
                long flowControlMaxOutstandingRequestBytes) {
            Preconditions.checkArgument(
                    flowControlMaxOutstandingRequestBytes > 0,
                    "flowControlMaxOutstandingRequestBytes must be positive");
            this.flowControlMaxOutstandingRequestBytes = flowControlMaxOutstandingRequestBytes;
            return this;
        }

        /**
         * Sets the total time budget of a publish including its retries. Optional; defaults to the
         * SDK's timeout.
         *
         * @param retryTotalTimeout the total timeout, positive
         * @return this builder
         */
        public Builder retryTotalTimeout(Duration retryTotalTimeout) {
            this.retryTotalTimeout = checkPositive(retryTotalTimeout, "retryTotalTimeout");
            return this;
        }

        /**
         * Sets the delay before the first publish retry. Optional; defaults to the SDK's delay.
         *
         * @param retryInitialDelay the initial retry delay, positive
         * @return this builder
         */
        public Builder retryInitialDelay(Duration retryInitialDelay) {
            this.retryInitialDelay = checkPositive(retryInitialDelay, "retryInitialDelay");
            return this;
        }

        /**
         * Sets the factor the retry delay grows by per attempt. Optional; defaults to the SDK's
         * multiplier.
         *
         * @param retryDelayMultiplier the delay multiplier, at least 1.0
         * @return this builder
         */
        public Builder retryDelayMultiplier(double retryDelayMultiplier) {
            Preconditions.checkArgument(
                    retryDelayMultiplier >= 1.0, "retryDelayMultiplier must be at least 1.0");
            this.retryDelayMultiplier = retryDelayMultiplier;
            return this;
        }

        /**
         * Caps the delay between publish retries. Optional; defaults to the SDK's cap.
         *
         * @param retryMaxDelay the maximum retry delay, positive
         * @return this builder
         */
        public Builder retryMaxDelay(Duration retryMaxDelay) {
            this.retryMaxDelay = checkPositive(retryMaxDelay, "retryMaxDelay");
            return this;
        }

        /**
         * Sets the timeout of the first publish RPC attempt. Optional; defaults to the SDK's
         * timeout.
         *
         * @param retryInitialRpcTimeout the initial per-RPC timeout, positive
         * @return this builder
         */
        public Builder retryInitialRpcTimeout(Duration retryInitialRpcTimeout) {
            this.retryInitialRpcTimeout =
                    checkPositive(retryInitialRpcTimeout, "retryInitialRpcTimeout");
            return this;
        }

        /**
         * Sets the factor the per-RPC timeout grows by per attempt. Optional; defaults to the SDK's
         * multiplier.
         *
         * @param retryRpcTimeoutMultiplier the timeout multiplier, at least 1.0
         * @return this builder
         */
        public Builder retryRpcTimeoutMultiplier(double retryRpcTimeoutMultiplier) {
            Preconditions.checkArgument(
                    retryRpcTimeoutMultiplier >= 1.0,
                    "retryRpcTimeoutMultiplier must be at least 1.0");
            this.retryRpcTimeoutMultiplier = retryRpcTimeoutMultiplier;
            return this;
        }

        /**
         * Caps the timeout of a publish RPC attempt. Optional; defaults to the SDK's cap.
         *
         * @param retryMaxRpcTimeout the maximum per-RPC timeout, positive
         * @return this builder
         */
        public Builder retryMaxRpcTimeout(Duration retryMaxRpcTimeout) {
            this.retryMaxRpcTimeout = checkPositive(retryMaxRpcTimeout, "retryMaxRpcTimeout");
            return this;
        }

        /**
         * Caps the publish attempts. Optional; defaults to the SDK's behavior of bounding retries
         * only by the total timeout ({@code 0} means the same).
         *
         * @param retryMaxAttempts the maximum attempts, non-negative
         * @return this builder
         */
        public Builder retryMaxAttempts(int retryMaxAttempts) {
            Preconditions.checkArgument(
                    retryMaxAttempts >= 0, "retryMaxAttempts must not be negative");
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        /**
         * Sets whether publishers honor message ordering keys. Defaults to {@code false}; the
         * writer rejects messages carrying an ordering key while this is disabled.
         *
         * @param enableMessageOrdering whether to enable message ordering
         * @return this builder
         */
        public Builder enableMessageOrdering(boolean enableMessageOrdering) {
            this.enableMessageOrdering = enableMessageOrdering;
            return this;
        }

        /**
         * Caps the writer's unacknowledged publishes; a write at the cap yields to the task mailbox
         * until completions bring the count back down. Defaults to 1000.
         *
         * @param maxInFlightMessages the in-flight cap, positive
         * @return this builder
         */
        public Builder maxInFlightMessages(int maxInFlightMessages) {
            Preconditions.checkArgument(
                    maxInFlightMessages > 0, "maxInFlightMessages must be positive");
            this.maxInFlightMessages = maxInFlightMessages;
            return this;
        }

        /**
         * Sets the first backoff of the topic auto-creation recovery (republishing after creating a
         * missing topic). Defaults to 500 ms.
         *
         * @param recoveryInitialBackoff the first backoff, positive
         * @return this builder
         */
        public Builder recoveryInitialBackoff(Duration recoveryInitialBackoff) {
            this.recoveryInitialBackoff =
                    checkPositive(recoveryInitialBackoff, "recoveryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the topic auto-creation recovery. Defaults to 10 s.
         *
         * @param recoveryMaxBackoff the backoff cap, positive and at least the initial backoff
         * @return this builder
         */
        public Builder recoveryMaxBackoff(Duration recoveryMaxBackoff) {
            this.recoveryMaxBackoff = checkPositive(recoveryMaxBackoff, "recoveryMaxBackoff");
            return this;
        }

        /**
         * Caps the republish attempts of the topic auto-creation recovery. Defaults to 10.
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
        public PubSubPublisherOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be at least recoveryInitialBackoff.");
            return new PubSubPublisherOptions(this);
        }

        private static Duration checkPositive(Duration duration, String name) {
            Preconditions.checkNotNull(duration, "%s must not be null", name);
            Preconditions.checkArgument(
                    !duration.isZero() && !duration.isNegative(), "%s must be positive", name);
            return duration;
        }
    }
}
