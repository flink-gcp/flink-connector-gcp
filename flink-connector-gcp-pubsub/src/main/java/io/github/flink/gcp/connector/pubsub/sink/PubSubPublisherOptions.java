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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tuning options for the sink's Pub/Sub publishers and its writer: SDK batching and publish-retry
 * settings, message ordering, the writer's in-flight caps, the backoff budget of the topic
 * auto-creation recovery, and the writer's shutdown budget.
 *
 * <p>Set via {@link PubSubSinkBuilder#publisherOptions(PubSubPublisherOptions)}; optional — every
 * knob left unset keeps the SDK's (or the sink's) default behavior, so {@link #defaults()} is
 * equivalent to not setting options at all.
 *
 * <p>In-flight publishes are bounded by the writer itself, along both dimensions that matter:
 * {@link Builder#maxInFlightMessages(int)} and {@link Builder#maxInFlightBytes(long)}. Both yield
 * to the task mailbox rather than blocking the task thread, and both apply with message ordering
 * enabled. The SDK publisher's own flow controller is deliberately <b>not</b> exposed; the
 * connector documentation records why.
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
    private final long maxInFlightBytes;
    private final Duration publishProgressTimeout;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;
    private final Duration shutdownTimeout;
    private final boolean perDestinationMetrics;

    private PubSubPublisherOptions(Builder builder) {
        this.batchElementCountThreshold = builder.batchElementCountThreshold;
        this.batchRequestByteThreshold = builder.batchRequestByteThreshold;
        this.batchDelayThreshold = builder.batchDelayThreshold;
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
        this.maxInFlightBytes = builder.maxInFlightBytes;
        this.publishProgressTimeout = builder.publishProgressTimeout;
        this.recoveryInitialBackoff = builder.recoveryInitialBackoff;
        this.recoveryMaxBackoff = builder.recoveryMaxBackoff;
        this.recoveryMaxAttempts = builder.recoveryMaxAttempts;
        this.shutdownTimeout = builder.shutdownTimeout;
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
     * Returns the default options: SDK-default batching and retries, ordering disabled, in-flight
     * caps of 1000 messages and 64 MiB, a topic auto-creation recovery budget of 500 ms doubling to
     * 10 s over 10 attempts, and a 30 s publisher shutdown budget.
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

    /** Returns the writer's cap on the serialized bytes of unacknowledged publishes. */
    public long getMaxInFlightBytes() {
        return maxInFlightBytes;
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

    /** Returns how long the writer waits with no publish completing before it fails. */
    public Duration getPublishProgressTimeout() {
        return publishProgressTimeout;
    }

    /** Returns how long the writer's close waits for a publisher to shut down. */
    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    /** Returns whether the writer registers per-topic send counters. */
    public boolean isPerDestinationMetrics() {
        return perDestinationMetrics;
    }

    /**
     * Returns the topic auto-creation recovery schedule the {@code recovery*} knobs describe.
     * Jittered: every subtask that parked publishes for the same missing topic resumes against the
     * same freshly created topic, so unjittered they would republish in lockstep.
     */
    @Internal
    public RetrySchedule toRecoverySchedule() {
        return new RetrySchedule(
                recoveryInitialBackoff.toMillis(),
                recoveryMaxBackoff.toMillis(),
                recoveryMaxAttempts,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    /** Returns whether any batching knob deviates from the SDK default. */
    public boolean hasBatchingOverrides() {
        return batchElementCountThreshold != null
                || batchRequestByteThreshold != null
                || batchDelayThreshold != null;
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
                && perDestinationMetrics == that.perDestinationMetrics
                && maxInFlightMessages == that.maxInFlightMessages
                && maxInFlightBytes == that.maxInFlightBytes
                && publishProgressTimeout.equals(that.publishProgressTimeout)
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && Objects.equals(batchElementCountThreshold, that.batchElementCountThreshold)
                && Objects.equals(batchRequestByteThreshold, that.batchRequestByteThreshold)
                && Objects.equals(batchDelayThreshold, that.batchDelayThreshold)
                && Objects.equals(retryTotalTimeout, that.retryTotalTimeout)
                && Objects.equals(retryInitialDelay, that.retryInitialDelay)
                && Objects.equals(retryDelayMultiplier, that.retryDelayMultiplier)
                && Objects.equals(retryMaxDelay, that.retryMaxDelay)
                && Objects.equals(retryInitialRpcTimeout, that.retryInitialRpcTimeout)
                && Objects.equals(retryRpcTimeoutMultiplier, that.retryRpcTimeoutMultiplier)
                && Objects.equals(retryMaxRpcTimeout, that.retryMaxRpcTimeout)
                && Objects.equals(retryMaxAttempts, that.retryMaxAttempts)
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff)
                && shutdownTimeout.equals(that.shutdownTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchElementCountThreshold,
                batchRequestByteThreshold,
                batchDelayThreshold,
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
                maxInFlightBytes,
                publishProgressTimeout,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts,
                shutdownTimeout,
                perDestinationMetrics);
    }

    @Override
    public String toString() {
        return "PubSubPublisherOptions{batchElementCountThreshold="
                + batchElementCountThreshold
                + ", batchRequestByteThreshold="
                + batchRequestByteThreshold
                + ", batchDelayThreshold="
                + batchDelayThreshold
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
                + ", maxInFlightBytes="
                + maxInFlightBytes
                + ", publishProgressTimeout="
                + publishProgressTimeout
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + ", shutdownTimeout="
                + shutdownTimeout
                + ", perDestinationMetrics="
                + perDestinationMetrics
                + "}";
    }

    /** Builder for {@link PubSubPublisherOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Long batchElementCountThreshold;
        @Nullable private Long batchRequestByteThreshold;
        @Nullable private Duration batchDelayThreshold;
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
        private long maxInFlightBytes = 64L * 1024 * 1024;

        /** The largest {@code Duration} a nanosecond budget can express. */
        private static final Duration MAX_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);

        private Duration publishProgressTimeout = Duration.ofSeconds(600);
        private Duration recoveryInitialBackoff = Duration.ofMillis(500);
        private Duration recoveryMaxBackoff = Duration.ofSeconds(10);
        private int recoveryMaxAttempts = 10;
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private boolean perDestinationMetrics;

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
         * Sets the total time budget of a publish including its retries. Optional; defaults to the
         * SDK's timeout.
         *
         * <p><b>Cannot be combined with {@link #enableMessageOrdering(boolean)
         * enableMessageOrdering(true)}</b>, which {@link #build()} rejects: an ordering-enabled SDK
         * publisher replaces this and {@link #retryMaxAttempts(int)} with an effectively infinite
         * budget, so setting either would promise a bound the publisher does not have.
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
         * <p><b>Cannot be combined with {@link #enableMessageOrdering(boolean)
         * enableMessageOrdering(true)}</b>; see {@link #retryTotalTimeout(Duration)}.
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
         * <p>Enabling it costs the publish retry budget: {@link #build()} rejects an explicit
         * {@link #retryTotalTimeout(Duration)} or {@link #retryMaxAttempts(int)} beside it, because
         * the SDK publisher would replace both.
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
         * Caps the total {@code PubsubMessage.getSerializedSize()} of the writer's unacknowledged
         * publishes, bounding sink memory where the message count cannot: Pub/Sub allows 10 MiB per
         * message, so {@link #maxInFlightMessages(int)} alone leaves the retained payload
         * unbounded. Defaults to 64 MiB per writer subtask — with the default message cap of 1000
         * that binds only above ~64 KiB per message, so small-message pipelines keep today's
         * behavior.
         *
         * <p><b>Sizing:</b> this value, times the sink subtasks sharing a TaskManager, must fit
         * that TaskManager's heap budget. The counter measures serialized protobuf size, which
         * under-counts actual JVM retention — leave headroom.
         *
         * <p>Like the message cap, a write at the cap yields to the task mailbox rather than
         * blocking the task thread, and it applies with {@link #enableMessageOrdering(boolean)}
         * enabled. A message larger than the cap is still published rather than rejected, exceeding
         * the cap until it completes. Pass {@link Long#MAX_VALUE} to bound by message count only.
         *
         * @param maxInFlightBytes the in-flight byte cap, positive
         * @return this builder
         */
        public Builder maxInFlightBytes(long maxInFlightBytes) {
            Preconditions.checkArgument(maxInFlightBytes > 0, "maxInFlightBytes must be positive");
            this.maxInFlightBytes = maxInFlightBytes;
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
                    checkAtLeastOneMilli(recoveryInitialBackoff, "recoveryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the topic auto-creation recovery. Defaults to 10 s.
         *
         * @param recoveryMaxBackoff the backoff cap, positive and at least the initial backoff
         * @return this builder
         */
        public Builder recoveryMaxBackoff(Duration recoveryMaxBackoff) {
            this.recoveryMaxBackoff =
                    checkAtLeastOneMilli(recoveryMaxBackoff, "recoveryMaxBackoff");
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
         * Sets how long the writer may wait with <em>no</em> publish completing before it fails.
         * Defaults to 600 seconds.
         *
         * <p>This bounds a stall, not a slow topic: the budget restarts at every completion, so a
         * topic that keeps answering — however slowly, and however long the wait in total — never
         * spends it, while a publisher that has stopped resolving anything at all fails the job
         * once. It covers both waits the writer makes on the task thread, the in-flight admission
         * gate in {@code write} and the drain at a checkpoint, because which of the two a stalled
         * sink is parked in depends only on whether the in-flight cap fills before the checkpoint
         * barrier arrives.
         *
         * <p>Without {@code enableMessageOrdering} this rarely fires: a publish gives up at {@code
         * retryTotalTimeout} (600 s by default), which fails the job by itself. With ordering the
         * SDK retries a publish without limit, so nothing <em>inside the sink</em> ends an outage
         * but this. Outside it, Flink's own {@code execution.checkpointing.timeout} still fails the
         * job at its default — later, and naming nothing about Pub/Sub — but only while {@code
         * execution.checkpointing.tolerable-failed-checkpoints} is 0. Raise that and this budget is
         * the only thing left (issue #333).
         *
         * <p>Expiry fails the job and drops nothing: the sink is at-least-once and stores nothing
         * in Flink state, so the records behind the unresolved publishes are replayed from the last
         * completed checkpoint. The cost of that is real and is the reason this is a knob: a
         * disturbance that outlasts the budget now restarts the job where the SDK's retries used to
         * absorb it, and a persistent one becomes a restart loop.
         *
         * @param publishProgressTimeout the no-progress budget, positive and at most {@code
         *     Duration.ofNanos(Long.MAX_VALUE)}
         * @return this builder
         */
        public Builder publishProgressTimeout(Duration publishProgressTimeout) {
            checkPositive(publishProgressTimeout, "publishProgressTimeout");
            // Expressible in nanoseconds, because that is the arithmetic the writer's wait does:
            // a Duration past that throws from toNanos() on a TaskManager rather than here, at the
            // first wait the writer makes (#334 is the same trap one level down, in the two
            // shutdownTimeout setters).
            Preconditions.checkArgument(
                    publishProgressTimeout.compareTo(MAX_TIMEOUT) <= 0,
                    "publishProgressTimeout must be at most %s",
                    MAX_TIMEOUT);
            this.publishProgressTimeout = publishProgressTimeout;
            return this;
        }

        /**
         * Sets how long the writer's close waits for one publisher to shut down. Defaults to 30
         * seconds.
         *
         * <p>The budget is measured from the moment the writer asks the publisher to shut down, and
         * every publisher it owns is asked before any is waited on, so a close costs this once
         * however many topics the writer wrote to. Keep it under Flink's {@code
         * task.cancellation.timeout} (180 s by default), past which a cancelling task is a fatal
         * TaskManager error — that watchdog covers cancellation only, so on a task failure or a
         * clean shutdown an over-long close merely delays the task rather than killing the
         * TaskManager.
         *
         * @param shutdownTimeout the shutdown budget, positive
         * @return this builder
         */
        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = checkPositive(shutdownTimeout, "shutdownTimeout");
            return this;
        }

        /**
         * Registers per-topic {@code recordsSend} and {@code sendErrors} counters beside the
         * writer's totals. Defaults to {@code false}.
         *
         * <p>Off by default because Flink cannot unregister a metric: with per-record destinations
         * the topic set is unbounded, so every topic the job ever writes to keeps a row in the
         * metric registry for the lifetime of the task. Switch it on for a sink whose destinations
         * are few and known.
         *
         * @param perDestinationMetrics whether to register per-topic counters
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
        public PubSubPublisherOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be at least recoveryInitialBackoff.");
            // Rejected rather than ignored: the SDK publisher's constructor replaces both of these
            // with "retry forever" whenever ordering is on, so a budget set here would silently
            // not be one. Only an explicitly set knob is a conflict — the SDK's own defaults are
            // what an ordering-enabled publisher is expected to override.
            if (enableMessageOrdering) {
                List<String> bounded = new ArrayList<>(2);
                if (retryTotalTimeout != null) {
                    bounded.add("retryTotalTimeout(...)");
                }
                if (retryMaxAttempts != null) {
                    bounded.add("retryMaxAttempts(...)");
                }
                // Names the knob that was actually set rather than both, as PubSubSourceBuilder's
                // cross-checks do: being told to remove something you never configured is the way
                // a correct message still costs a reader time.
                String names = String.join(" and ", bounded);
                Preconditions.checkState(
                        bounded.isEmpty(),
                        "%s cannot be combined with enableMessageOrdering(true): an ordering-enabled"
                                + " SDK publisher retries without limit, so neither an attempt cap"
                                + " nor a total timeout can bound a publish there — for messages"
                                + " without an ordering key too. Remove %s, or disable message"
                                + " ordering. The other six retry knobs are unaffected.",
                        names,
                        names);
            }
            return new PubSubPublisherOptions(this);
        }

        private static Duration checkPositive(Duration duration, String name) {
            Preconditions.checkNotNull(duration, "%s must not be null", name);
            Preconditions.checkArgument(
                    !duration.isZero() && !duration.isNegative(), "%s must be positive", name);
            return duration;
        }

        private static Duration checkAtLeastOneMilli(Duration duration, String name) {
            checkPositive(duration, name);
            Preconditions.checkArgument(
                    duration.toMillis() >= 1,
                    "%s must be at least 1 millisecond (it is applied at millisecond"
                            + " granularity)",
                    name);
            return duration;
        }
    }
}
