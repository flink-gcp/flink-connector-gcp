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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the source's Pub/Sub subscribers and its reader: SDK flow control, the
 * streaming-pull connection count and the acknowledgement-deadline extension settings, plus the
 * source's own drain size, subscriber shutdown budget and first-checkpoint watchdog.
 *
 * <p>Set via {@link PubSubSourceBuilder#subscriberOptions(PubSubSubscriberOptions)}; optional —
 * every knob left unset keeps the SDK's (or the source's) default behavior, so {@link #defaults()}
 * is equivalent to not setting options at all.
 *
 * <p>Flow control is what bounds how many messages the client library holds for this source, and
 * therefore how much a reader buffers: the source acknowledges only on checkpoint completion, so
 * everything received since the last completed checkpoint counts against these limits. The limit
 * behavior itself is not exposed because the SDK subscriber does not expose it either — it forces
 * blocking regardless of what the settings say, which for a subscriber simply means it stops
 * pulling.
 *
 * <p>The subscriber shutdown <em>mode</em> is deliberately not a knob. It is fixed to {@code
 * NACK_IMMEDIATELY} so that closing a reader releases messages at once; the SDK's {@code
 * WAIT_FOR_PROCESSING} default would wait for acknowledgements that only arrive at checkpoint
 * completion, which never happens during shutdown. Only {@link Builder#shutdownTimeout(Duration)}
 * is configurable.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class PubSubSubscriberOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final PubSubSubscriberOptions DEFAULTS = builder().build();

    @Nullable private final Long flowControlMaxOutstandingElementCount;
    @Nullable private final Long flowControlMaxOutstandingRequestBytes;
    @Nullable private final Integer parallelPullCount;
    @Nullable private final Duration maxAckExtensionPeriod;
    @Nullable private final Duration minDurationPerAckExtension;
    @Nullable private final Duration maxDurationPerAckExtension;
    @Nullable private final Duration awaitAckConfirmation;
    private final Duration shutdownTimeout;
    private final int maxRecordsPerFetch;
    private final Duration firstCheckpointTimeout;

    private PubSubSubscriberOptions(Builder builder) {
        this.flowControlMaxOutstandingElementCount = builder.flowControlMaxOutstandingElementCount;
        this.flowControlMaxOutstandingRequestBytes = builder.flowControlMaxOutstandingRequestBytes;
        this.parallelPullCount = builder.parallelPullCount;
        this.maxAckExtensionPeriod = builder.maxAckExtensionPeriod;
        this.minDurationPerAckExtension = builder.minDurationPerAckExtension;
        this.maxDurationPerAckExtension = builder.maxDurationPerAckExtension;
        this.awaitAckConfirmation = builder.awaitAckConfirmation;
        this.shutdownTimeout = builder.shutdownTimeout;
        this.maxRecordsPerFetch = builder.maxRecordsPerFetch;
        this.firstCheckpointTimeout = builder.firstCheckpointTimeout;
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
     * Returns the default options: SDK-default flow control, connection count and
     * acknowledgement-deadline extension, a 5 s subscriber shutdown budget, 1000 messages drained
     * per split per fetch, and a 10 min first-checkpoint watchdog.
     *
     * @return the default options
     */
    public static PubSubSubscriberOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the flow-control outstanding-message limit, or {@code null} for the SDK default. */
    @Nullable
    public Long getFlowControlMaxOutstandingElementCount() {
        return flowControlMaxOutstandingElementCount;
    }

    /** Returns the flow-control outstanding-byte limit, or {@code null} for the SDK default. */
    @Nullable
    public Long getFlowControlMaxOutstandingRequestBytes() {
        return flowControlMaxOutstandingRequestBytes;
    }

    /** Returns the streaming-pull connection count, or {@code null} for the SDK default. */
    @Nullable
    public Integer getParallelPullCount() {
        return parallelPullCount;
    }

    /**
     * Returns the total acknowledgement-deadline extension budget, or {@code null} for the SDK
     * default.
     */
    @Nullable
    public Duration getMaxAckExtensionPeriod() {
        return maxAckExtensionPeriod;
    }

    /** Returns the smallest single deadline extension, or {@code null} for the SDK default. */
    @Nullable
    public Duration getMinDurationPerAckExtension() {
        return minDurationPerAckExtension;
    }

    /** Returns the largest single deadline extension, or {@code null} for the SDK default. */
    @Nullable
    public Duration getMaxDurationPerAckExtension() {
        return maxDurationPerAckExtension;
    }

    /**
     * Returns how long a completed checkpoint waits for its acknowledgements to be confirmed, or
     * {@code null} when acknowledgement is fire-and-forget.
     */
    @Nullable
    public Duration getAwaitAckConfirmation() {
        return awaitAckConfirmation;
    }

    /** Returns how long closing one subscriber waits for it to release its messages. */
    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    /** Returns the maximum number of messages drained from one split per fetch. */
    public int getMaxRecordsPerFetch() {
        return maxRecordsPerFetch;
    }

    /**
     * Returns how long the reader waits for its first checkpoint before failing the job, or {@link
     * Duration#ZERO} when the watchdog is disabled.
     */
    public Duration getFirstCheckpointTimeout() {
        return firstCheckpointTimeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubSubscriberOptions that = (PubSubSubscriberOptions) o;
        return maxRecordsPerFetch == that.maxRecordsPerFetch
                && Objects.equals(awaitAckConfirmation, that.awaitAckConfirmation)
                && Objects.equals(
                        flowControlMaxOutstandingElementCount,
                        that.flowControlMaxOutstandingElementCount)
                && Objects.equals(
                        flowControlMaxOutstandingRequestBytes,
                        that.flowControlMaxOutstandingRequestBytes)
                && Objects.equals(parallelPullCount, that.parallelPullCount)
                && Objects.equals(maxAckExtensionPeriod, that.maxAckExtensionPeriod)
                && Objects.equals(minDurationPerAckExtension, that.minDurationPerAckExtension)
                && Objects.equals(maxDurationPerAckExtension, that.maxDurationPerAckExtension)
                && shutdownTimeout.equals(that.shutdownTimeout)
                && firstCheckpointTimeout.equals(that.firstCheckpointTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                flowControlMaxOutstandingElementCount,
                flowControlMaxOutstandingRequestBytes,
                parallelPullCount,
                maxAckExtensionPeriod,
                minDurationPerAckExtension,
                maxDurationPerAckExtension,
                awaitAckConfirmation,
                shutdownTimeout,
                maxRecordsPerFetch,
                firstCheckpointTimeout);
    }

    @Override
    public String toString() {
        return "PubSubSubscriberOptions{flowControlMaxOutstandingElementCount="
                + flowControlMaxOutstandingElementCount
                + ", flowControlMaxOutstandingRequestBytes="
                + flowControlMaxOutstandingRequestBytes
                + ", parallelPullCount="
                + parallelPullCount
                + ", maxAckExtensionPeriod="
                + maxAckExtensionPeriod
                + ", minDurationPerAckExtension="
                + minDurationPerAckExtension
                + ", maxDurationPerAckExtension="
                + maxDurationPerAckExtension
                + ", awaitAckConfirmation="
                + awaitAckConfirmation
                + ", shutdownTimeout="
                + shutdownTimeout
                + ", maxRecordsPerFetch="
                + maxRecordsPerFetch
                + ", firstCheckpointTimeout="
                + firstCheckpointTimeout
                + "}";
    }

    /** Builder for {@link PubSubSubscriberOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Long flowControlMaxOutstandingElementCount;
        @Nullable private Long flowControlMaxOutstandingRequestBytes;
        @Nullable private Integer parallelPullCount;
        @Nullable private Duration maxAckExtensionPeriod;
        @Nullable private Duration minDurationPerAckExtension;
        @Nullable private Duration maxDurationPerAckExtension;
        @Nullable private Duration awaitAckConfirmation;
        private Duration shutdownTimeout = Duration.ofSeconds(5);
        private int maxRecordsPerFetch = 1_000;
        private Duration firstCheckpointTimeout = Duration.ofMinutes(10);

        private Builder() {}

        /**
         * Caps the messages one subscriber holds outstanding; the client library stops pulling once
         * the cap is reached. Optional; defaults to the SDK's limit of 1000 messages. Because the
         * source acknowledges only on checkpoint completion, everything received since the last
         * completed checkpoint counts against this cap.
         *
         * @param flowControlMaxOutstandingElementCount the outstanding-message limit, positive
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
         * Caps the bytes one subscriber holds outstanding; the client library stops pulling once
         * the cap is reached. Optional; defaults to the SDK's limit of 100 MB. This is the
         * byte-level bound the message-count cap cannot provide.
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
         * Sets how many streaming-pull connections one subscriber opens. Optional; defaults to the
         * SDK's single connection. More connections raise a single split's throughput at the cost
         * of more gRPC streams.
         *
         * <p>Cannot be combined with {@link OrderingMode#PER_KEY}; {@link
         * PubSubSourceBuilder#build()} rejects the combination and explains why.
         *
         * @param parallelPullCount the streaming-pull connection count, positive
         * @return this builder
         */
        public Builder parallelPullCount(int parallelPullCount) {
            Preconditions.checkArgument(
                    parallelPullCount > 0, "parallelPullCount must be positive");
            this.parallelPullCount = parallelPullCount;
            return this;
        }

        /**
         * Sets how long the client library keeps extending a message's acknowledgement deadline
         * before giving up on it. Optional; defaults to the SDK's 1 hour.
         *
         * <p>This is the ceiling on how long a message may wait for the checkpoint that
         * acknowledges it: once the budget is spent the lease expires and Pub/Sub redelivers, so it
         * must stay comfortably above the checkpoint interval.
         *
         * @param maxAckExtensionPeriod the total extension budget, positive
         * @return this builder
         */
        public Builder maxAckExtensionPeriod(Duration maxAckExtensionPeriod) {
            this.maxAckExtensionPeriod =
                    checkPositive(maxAckExtensionPeriod, "maxAckExtensionPeriod");
            return this;
        }

        /**
         * Sets the smallest deadline extension the client library requests at a time. Optional;
         * defaults to the SDK's adaptive choice, which derives the extension from observed
         * acknowledgement latencies.
         *
         * @param minDurationPerAckExtension the smallest single extension, positive and below
         *     {@link #maxDurationPerAckExtension(Duration)}
         * @return this builder
         */
        public Builder minDurationPerAckExtension(Duration minDurationPerAckExtension) {
            this.minDurationPerAckExtension =
                    checkPositive(minDurationPerAckExtension, "minDurationPerAckExtension");
            return this;
        }

        /**
         * Sets the largest deadline extension the client library requests at a time. Optional;
         * defaults to the SDK's adaptive choice.
         *
         * @param maxDurationPerAckExtension the largest single extension, positive and above {@link
         *     #minDurationPerAckExtension(Duration)}
         * @return this builder
         */
        public Builder maxDurationPerAckExtension(Duration maxDurationPerAckExtension) {
            this.maxDurationPerAckExtension =
                    checkPositive(maxDurationPerAckExtension, "maxDurationPerAckExtension");
            return this;
        }

        /**
         * Makes each completed checkpoint wait for its acknowledgements to be confirmed by the
         * server, failing the job if they are not confirmed within the given time. Optional;
         * defaults to fire-and-forget acknowledgement, which adds no latency.
         *
         * <p>This exists because a failed acknowledgement is otherwise invisible. On an ordinary
         * subscription the client library does not retry one — it logs a warning and stops. No data
         * is lost, since an unacknowledged message has its lease expire and is redelivered, which
         * is the at-least-once contract; but a <em>persistent</em> failure such as a revoked
         * permission becomes a silent reprocessing loop.
         *
         * <p><b>The timeout is the only detector.</b> On a subscription without exactly-once
         * delivery the acknowledgement future completes with {@code SUCCESSFUL} on success and
         * <em>never completes at all</em> on failure, so there is no error to observe — only the
         * absence of a confirmation. Choose a value comfortably above a normal acknowledgement
         * round trip.
         *
         * <p>The wait happens on the task thread when the checkpoint completes, so it delays
         * processing by up to this long. That is the price of the confirmation.
         *
         * @param awaitAckConfirmation how long to wait for confirmation, positive
         * @return this builder
         */
        public Builder awaitAckConfirmation(Duration awaitAckConfirmation) {
            this.awaitAckConfirmation = checkPositive(awaitAckConfirmation, "awaitAckConfirmation");
            return this;
        }

        /**
         * Sets how long closing a subscriber waits for it to release its messages. Defaults to 5 s.
         *
         * <p>It bounds a reader's whole close, not each split's: the reader nacks every split and
         * asks every client to stop before it waits on any, so the waits overlap however many
         * splits it owns. Keep it under Flink's {@code source.reader.close.timeout} (30 s by
         * default), past which a reader is abandoned mid-close and the messages it had not yet
         * released only return after their acknowledgement deadline.
         *
         * @param shutdownTimeout the shutdown budget, positive
         * @return this builder
         */
        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = checkPositive(shutdownTimeout, "shutdownTimeout");
            return this;
        }

        /**
         * Caps how many messages one fetch drains from one split. Defaults to 1000. Bounds how much
         * a single fetch buffers while still amortizing the element-queue handoff; flow control,
         * not this, is the real limit on in-flight messages.
         *
         * @param maxRecordsPerFetch the drain size, positive
         * @return this builder
         */
        public Builder maxRecordsPerFetch(int maxRecordsPerFetch) {
            Preconditions.checkArgument(
                    maxRecordsPerFetch > 0, "maxRecordsPerFetch must be positive");
            this.maxRecordsPerFetch = maxRecordsPerFetch;
            return this;
        }

        /**
         * Sets how long a reader holding unacknowledged messages waits for its first checkpoint
         * before failing the job. Defaults to 10 min; {@link Duration#ZERO} disables the detector.
         *
         * <p>The source acknowledges only on checkpoint completion, so a job running without
         * checkpointing never acknowledges anything and stalls silently once flow control fills.
         * Raise this above the checkpoint interval for jobs that checkpoint less often than every
         * 10 min. See {@code MissingCheckpointDetector} for why the guard measures elapsed time
         * rather than reading the checkpoint configuration.
         *
         * @param firstCheckpointTimeout the detector budget, non-negative; zero disables it
         * @return this builder
         */
        public Builder firstCheckpointTimeout(Duration firstCheckpointTimeout) {
            Preconditions.checkNotNull(
                    firstCheckpointTimeout, "firstCheckpointTimeout must not be null");
            Preconditions.checkArgument(
                    !firstCheckpointTimeout.isNegative(),
                    "firstCheckpointTimeout must not be negative");
            this.firstCheckpointTimeout = firstCheckpointTimeout;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public PubSubSubscriberOptions build() {
            // The SDK enforces this itself, but with a message-less argument check; saying which
            // two knobs conflict is the whole value of doing it here.
            Preconditions.checkState(
                    minDurationPerAckExtension == null
                            || maxDurationPerAckExtension == null
                            || minDurationPerAckExtension.compareTo(maxDurationPerAckExtension) < 0,
                    "minDurationPerAckExtension must be shorter than maxDurationPerAckExtension.");
            return new PubSubSubscriberOptions(this);
        }

        // Duplicated from PubSubPublisherOptions.Builder; extracting a shared option-validation
        // helper is not worth a new public type for two call sites.
        private static Duration checkPositive(Duration duration, String name) {
            Preconditions.checkNotNull(duration, "%s must not be null", name);
            Preconditions.checkArgument(
                    !duration.isZero() && !duration.isNegative(), "%s must be positive", name);
            return duration;
        }
    }
}
