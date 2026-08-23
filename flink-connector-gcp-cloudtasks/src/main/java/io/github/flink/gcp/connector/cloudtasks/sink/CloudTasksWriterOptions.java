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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the sink's writer: the in-flight cap, the transport channel pool and the two
 * retry budgets.
 *
 * <p>Set via {@link CloudTasksSinkBuilder#writerOptions(CloudTasksWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all.
 *
 * <p>There are deliberately <em>no</em> rate knobs. Cloud Tasks paces dispatch on the queue ({@code
 * maxDispatchesPerSecond}, {@code maxConcurrentDispatches}, the retry configuration), which is
 * queue configuration applied by whoever creates the queue. What this sink bounds is how many task
 * creations it keeps outstanding, not how fast the tasks execute. The channel pool is not a rate
 * knob either: it sizes how much of the in-flight cap the transport can actually carry
 * concurrently.
 *
 * <p>Retries are the sink's own because the generated client does not retry {@code CreateTask}: its
 * retryable-code set is empty and its total timeout is 20 seconds, while the read-only methods do
 * retry (verified in {@code CloudTasksStubSettings}; the dated verification record is ADR-0048).
 * {@code NOT_FOUND} has a budget of its own because a queue idle for 30 days takes a few minutes to
 * re-activate and returns {@code NOT_FOUND} meanwhile, while a mistyped queue name must not burn
 * the full retry budget on every record before the job fails.
 *
 * <p>Instances are immutable and serializable.
 */
@Public
public final class CloudTasksWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final CloudTasksWriterOptions DEFAULTS = builder().build();

    private final int maxInFlightTasks;
    @Nullable private final Integer channelPoolSize;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;
    private final Duration notFoundRecoveryInitialBackoff;
    private final Duration notFoundRecoveryMaxBackoff;
    private final int notFoundRecoveryMaxAttempts;
    private final boolean perDestinationMetrics;

    private CloudTasksWriterOptions(Builder builder) {
        this.maxInFlightTasks = builder.maxInFlightTasks;
        this.channelPoolSize = builder.channelPoolSize;
        this.recoveryInitialBackoff = builder.recoveryInitialBackoff;
        this.recoveryMaxBackoff = builder.recoveryMaxBackoff;
        this.recoveryMaxAttempts = builder.recoveryMaxAttempts;
        this.notFoundRecoveryInitialBackoff = builder.notFoundRecoveryInitialBackoff;
        this.notFoundRecoveryMaxBackoff = builder.notFoundRecoveryMaxBackoff;
        this.notFoundRecoveryMaxAttempts = builder.notFoundRecoveryMaxAttempts;
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
     * Returns the default options: an in-flight cap of 1000, the client's default transport (one
     * gRPC channel), a transient-failure budget of 100 ms doubling to 10 s over 8 attempts, and a
     * {@code NOT_FOUND} budget of 500 ms doubling to 2 s over 3 attempts.
     *
     * @return the default options
     */
    public static CloudTasksWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the writer's cap on outstanding task creations. */
    public int getMaxInFlightTasks() {
        return maxInFlightTasks;
    }

    /**
     * Returns the configured gRPC channel-pool size, or {@code null} when the client's default
     * transport is left alone.
     */
    @Nullable
    public Integer getChannelPoolSize() {
        return channelPoolSize;
    }

    /** Returns the first backoff of the transient-failure retry. */
    public Duration getRecoveryInitialBackoff() {
        return recoveryInitialBackoff;
    }

    /** Returns the backoff cap of the transient-failure retry. */
    public Duration getRecoveryMaxBackoff() {
        return recoveryMaxBackoff;
    }

    /** Returns the maximum attempts of the transient-failure retry. */
    public int getRecoveryMaxAttempts() {
        return recoveryMaxAttempts;
    }

    /** Returns the first backoff of the {@code NOT_FOUND} retry. */
    public Duration getNotFoundRecoveryInitialBackoff() {
        return notFoundRecoveryInitialBackoff;
    }

    /** Returns the backoff cap of the {@code NOT_FOUND} retry. */
    public Duration getNotFoundRecoveryMaxBackoff() {
        return notFoundRecoveryMaxBackoff;
    }

    /** Returns the maximum attempts of the {@code NOT_FOUND} retry. */
    public int getNotFoundRecoveryMaxAttempts() {
        return notFoundRecoveryMaxAttempts;
    }

    /** Returns whether the writer registers per-queue send counters. */
    public boolean isPerDestinationMetrics() {
        return perDestinationMetrics;
    }

    /**
     * Returns the schedule retrying {@code UNAVAILABLE}, {@code DEADLINE_EXCEEDED} and {@code
     * RESOURCE_EXHAUSTED} creations.
     */
    @Internal
    public RetrySchedule toRecoverySchedule() {
        return new RetrySchedule(
                recoveryInitialBackoff.toMillis(),
                recoveryMaxBackoff.toMillis(),
                recoveryMaxAttempts,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    /**
     * Returns the schedule retrying {@code NOT_FOUND} creations. Jittered like the transient
     * schedule: every subtask that hit the same missing queue retries against the same queue once
     * it is created, and the jitter is mean-preserving, so spreading the attempts costs the short
     * budget nothing in expectation.
     */
    @Internal
    public RetrySchedule toNotFoundRecoverySchedule() {
        return new RetrySchedule(
                notFoundRecoveryInitialBackoff.toMillis(),
                notFoundRecoveryMaxBackoff.toMillis(),
                notFoundRecoveryMaxAttempts,
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
        CloudTasksWriterOptions that = (CloudTasksWriterOptions) o;
        return maxInFlightTasks == that.maxInFlightTasks
                && Objects.equals(channelPoolSize, that.channelPoolSize)
                && perDestinationMetrics == that.perDestinationMetrics
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && notFoundRecoveryMaxAttempts == that.notFoundRecoveryMaxAttempts
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff)
                && notFoundRecoveryInitialBackoff.equals(that.notFoundRecoveryInitialBackoff)
                && notFoundRecoveryMaxBackoff.equals(that.notFoundRecoveryMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxInFlightTasks,
                channelPoolSize,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts,
                notFoundRecoveryInitialBackoff,
                notFoundRecoveryMaxBackoff,
                notFoundRecoveryMaxAttempts,
                perDestinationMetrics);
    }

    @Override
    public String toString() {
        return "CloudTasksWriterOptions{maxInFlightTasks="
                + maxInFlightTasks
                + ", channelPoolSize="
                + channelPoolSize
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + ", notFoundRecoveryInitialBackoff="
                + notFoundRecoveryInitialBackoff
                + ", notFoundRecoveryMaxBackoff="
                + notFoundRecoveryMaxBackoff
                + ", notFoundRecoveryMaxAttempts="
                + notFoundRecoveryMaxAttempts
                + ", perDestinationMetrics="
                + perDestinationMetrics
                + "}";
    }

    /** Builder for {@link CloudTasksWriterOptions}. */
    @Public
    public static final class Builder {

        private int maxInFlightTasks = 1000;
        @Nullable private Integer channelPoolSize;
        private Duration recoveryInitialBackoff = Duration.ofMillis(100);
        private Duration recoveryMaxBackoff = Duration.ofSeconds(10);
        private int recoveryMaxAttempts = 8;
        private boolean perDestinationMetrics;
        private Duration notFoundRecoveryInitialBackoff = Duration.ofMillis(500);
        private Duration notFoundRecoveryMaxBackoff = Duration.ofSeconds(2);
        private int notFoundRecoveryMaxAttempts = 3;

        private Builder() {}

        /**
         * Caps the task creations the writer keeps outstanding — those in flight plus those waiting
         * out a retry backoff. A write at the cap yields to the task mailbox until creations
         * complete, bounding sink memory between checkpoints. Defaults to 1000.
         *
         * @param maxInFlightTasks the in-flight cap, positive
         * @return this builder
         */
        public Builder maxInFlightTasks(int maxInFlightTasks) {
            Preconditions.checkArgument(maxInFlightTasks > 0, "maxInFlightTasks must be positive");
            this.maxInFlightTasks = maxInFlightTasks;
            return this;
        }

        /**
         * Sizes the client's gRPC channel pool. Unset by default, leaving the client's own
         * transport configuration — one channel — alone.
         *
         * <p>One HTTP/2 channel carries about 100 concurrent streams, so the default transport
         * delivers only ~100 of the in-flight cap's nominal concurrency no matter how high the cap
         * is set; a subtask that needs more concurrent creates than that needs more channels, about
         * one per 100 concurrent creates. Raising it can push a single subtask past the queue's
         * recommended ~1,000 TPS ceiling — mind the documented ramp guidance before doing so.
         * Rejected in combination with an emulator endpoint when the sink is built: the emulator
         * always uses one plaintext channel, so the pool would be silently ignored.
         *
         * @param channelPoolSize the number of gRPC channels, positive
         * @return this builder
         */
        public Builder channelPoolSize(int channelPoolSize) {
            Preconditions.checkArgument(channelPoolSize > 0, "channelPoolSize must be positive");
            this.channelPoolSize = channelPoolSize;
            return this;
        }

        /**
         * Sets the first backoff of the transient-failure retry ({@code UNAVAILABLE}, {@code
         * DEADLINE_EXCEEDED}, {@code RESOURCE_EXHAUSTED}). Defaults to 100 ms.
         *
         * @param recoveryInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder recoveryInitialBackoff(Duration recoveryInitialBackoff) {
            this.recoveryInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            recoveryInitialBackoff, "recoveryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the transient-failure retry. Defaults to 10 s.
         *
         * @param recoveryMaxBackoff the backoff cap, at least 1 ms and at least the initial backoff
         * @return this builder
         */
        public Builder recoveryMaxBackoff(Duration recoveryMaxBackoff) {
            this.recoveryMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(recoveryMaxBackoff, "recoveryMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts of the transient-failure retry, the initial creation included. Defaults
         * to 8; exhausting the budget fails the job.
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
         * Sets the first backoff of the {@code NOT_FOUND} retry. Defaults to 500 ms.
         *
         * @param notFoundRecoveryInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder notFoundRecoveryInitialBackoff(Duration notFoundRecoveryInitialBackoff) {
            this.notFoundRecoveryInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            notFoundRecoveryInitialBackoff, "notFoundRecoveryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the {@code NOT_FOUND} retry. Defaults to 2 s.
         *
         * @param notFoundRecoveryMaxBackoff the backoff cap, at least 1 ms and at least the initial
         *     backoff
         * @return this builder
         */
        public Builder notFoundRecoveryMaxBackoff(Duration notFoundRecoveryMaxBackoff) {
            this.notFoundRecoveryMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            notFoundRecoveryMaxBackoff, "notFoundRecoveryMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts of the {@code NOT_FOUND} retry, the initial creation included. Defaults
         * to 3: long enough to ride out a queue that is briefly unavailable, short enough that a
         * mistyped queue name fails quickly. A queue that takes minutes to re-activate outlives
         * this budget on purpose — recovering from that is the job's restart strategy, not the
         * writer's.
         *
         * @param notFoundRecoveryMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder notFoundRecoveryMaxAttempts(int notFoundRecoveryMaxAttempts) {
            Preconditions.checkArgument(
                    notFoundRecoveryMaxAttempts > 0,
                    "notFoundRecoveryMaxAttempts must be positive");
            this.notFoundRecoveryMaxAttempts = notFoundRecoveryMaxAttempts;
            return this;
        }

        /**
         * Registers per-queue {@code recordsSend} and {@code sendErrors} counters beside the
         * writer's totals. Defaults to {@code false}.
         *
         * <p>Off by default because Flink cannot unregister a metric: with a per-record {@code
         * destinationResolver} the queue set is unbounded, so every queue the job ever writes to
         * keeps a row in the metric registry for the lifetime of the task. Switch it on for a sink
         * whose queues are few and known — a fixed {@code queue(...)} especially.
         *
         * @param perDestinationMetrics whether to register per-queue counters
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
        public CloudTasksWriterOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be at least recoveryInitialBackoff.");
            Preconditions.checkState(
                    notFoundRecoveryMaxBackoff.compareTo(notFoundRecoveryInitialBackoff) >= 0,
                    "notFoundRecoveryMaxBackoff must be at least notFoundRecoveryInitialBackoff.");
            return new CloudTasksWriterOptions(this);
        }
    }
}
