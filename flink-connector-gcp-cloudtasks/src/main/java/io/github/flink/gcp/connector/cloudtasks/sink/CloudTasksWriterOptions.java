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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the sink's writer: the in-flight cap and the two retry budgets.
 *
 * <p>Set via {@link CloudTasksSinkBuilder#writerOptions(CloudTasksWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all.
 *
 * <p>There are deliberately <em>no</em> rate knobs. Cloud Tasks paces dispatch on the queue ({@code
 * maxDispatchesPerSecond}, {@code maxConcurrentDispatches}, the retry configuration), which is
 * queue configuration applied by whoever creates the queue. What this sink bounds is how many task
 * creations it keeps outstanding, not how fast the tasks execute.
 *
 * <p>Retries are the sink's own because the generated client does not retry {@code CreateTask}: its
 * retryable-code set is empty and its total timeout is 20 seconds (verified in {@code
 * CloudTasksStubSettings} for {@code google-cloud-tasks} 2.94.0, where the read-only methods do
 * retry). {@code NOT_FOUND} has a budget of its own because a queue idle for 30 days takes a few
 * minutes to re-activate and returns {@code NOT_FOUND} meanwhile, while a mistyped queue name must
 * not burn the full retry budget on every record before the job fails.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class CloudTasksWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final CloudTasksWriterOptions DEFAULTS = builder().build();

    private final int maxInFlightTasks;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final int retryMaxAttempts;
    private final Duration notFoundInitialBackoff;
    private final Duration notFoundMaxBackoff;
    private final int notFoundMaxAttempts;
    private final boolean perDestinationMetrics;

    private CloudTasksWriterOptions(Builder builder) {
        this.maxInFlightTasks = builder.maxInFlightTasks;
        this.retryInitialBackoff = builder.retryInitialBackoff;
        this.retryMaxBackoff = builder.retryMaxBackoff;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.notFoundInitialBackoff = builder.notFoundInitialBackoff;
        this.notFoundMaxBackoff = builder.notFoundMaxBackoff;
        this.notFoundMaxAttempts = builder.notFoundMaxAttempts;
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
     * Returns the default options: an in-flight cap of 1000, a transient-failure budget of 100 ms
     * doubling to 10 s over 8 attempts, and a {@code NOT_FOUND} budget of 500 ms doubling to 2 s
     * over 3 attempts.
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

    /** Returns the first backoff of the transient-failure retry. */
    public Duration getRetryInitialBackoff() {
        return retryInitialBackoff;
    }

    /** Returns the backoff cap of the transient-failure retry. */
    public Duration getRetryMaxBackoff() {
        return retryMaxBackoff;
    }

    /** Returns the maximum attempts of the transient-failure retry. */
    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    /** Returns the first backoff of the {@code NOT_FOUND} retry. */
    public Duration getNotFoundInitialBackoff() {
        return notFoundInitialBackoff;
    }

    /** Returns the backoff cap of the {@code NOT_FOUND} retry. */
    public Duration getNotFoundMaxBackoff() {
        return notFoundMaxBackoff;
    }

    /** Returns the maximum attempts of the {@code NOT_FOUND} retry. */
    public int getNotFoundMaxAttempts() {
        return notFoundMaxAttempts;
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
    public RetrySchedule toRetrySchedule() {
        return new RetrySchedule(
                retryInitialBackoff.toMillis(),
                retryMaxBackoff.toMillis(),
                retryMaxAttempts,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    /**
     * Returns the schedule retrying {@code NOT_FOUND} creations. Jittered like the transient
     * schedule: every subtask that hit the same missing queue retries against the same queue once
     * it is created, and the jitter is mean-preserving, so spreading the attempts costs the short
     * budget nothing in expectation.
     */
    @Internal
    public RetrySchedule toNotFoundRetrySchedule() {
        return new RetrySchedule(
                notFoundInitialBackoff.toMillis(),
                notFoundMaxBackoff.toMillis(),
                notFoundMaxAttempts,
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
                && perDestinationMetrics == that.perDestinationMetrics
                && retryMaxAttempts == that.retryMaxAttempts
                && notFoundMaxAttempts == that.notFoundMaxAttempts
                && retryInitialBackoff.equals(that.retryInitialBackoff)
                && retryMaxBackoff.equals(that.retryMaxBackoff)
                && notFoundInitialBackoff.equals(that.notFoundInitialBackoff)
                && notFoundMaxBackoff.equals(that.notFoundMaxBackoff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxInFlightTasks,
                retryInitialBackoff,
                retryMaxBackoff,
                retryMaxAttempts,
                notFoundInitialBackoff,
                notFoundMaxBackoff,
                notFoundMaxAttempts,
                perDestinationMetrics);
    }

    @Override
    public String toString() {
        return "CloudTasksWriterOptions{maxInFlightTasks="
                + maxInFlightTasks
                + ", retryInitialBackoff="
                + retryInitialBackoff
                + ", retryMaxBackoff="
                + retryMaxBackoff
                + ", retryMaxAttempts="
                + retryMaxAttempts
                + ", notFoundInitialBackoff="
                + notFoundInitialBackoff
                + ", notFoundMaxBackoff="
                + notFoundMaxBackoff
                + ", notFoundMaxAttempts="
                + notFoundMaxAttempts
                + ", perDestinationMetrics="
                + perDestinationMetrics
                + "}";
    }

    /** Builder for {@link CloudTasksWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        private int maxInFlightTasks = 1000;
        private Duration retryInitialBackoff = Duration.ofMillis(100);
        private Duration retryMaxBackoff = Duration.ofSeconds(10);
        private int retryMaxAttempts = 8;
        private boolean perDestinationMetrics;
        private Duration notFoundInitialBackoff = Duration.ofMillis(500);
        private Duration notFoundMaxBackoff = Duration.ofSeconds(2);
        private int notFoundMaxAttempts = 3;

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
         * Sets the first backoff of the transient-failure retry ({@code UNAVAILABLE}, {@code
         * DEADLINE_EXCEEDED}, {@code RESOURCE_EXHAUSTED}). Defaults to 100 ms.
         *
         * @param retryInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder retryInitialBackoff(Duration retryInitialBackoff) {
            this.retryInitialBackoff =
                    checkAtLeastOneMilli(retryInitialBackoff, "retryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the transient-failure retry. Defaults to 10 s.
         *
         * @param retryMaxBackoff the backoff cap, at least 1 ms and at least the initial backoff
         * @return this builder
         */
        public Builder retryMaxBackoff(Duration retryMaxBackoff) {
            this.retryMaxBackoff = checkAtLeastOneMilli(retryMaxBackoff, "retryMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts of the transient-failure retry, the initial creation included. Defaults
         * to 8; exhausting the budget fails the job.
         *
         * @param retryMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder retryMaxAttempts(int retryMaxAttempts) {
            Preconditions.checkArgument(retryMaxAttempts > 0, "retryMaxAttempts must be positive");
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        /**
         * Sets the first backoff of the {@code NOT_FOUND} retry. Defaults to 500 ms.
         *
         * @param notFoundInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder notFoundInitialBackoff(Duration notFoundInitialBackoff) {
            this.notFoundInitialBackoff =
                    checkAtLeastOneMilli(notFoundInitialBackoff, "notFoundInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the {@code NOT_FOUND} retry. Defaults to 2 s.
         *
         * @param notFoundMaxBackoff the backoff cap, at least 1 ms and at least the initial backoff
         * @return this builder
         */
        public Builder notFoundMaxBackoff(Duration notFoundMaxBackoff) {
            this.notFoundMaxBackoff =
                    checkAtLeastOneMilli(notFoundMaxBackoff, "notFoundMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts of the {@code NOT_FOUND} retry, the initial creation included. Defaults
         * to 3: long enough to ride out a queue that is briefly unavailable, short enough that a
         * mistyped queue name fails quickly. A queue that takes minutes to re-activate outlives
         * this budget on purpose — recovering from that is the job's restart strategy, not the
         * writer's.
         *
         * @param notFoundMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder notFoundMaxAttempts(int notFoundMaxAttempts) {
            Preconditions.checkArgument(
                    notFoundMaxAttempts > 0, "notFoundMaxAttempts must be positive");
            this.notFoundMaxAttempts = notFoundMaxAttempts;
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
                    retryMaxBackoff.compareTo(retryInitialBackoff) >= 0,
                    "retryMaxBackoff must be at least retryInitialBackoff.");
            Preconditions.checkState(
                    notFoundMaxBackoff.compareTo(notFoundInitialBackoff) >= 0,
                    "notFoundMaxBackoff must be at least notFoundInitialBackoff.");
            return new CloudTasksWriterOptions(this);
        }

        private static Duration checkAtLeastOneMilli(Duration duration, String name) {
            Preconditions.checkNotNull(duration, "%s must not be null", name);
            Preconditions.checkArgument(
                    duration.toMillis() >= 1,
                    "%s must be at least 1 millisecond (it is applied at millisecond granularity)",
                    name);
            return duration;
        }
    }
}
