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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable staging and recovery settings for checkpointed Cloud Tasks creation. Set through {@link
 * CloudTasksSinkBuilder#stagedOptions(CloudTasksStagedOptions)}. The writer caps bound one batch,
 * not Flink's pending committable collector.
 */
@Experimental
public final class CloudTasksStagedOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Explicit recovery decisions outside the guarantee after an envelope expires. */
    @Experimental
    public enum ExpiredEnvelopePolicy {
        /** Fails without sending and leaves recovery to the retained checkpoint. */
        FAIL("fail"),
        /** Assumes expired work completed; use after a stop-with-savepoint reached FINISHED. */
        ASSUME_COMMITTED("assume-committed"),
        /** Creates expired work accepting the risk of duplicate tasks. */
        CREATE_ANYWAY("create-anyway"),
        /** Discards expired work accepting the risk of lost tasks. */
        DROP("drop");

        private final String value;

        ExpiredEnvelopePolicy(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private final Duration nameRetention;
    private final Duration clockSkewAllowance;
    private final Duration requestTimeout;
    private final int maxStagedTasks;
    private final long maxStagedBytes;
    private final boolean verifyQueueRetention;
    private final ExpiredEnvelopePolicy expiredEnvelopePolicy;

    private CloudTasksStagedOptions(Builder builder) {
        this.nameRetention = builder.nameRetention;
        this.clockSkewAllowance = builder.clockSkewAllowance;
        this.requestTimeout = builder.requestTimeout;
        this.maxStagedTasks = builder.maxStagedTasks;
        this.maxStagedBytes = builder.maxStagedBytes;
        this.verifyQueueRetention = builder.verifyQueueRetention;
        this.expiredEnvelopePolicy = builder.expiredEnvelopePolicy;
        toStagingConfig();
        Preconditions.checkNotNull(expiredEnvelopePolicy, "expiredEnvelopePolicy must not be null");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CloudTasksStagedOptions)) {
            return false;
        }
        CloudTasksStagedOptions that = (CloudTasksStagedOptions) other;
        return nameRetention.equals(that.nameRetention)
                && clockSkewAllowance.equals(that.clockSkewAllowance)
                && requestTimeout.equals(that.requestTimeout)
                && maxStagedTasks == that.maxStagedTasks
                && maxStagedBytes == that.maxStagedBytes
                && verifyQueueRetention == that.verifyQueueRetention
                && expiredEnvelopePolicy == that.expiredEnvelopePolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                nameRetention,
                clockSkewAllowance,
                requestTimeout,
                maxStagedTasks,
                maxStagedBytes,
                verifyQueueRetention,
                expiredEnvelopePolicy);
    }

    /** Returns a new builder with the documented staging and recovery defaults. */
    public static Builder builder() {
        return new Builder();
    }

    /** Revalidates the settings and returns the writer's internal staging projection. */
    @Internal
    public CloudTasksStagingConfig toStagingConfig() {
        Preconditions.checkNotNull(expiredEnvelopePolicy, "expiredEnvelopePolicy must not be null");
        return new CloudTasksStagingConfig(
                maxStagedTasks, maxStagedBytes, nameRetention, clockSkewAllowance, requestTimeout);
    }

    /** Returns the assumed queue name-retention period. */
    public Duration getNameRetention() {
        return nameRetention;
    }

    /** Returns the allowed relative writer/committer clock error. */
    public Duration getClockSkewAllowance() {
        return clockSkewAllowance;
    }

    /** Returns the absolute client-side budget for each create RPC. */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /** Returns the maximum records in one writer batch. */
    public int getMaxStagedTasks() {
        return maxStagedTasks;
    }

    /** Returns the maximum accounted bytes in one writer batch. */
    public long getMaxStagedBytes() {
        return maxStagedBytes;
    }

    /** Returns whether to read back queue retention before committing. */
    public boolean isVerifyQueueRetention() {
        return verifyQueueRetention;
    }

    /** Returns the operator decision for expired envelopes. */
    public ExpiredEnvelopePolicy getExpiredEnvelopePolicy() {
        return expiredEnvelopePolicy;
    }

    /** Builder for immutable checkpointed creation settings. */
    @Experimental
    public static final class Builder {
        private Duration nameRetention = Duration.ofHours(1);
        private Duration clockSkewAllowance = Duration.ofMinutes(5);
        private Duration requestTimeout = Duration.ofSeconds(20);
        private int maxStagedTasks = 100_000;
        private long maxStagedBytes = 64L * 1024 * 1024;
        private boolean verifyQueueRetention = true;
        private ExpiredEnvelopePolicy expiredEnvelopePolicy = ExpiredEnvelopePolicy.FAIL;

        private Builder() {}

        /**
         * Sets the assumed queue name-retention period.
         *
         * @param nameRetention the assumed queue name-retention period; at most
         *     Duration.ofNanos(Long.MAX_VALUE) (about 292 years)
         * @return this builder
         */
        public Builder nameRetention(Duration nameRetention) {
            Preconditions.checkNotNull(nameRetention, "nameRetention must not be null");
            Preconditions.checkArgument(
                    !nameRetention.isNegative() && !nameRetention.isZero(),
                    "nameRetention must be positive");
            OptionChecks.checkExpressibleInNanos(nameRetention, "nameRetention");
            this.nameRetention = nameRetention;
            return this;
        }

        /**
         * Sets the allowed relative writer/committer clock error.
         *
         * @param clockSkewAllowance the allowed relative writer/committer clock error; at most
         *     Duration.ofNanos(Long.MAX_VALUE) (about 292 years)
         * @return this builder
         */
        public Builder clockSkewAllowance(Duration clockSkewAllowance) {
            Preconditions.checkNotNull(clockSkewAllowance, "clockSkewAllowance must not be null");
            Preconditions.checkArgument(
                    !clockSkewAllowance.isNegative(), "clockSkewAllowance must be non-negative");
            OptionChecks.checkExpressibleInNanos(clockSkewAllowance, "clockSkewAllowance");
            this.clockSkewAllowance = clockSkewAllowance;
            return this;
        }

        /**
         * Sets the absolute client-side budget for each create RPC.
         *
         * @param requestTimeout the absolute client-side budget for each create RPC; at most
         *     Duration.ofNanos(Long.MAX_VALUE) (about 292 years)
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            Preconditions.checkNotNull(requestTimeout, "requestTimeout must not be null");
            Preconditions.checkArgument(
                    !requestTimeout.isNegative() && !requestTimeout.isZero(),
                    "requestTimeout must be positive");
            OptionChecks.checkExpressibleInNanos(requestTimeout, "requestTimeout");
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Sets the maximum records in one writer batch.
         *
         * @param maxStagedTasks the maximum records in one writer batch
         * @return this builder
         */
        public Builder maxStagedTasks(int maxStagedTasks) {
            Preconditions.checkArgument(maxStagedTasks > 0, "maxStagedTasks must be positive");
            this.maxStagedTasks = maxStagedTasks;
            return this;
        }

        /**
         * Sets the maximum accounted bytes in one writer batch.
         *
         * @param maxStagedBytes the maximum accounted bytes in one writer batch
         * @return this builder
         */
        public Builder maxStagedBytes(long maxStagedBytes) {
            Preconditions.checkArgument(maxStagedBytes > 0, "maxStagedBytes must be positive");
            this.maxStagedBytes = maxStagedBytes;
            return this;
        }

        /**
         * Sets whether to read back queue retention before committing.
         *
         * @param verifyQueueRetention whether to read back queue retention before committing
         * @return this builder
         */
        public Builder verifyQueueRetention(boolean verifyQueueRetention) {
            this.verifyQueueRetention = verifyQueueRetention;
            return this;
        }

        /**
         * Sets the operator decision for expired envelopes.
         *
         * @param expiredEnvelopePolicy the operator decision for expired envelopes
         * @return this builder
         */
        public Builder expiredEnvelopePolicy(ExpiredEnvelopePolicy expiredEnvelopePolicy) {
            Preconditions.checkNotNull(
                    expiredEnvelopePolicy, "expiredEnvelopePolicy must not be null");
            this.expiredEnvelopePolicy = expiredEnvelopePolicy;
            return this;
        }

        /** Builds validated options; retention must exceed skew plus request timeout. */
        public CloudTasksStagedOptions build() {
            return new CloudTasksStagedOptions(this);
        }
    }
}
