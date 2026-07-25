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

package io.github.flink.gcp.connector.pubsub.source.subscriptions;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/**
 * The settings of an existing subscription that the source's preflight reads.
 *
 * <p>Deliberately not the Pub/Sub {@code Subscription} message: this carries only the five facts
 * the preflight acts on, which keeps the admin SPI free of protobuf and makes it obvious — from the
 * type alone — what a change to the preflight is allowed to depend on.
 */
@Internal
public final class SubscriptionInfo {

    private final boolean messageOrderingEnabled;
    private final boolean exactlyOnceDeliveryEnabled;
    private final boolean retainAckedMessages;
    private final boolean deadLetterPolicyConfigured;
    private final boolean topicMessageRetentionConfigured;

    private SubscriptionInfo(Builder builder) {
        this.messageOrderingEnabled = builder.messageOrderingEnabled;
        this.exactlyOnceDeliveryEnabled = builder.exactlyOnceDeliveryEnabled;
        this.retainAckedMessages = builder.retainAckedMessages;
        this.deadLetterPolicyConfigured = builder.deadLetterPolicyConfigured;
        this.topicMessageRetentionConfigured = builder.topicMessageRetentionConfigured;
    }

    /** Returns a builder; every flag defaults to {@code false}. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns whether the subscription preserves ordering-key order, which {@link
     * io.github.flink.gcp.connector.pubsub.source.OrderingMode#PER_KEY} requires.
     */
    public boolean isMessageOrderingEnabled() {
        return messageOrderingEnabled;
    }

    /**
     * Returns whether the subscription has exactly-once delivery enabled, which the source cannot
     * consume: acknowledgement ids are invalidated on redelivery and expire with the
     * acknowledgement deadline, while the source holds them for a whole checkpoint interval.
     */
    public boolean isExactlyOnceDeliveryEnabled() {
        return exactlyOnceDeliveryEnabled;
    }

    /**
     * Returns whether the subscription retains acknowledged messages, which a backwards seek needs
     * in order to replay anything already consumed.
     */
    public boolean isRetainAckedMessages() {
        return retainAckedMessages;
    }

    /**
     * Returns whether the subscription has a dead-letter policy, which bounds redelivery of a
     * message a nacking failure policy would otherwise return forever.
     */
    public boolean isDeadLetterPolicyConfigured() {
        return deadLetterPolicyConfigured;
    }

    /**
     * Returns whether the subscription's topic retains messages. Topic retention lets a backwards
     * seek reach messages older than the subscription's own state, so it rescues a replay that
     * {@link #isRetainAckedMessages()} alone would not.
     */
    public boolean isTopicMessageRetentionConfigured() {
        return topicMessageRetentionConfigured;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionInfo that = (SubscriptionInfo) o;
        return messageOrderingEnabled == that.messageOrderingEnabled
                && exactlyOnceDeliveryEnabled == that.exactlyOnceDeliveryEnabled
                && retainAckedMessages == that.retainAckedMessages
                && deadLetterPolicyConfigured == that.deadLetterPolicyConfigured
                && topicMessageRetentionConfigured == that.topicMessageRetentionConfigured;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageOrderingEnabled,
                exactlyOnceDeliveryEnabled,
                retainAckedMessages,
                deadLetterPolicyConfigured,
                topicMessageRetentionConfigured);
    }

    @Override
    public String toString() {
        return "SubscriptionInfo{messageOrderingEnabled="
                + messageOrderingEnabled
                + ", exactlyOnceDeliveryEnabled="
                + exactlyOnceDeliveryEnabled
                + ", retainAckedMessages="
                + retainAckedMessages
                + ", deadLetterPolicyConfigured="
                + deadLetterPolicyConfigured
                + ", topicMessageRetentionConfigured="
                + topicMessageRetentionConfigured
                + "}";
    }

    /** Builder for {@link SubscriptionInfo}. */
    @Internal
    public static final class Builder {

        private boolean messageOrderingEnabled;
        private boolean exactlyOnceDeliveryEnabled;
        private boolean retainAckedMessages;
        private boolean deadLetterPolicyConfigured;
        private boolean topicMessageRetentionConfigured;

        private Builder() {}

        /**
         * Sets whether the subscription preserves ordering-key order.
         *
         * @param messageOrderingEnabled the subscription's {@code enableMessageOrdering}
         * @return this builder
         */
        public Builder messageOrderingEnabled(boolean messageOrderingEnabled) {
            this.messageOrderingEnabled = messageOrderingEnabled;
            return this;
        }

        /**
         * Sets whether the subscription has exactly-once delivery enabled.
         *
         * @param exactlyOnceDeliveryEnabled the subscription's {@code enableExactlyOnceDelivery}
         * @return this builder
         */
        public Builder exactlyOnceDeliveryEnabled(boolean exactlyOnceDeliveryEnabled) {
            this.exactlyOnceDeliveryEnabled = exactlyOnceDeliveryEnabled;
            return this;
        }

        /**
         * Sets whether the subscription retains acknowledged messages.
         *
         * @param retainAckedMessages the subscription's {@code retainAckedMessages}
         * @return this builder
         */
        public Builder retainAckedMessages(boolean retainAckedMessages) {
            this.retainAckedMessages = retainAckedMessages;
            return this;
        }

        /**
         * Sets whether the subscription has a dead-letter policy.
         *
         * @param deadLetterPolicyConfigured whether {@code deadLetterPolicy} is present
         * @return this builder
         */
        public Builder deadLetterPolicyConfigured(boolean deadLetterPolicyConfigured) {
            this.deadLetterPolicyConfigured = deadLetterPolicyConfigured;
            return this;
        }

        /**
         * Sets whether the subscription's topic retains messages. Output-only on {@code
         * GetSubscription}: it reflects the topic's setting, not the subscription's.
         *
         * @param topicMessageRetentionConfigured whether {@code topicMessageRetentionDuration} is
         *     present
         * @return this builder
         */
        public Builder topicMessageRetentionConfigured(boolean topicMessageRetentionConfigured) {
            this.topicMessageRetentionConfigured = topicMessageRetentionConfigured;
            return this;
        }

        /**
         * Builds the settings.
         *
         * @return the settings
         */
        public SubscriptionInfo build() {
            return new SubscriptionInfo(this);
        }
    }
}
