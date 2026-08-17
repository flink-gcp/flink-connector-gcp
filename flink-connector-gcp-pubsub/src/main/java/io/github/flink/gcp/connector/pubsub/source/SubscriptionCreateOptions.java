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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Settings the source applies when it creates a subscription that does not exist.
 *
 * <p><b>Supplying these options for a subscription is what authorises creating it.</b> There is no
 * separate disposition enum, because there is no meaningful "create with defaults": a subscription
 * without a topic is not a subscription, and only the user knows which topic to bind. A
 * subscription passed to {@link PubSubSourceBuilder#subscription(SubscriptionDestination)} without
 * options must therefore already exist, and the source fails at startup if it does not.
 *
 * <p>Options are per subscription, and the topic binding is why. One options object shared by
 * several subscriptions would bind them all to the same topic, and Pub/Sub delivers a complete copy
 * of the stream to every subscription of a topic — so the source would emit each message once per
 * subscription, with nothing anywhere reporting an error.
 *
 * <p>Options only affect creation. A subscription that already exists is used exactly as it is
 * configured, and these settings are neither applied to it nor compared against it (except for the
 * two the source cannot work around, which the startup check rejects: ordering under {@link
 * OrderingMode#PER_KEY}, and exactly-once delivery).
 *
 * <p>Every knob but the topic is optional and unset means absent, leaving Pub/Sub's own default.
 * Values are validated here only where the failure would otherwise be silent or obscure; documented
 * service ranges (an acknowledgement deadline of 10-600 seconds, for example) are left to Pub/Sub,
 * whose rejection already names the field and the limit.
 */
@Public
public final class SubscriptionCreateOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TopicDestination topic;
    @Nullable private final Duration ackDeadline;
    private final boolean enableMessageOrdering;
    @Nullable private final Duration messageRetention;
    private final boolean retainAckedMessages;
    @Nullable private final Duration expirationTtl;
    private final boolean neverExpire;
    @Nullable private final TopicDestination deadLetterTopic;
    private final int deadLetterMaxDeliveryAttempts;
    @Nullable private final String filter;

    private SubscriptionCreateOptions(Builder builder) {
        this.topic = builder.topic;
        this.ackDeadline = builder.ackDeadline;
        this.enableMessageOrdering = builder.enableMessageOrdering;
        this.messageRetention = builder.messageRetention;
        this.retainAckedMessages = builder.retainAckedMessages;
        this.expirationTtl = builder.expirationTtl;
        this.neverExpire = builder.neverExpire;
        this.deadLetterTopic = builder.deadLetterTopic;
        this.deadLetterMaxDeliveryAttempts = builder.deadLetterMaxDeliveryAttempts;
        this.filter = builder.filter;
    }

    /** Returns a builder. The topic is required; everything else is optional. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the topic the subscription is created against. */
    public TopicDestination getTopic() {
        return topic;
    }

    /** Returns the acknowledgement deadline, or {@code null} for the Pub/Sub default (10 s). */
    @Nullable
    public Duration getAckDeadline() {
        return ackDeadline;
    }

    /** Returns whether the subscription is created with ordering-key ordering enabled. */
    public boolean isEnableMessageOrdering() {
        return enableMessageOrdering;
    }

    /** Returns how long messages are retained, or {@code null} for the Pub/Sub default (7 days). */
    @Nullable
    public Duration getMessageRetention() {
        return messageRetention;
    }

    /** Returns whether acknowledged messages are retained for replay. */
    public boolean isRetainAckedMessages() {
        return retainAckedMessages;
    }

    /**
     * Returns how long the subscription may sit inactive before Pub/Sub deletes it, or {@code null}
     * when the default (31 days) applies or when expiration is disabled — see {@link
     * #isNeverExpire()}.
     */
    @Nullable
    public Duration getExpirationTtl() {
        return expirationTtl;
    }

    /** Returns whether the subscription is created never to expire. */
    public boolean isNeverExpire() {
        return neverExpire;
    }

    /**
     * Returns the topic undeliverable messages are forwarded to, or {@code null} when no
     * dead-letter policy is configured.
     */
    @Nullable
    public TopicDestination getDeadLetterTopic() {
        return deadLetterTopic;
    }

    /**
     * Returns how many delivery attempts a message gets before it is dead-lettered, or {@code 0}
     * when no dead-letter policy is configured.
     */
    public int getDeadLetterMaxDeliveryAttempts() {
        return deadLetterMaxDeliveryAttempts;
    }

    /** Returns the subscription filter expression, or {@code null} when unfiltered. */
    @Nullable
    public String getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionCreateOptions that = (SubscriptionCreateOptions) o;
        return enableMessageOrdering == that.enableMessageOrdering
                && retainAckedMessages == that.retainAckedMessages
                && neverExpire == that.neverExpire
                && deadLetterMaxDeliveryAttempts == that.deadLetterMaxDeliveryAttempts
                && topic.equals(that.topic)
                && Objects.equals(ackDeadline, that.ackDeadline)
                && Objects.equals(messageRetention, that.messageRetention)
                && Objects.equals(expirationTtl, that.expirationTtl)
                && Objects.equals(deadLetterTopic, that.deadLetterTopic)
                && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topic,
                ackDeadline,
                enableMessageOrdering,
                messageRetention,
                retainAckedMessages,
                expirationTtl,
                neverExpire,
                deadLetterTopic,
                deadLetterMaxDeliveryAttempts,
                filter);
    }

    @Override
    public String toString() {
        return "SubscriptionCreateOptions{topic="
                + topic
                + ", ackDeadline="
                + ackDeadline
                + ", enableMessageOrdering="
                + enableMessageOrdering
                + ", messageRetention="
                + messageRetention
                + ", retainAckedMessages="
                + retainAckedMessages
                + ", expirationTtl="
                + expirationTtl
                + ", neverExpire="
                + neverExpire
                + ", deadLetterTopic="
                + deadLetterTopic
                + ", deadLetterMaxDeliveryAttempts="
                + deadLetterMaxDeliveryAttempts
                + ", filter="
                + filter
                + "}";
    }

    /** Builder for {@link SubscriptionCreateOptions}. */
    @Public
    public static final class Builder {

        private TopicDestination topic;
        @Nullable private Duration ackDeadline;
        private boolean enableMessageOrdering;
        @Nullable private Duration messageRetention;
        private boolean retainAckedMessages;
        @Nullable private Duration expirationTtl;
        private boolean neverExpire;
        @Nullable private TopicDestination deadLetterTopic;
        private int deadLetterMaxDeliveryAttempts;
        @Nullable private String filter;

        private Builder() {}

        /**
         * Sets the topic the subscription is created against. Required — a subscription cannot
         * exist without one. The topic may live in a different project from the subscription.
         *
         * @param topic the topic to bind
         * @return this builder
         */
        public Builder topic(TopicDestination topic) {
            this.topic = Preconditions.checkNotNull(topic, "topic must not be null");
            return this;
        }

        /**
         * Sets how long a consumer has to acknowledge a message before Pub/Sub redelivers it.
         * Pub/Sub stores this in whole seconds, so the duration must be a whole number of seconds —
         * a sub-second remainder would be silently dropped. Defaults to the Pub/Sub default (10 s).
         *
         * <p>This is only the starting deadline. The client library extends it while a message is
         * outstanding, up to {@link
         * PubSubSubscriberOptions.Builder#maxAckExtensionPeriod(Duration)}, which is what actually
         * has to cover the source's checkpoint interval.
         *
         * @param ackDeadline the acknowledgement deadline
         * @return this builder
         */
        public Builder ackDeadline(Duration ackDeadline) {
            OptionChecks.checkPositive(ackDeadline, "ackDeadline");
            Preconditions.checkArgument(
                    ackDeadline.getNano() == 0,
                    "ackDeadline must be a whole number of seconds, but was %s; Pub/Sub stores it"
                            + " in seconds and the remainder would be dropped.",
                    ackDeadline);
            this.ackDeadline = ackDeadline;
            return this;
        }

        /**
         * Creates the subscription with ordering-key ordering enabled. Required by {@link
         * OrderingMode#PER_KEY}, which the source builder checks. Cannot be turned on later: a
         * subscription's ordering setting is fixed at creation.
         *
         * @param enableMessageOrdering whether to preserve ordering-key order
         * @return this builder
         */
        public Builder enableMessageOrdering(boolean enableMessageOrdering) {
            this.enableMessageOrdering = enableMessageOrdering;
            return this;
        }

        /**
         * Sets how long unacknowledged messages are retained. Defaults to the Pub/Sub default (7
         * days). Together with {@link #retainAckedMessages(boolean)} this bounds how far back a
         * backwards {@link StartPosition} can reach.
         *
         * @param messageRetention the retention duration
         * @return this builder
         */
        public Builder messageRetention(Duration messageRetention) {
            OptionChecks.checkPositive(messageRetention, "messageRetention");
            this.messageRetention = messageRetention;
            return this;
        }

        /**
         * Retains messages after they are acknowledged, so a backwards seek can replay them.
         * Without this, a seek into the past only recovers messages that were never acknowledged.
         *
         * @param retainAckedMessages whether to retain acknowledged messages
         * @return this builder
         */
        public Builder retainAckedMessages(boolean retainAckedMessages) {
            this.retainAckedMessages = retainAckedMessages;
            return this;
        }

        /**
         * Sets how long the subscription may sit inactive before Pub/Sub deletes it. Defaults to
         * the Pub/Sub default (31 days). A running job keeps its subscriptions active; this matters
         * for one that is stopped for longer than the TTL. Clears any previous {@link
         * #neverExpire()}.
         *
         * @param expirationTtl the inactivity TTL
         * @return this builder
         */
        public Builder expirationTtl(Duration expirationTtl) {
            OptionChecks.checkPositive(expirationTtl, "expirationTtl");
            this.expirationTtl = expirationTtl;
            this.neverExpire = false;
            return this;
        }

        /**
         * Creates the subscription so that it never expires, however long it sits inactive. Clears
         * any previous {@link #expirationTtl(Duration)}.
         *
         * @return this builder
         */
        public Builder neverExpire() {
            this.neverExpire = true;
            this.expirationTtl = null;
            return this;
        }

        /**
         * Forwards a message to the given topic once it has been delivered {@code
         * maxDeliveryAttempts} times without being acknowledged.
         *
         * <p><b>Dead-lettering counts deliveries, not causes.</b> A redelivery after a job restart
         * raises the same counter as one after a nack, so a low attempt limit on a job that
         * restarts repeatedly dead-letters healthy messages.
         *
         * <p>Pub/Sub also needs its own service account granted publish on the dead-letter topic
         * and subscribe on this subscription; without those grants it silently keeps redelivering.
         *
         * @param deadLetterTopic the topic undeliverable messages are forwarded to
         * @param maxDeliveryAttempts delivery attempts before forwarding (Pub/Sub accepts 5 to 100)
         * @return this builder
         */
        public Builder deadLetterPolicy(TopicDestination deadLetterTopic, int maxDeliveryAttempts) {
            Preconditions.checkNotNull(deadLetterTopic, "deadLetterTopic must not be null");
            Preconditions.checkArgument(
                    maxDeliveryAttempts > 0,
                    "maxDeliveryAttempts must be positive, but was %s",
                    maxDeliveryAttempts);
            this.deadLetterTopic = deadLetterTopic;
            this.deadLetterMaxDeliveryAttempts = maxDeliveryAttempts;
            return this;
        }

        /**
         * Sets a filter expression, so Pub/Sub delivers only matching messages and acknowledges the
         * rest on the subscription's behalf. Fixed at creation: a subscription's filter cannot be
         * changed later.
         *
         * @param filter the filter expression, in Pub/Sub's filtering syntax
         * @return this builder
         */
        public Builder filter(String filter) {
            Preconditions.checkNotNull(filter, "filter must not be null");
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(filter), "filter must not be blank");
            this.filter = filter;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public SubscriptionCreateOptions build() {
            Preconditions.checkState(
                    topic != null,
                    "topic(...) is required: a subscription cannot be created without a topic to"
                            + " bind it to.");
            return new SubscriptionCreateOptions(this);
        }
    }
}
