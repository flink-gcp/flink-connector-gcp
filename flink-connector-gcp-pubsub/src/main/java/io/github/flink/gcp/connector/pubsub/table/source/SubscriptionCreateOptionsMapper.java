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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Builds {@link SubscriptionCreateOptions} from the table options.
 *
 * <p>Under the same contract as {@code SubscriberOptionsMapper}: every knob is applied with {@code
 * getOptional(...).ifPresent(...)}, no default is introduced, and value validation is left to the
 * builder so a SQL user gets the message a DataStream user gets. What is different is that three of
 * this options object's setters do not take a {@code ConfigOption}'s shape, so the rules below
 * exist here and nowhere else.
 *
 * <p><b>Auto-creation is authorized by {@code scan.auto-create.topic} and requires exactly one
 * subscription.</b> The DataStream API keys creation settings by subscription because they carry
 * the topic binding, and a flat DDL namespace cannot express one object per subscription. Sharing
 * one across several is the hazard the API exists to prevent: Pub/Sub delivers a complete copy of a
 * topic's stream to every subscription of it, so the source would emit each message once per
 * subscription with nothing reporting an error. One precondition makes that inexpressible.
 *
 * <p><b>{@code expiration-ttl} and {@code never-expire} are rejected together here, and only
 * here.</b> On the builder they are last-writer-wins — each setter clears the other — which is
 * sensible for a call sequence and meaningless for a {@code WITH} clause, where the two keys carry
 * no order. There is no builder exception to defer to, so this check is the only thing between a
 * contradictory DDL and a subscription created with whichever value the mapper happened to read
 * last.
 *
 * <p><b>The dead-letter options are required together.</b> Defaulting the attempt count here would
 * put back the third state the option design removes: absent, and set to something nobody chose.
 */
@Internal
public final class SubscriptionCreateOptionsMapper {

    /**
     * The options that only mean anything alongside {@code scan.auto-create.topic}. Set without it,
     * they would be read by nothing at all, so they are rejected rather than ignored.
     */
    private static final List<ConfigOption<?>> REQUIRE_TOPIC =
            Arrays.asList(
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_ACK_DEADLINE,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_RETENTION,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_NEVER_EXPIRE,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_FILTER);

    private SubscriptionCreateOptionsMapper() {}

    /**
     * Maps the table options onto the settings a missing subscription is created with.
     *
     * @param config the table options
     * @return the creation settings, or {@code null} when {@code scan.auto-create.topic} is absent,
     *     which leaves the subscription required to exist already
     */
    @Nullable
    public static SubscriptionCreateOptions map(ReadableConfig config) {
        Optional<String> topic = config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPIC);
        if (!topic.isPresent()) {
            checkNoOrphanedOptions(config);
            return null;
        }
        checkExactlyOneSubscription(config);

        String project = config.get(PubSubConnectorOptions.PROJECT);
        SubscriptionCreateOptions.Builder builder =
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of(project, topic.get()));

        config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_ACK_DEADLINE)
                .ifPresent(builder::ackDeadline);
        config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED)
                .ifPresent(builder::enableMessageOrdering);
        config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_RETENTION)
                .ifPresent(builder::messageRetention);
        config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES)
                .ifPresent(builder::retainAckedMessages);
        config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_FILTER)
                .ifPresent(builder::filter);

        applyExpiration(config, builder);
        applyDeadLetterPolicy(config, project, builder);

        return builder.build();
    }

    private static void checkNoOrphanedOptions(ReadableConfig config) {
        List<String> orphaned = new ArrayList<>();
        for (ConfigOption<?> option : REQUIRE_TOPIC) {
            if (config.getOptional(option).isPresent()) {
                orphaned.add(option.key());
            }
        }
        if (!orphaned.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Options %s configure a subscription this table never creates, because"
                                    + " '%s' is not set. Setting that option is what authorizes"
                                    + " creating a missing subscription; without it the"
                                    + " subscription must already exist, and an existing one keeps"
                                    + " its own settings.",
                            orphaned, PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPIC.key()));
        }
    }

    private static void checkExactlyOneSubscription(ReadableConfig config) {
        List<String> subscriptions =
                config.getOptional(PubSubConnectorOptions.SUBSCRIPTION)
                        .orElse(Collections.emptyList());
        if (subscriptions.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' cannot be combined with several subscriptions, but '%s'"
                                    + " named %s: %s. Creation settings carry the topic binding, so"
                                    + " one set of them would bind every subscription to the same"
                                    + " topic — and Pub/Sub delivers a complete copy of a topic's"
                                    + " stream to each of its subscriptions, so this table would"
                                    + " emit every message once per subscription with nothing"
                                    + " reporting an error. Give each subscription its own table.",
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPIC.key(),
                            PubSubConnectorOptions.SUBSCRIPTION.key(),
                            subscriptions.size(),
                            subscriptions));
        }
    }

    private static void applyExpiration(
            ReadableConfig config, SubscriptionCreateOptions.Builder builder) {
        Optional<Duration> ttl =
                config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL);
        boolean neverExpire =
                config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_NEVER_EXPIRE)
                        .orElse(false);
        if (ttl.isPresent() && neverExpire) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' = 'true' contradict each other: a subscription"
                                    + " either expires after an idle period or never expires. Set"
                                    + " one of them.",
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL.key(),
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_NEVER_EXPIRE.key()));
        }
        ttl.ifPresent(builder::expirationTtl);
        if (neverExpire) {
            // Only 'true' calls the setter: it takes no argument, so 'false' can only mean "leave
            // the expiration alone", which is what not calling it does.
            builder.neverExpire();
        }
    }

    private static void applyDeadLetterPolicy(
            ReadableConfig config, String project, SubscriptionCreateOptions.Builder builder) {
        Optional<String> topic =
                config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC);
        Optional<Integer> maxDeliveryAttempts =
                config.getOptional(
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS);
        if (topic.isPresent() != maxDeliveryAttempts.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' are required together, but only '%s' was set. A"
                                    + " dead-letter policy is one setting with two halves, and"
                                    + " choosing an attempt count on your behalf would be a"
                                    + " redelivery limit nobody picked.",
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC.key(),
                            PubSubConnectorOptions
                                    .SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS
                                    .key(),
                            topic.isPresent()
                                    ? PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC
                                            .key()
                                    : PubSubConnectorOptions
                                            .SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS
                                            .key()));
        }
        if (topic.isPresent()) {
            builder.deadLetterPolicy(
                    TopicDestination.of(project, topic.get()), maxDeliveryAttempts.get());
        }
    }
}
