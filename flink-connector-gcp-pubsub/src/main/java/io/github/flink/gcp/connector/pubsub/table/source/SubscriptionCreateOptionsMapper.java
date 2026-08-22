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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.table.OptionSetters;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Builds per-subscription {@link SubscriptionCreateOptions} from the table options.
 *
 * <p>Under the same contract as {@code SubscriberOptionsMapper}: every knob is applied through
 * {@link OptionSetters}, no default is introduced, and each bound stays in the builder — a value it
 * rejects is renamed to the option key (issue #1030). What is different is that three of this
 * options object's setters do not take a {@code ConfigOption}'s shape, so the rules below exist
 * here and nowhere else.
 *
 * <p><b>Auto-creation is authorized by {@code scan.auto-create.topics}.</b> Its map keys must match
 * the {@code subscription} list exactly, and each value supplies that subscription's topic binding.
 * The remaining creation settings apply to every map entry. Keeping only the topic binding per
 * subscription avoids sharing one binding across subscriptions, which would make Pub/Sub deliver a
 * complete copy of one topic's stream through each of them.
 *
 * <p><b>{@code expiration-ttl} and {@code never-expire} are rejected together here, and only
 * here.</b> On the builder they are last-writer-wins — each setter clears the other — which is
 * sensible for a call sequence and meaningless for a {@code WITH} clause, where the two keys carry
 * no order. There is no builder exception to defer to, so this check is the only thing between a
 * contradictory DDL and subscriptions created with whichever value the mapper happened to read
 * last.
 *
 * <p><b>The dead-letter options are required together.</b> Defaulting the attempt count here would
 * put back the third state the option design removes: absent, and set to something nobody chose.
 */
@Internal
public final class SubscriptionCreateOptionsMapper {

    /** Options that have no meaning unless the topic map authorizes subscription creation. */
    private static final List<ConfigOption<?>> REQUIRE_TOPICS =
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
     * Maps the table options onto the settings each missing subscription is created with.
     *
     * @param config the table options
     * @return creation settings keyed by subscription, or an empty map when every subscription is
     *     required to exist already
     */
    public static Map<SubscriptionDestination, SubscriptionCreateOptions> map(
            ReadableConfig config) {
        Optional<Map<String, String>> configuredTopics =
                config.getOptional(PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS);
        if (!configuredTopics.isPresent()) {
            checkNoOrphanedOptions(config);
            return Collections.emptyMap();
        }

        Map<String, String> topics = configuredTopics.get();
        if (topics.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must map at least one subscription to a topic.",
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS.key()));
        }

        List<String> subscriptions =
                config.getOptional(PubSubConnectorOptions.SUBSCRIPTION)
                        .orElse(Collections.emptyList());
        checkTopicKeys(subscriptions, topics);

        String project = config.get(PubSubConnectorOptions.PROJECT);
        Map<SubscriptionDestination, SubscriptionCreateOptions> mapped = new LinkedHashMap<>();
        for (String subscription : subscriptions) {
            SubscriptionCreateOptions.Builder builder =
                    SubscriptionCreateOptions.builder()
                            .topic(
                                    OptionSetters.convert(
                                            PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS.key(),
                                            topics.get(subscription),
                                            value ->
                                                    topicDestination(
                                                            project, subscription, value)));
            applySharedSettings(config, project, builder);
            mapped.put(SubscriptionDestination.of(project, subscription), builder.build());
        }
        return Collections.unmodifiableMap(mapped);
    }

    private static void checkNoOrphanedOptions(ReadableConfig config) {
        List<String> orphaned = new ArrayList<>();
        for (ConfigOption<?> option : REQUIRE_TOPICS) {
            if (config.getOptional(option).isPresent()) {
                orphaned.add(option.key());
            }
        }
        if (!orphaned.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Options %s configure subscriptions this table never creates, because"
                                    + " '%s' is not set. Setting that option is what authorizes"
                                    + " creating missing subscriptions; without it every"
                                    + " subscription must already exist, and existing ones keep"
                                    + " their own settings.",
                            orphaned, PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS.key()));
        }
    }

    private static void checkTopicKeys(List<String> subscriptions, Map<String, String> topics) {
        Set<String> expected = new LinkedHashSet<>(subscriptions);
        Set<String> missing = new TreeSet<>(expected);
        missing.removeAll(topics.keySet());
        Set<String> unexpected = new TreeSet<>(topics.keySet());
        unexpected.removeAll(expected);
        if (!missing.isEmpty() || !unexpected.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must have exactly the subscription names in option '%s'"
                                    + " as its keys. Missing keys: %s. Unexpected keys: %s.",
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS.key(),
                            PubSubConnectorOptions.SUBSCRIPTION.key(),
                            missing,
                            unexpected));
        }
    }

    private static void applySharedSettings(
            ReadableConfig config, String project, SubscriptionCreateOptions.Builder builder) {
        OptionSetters.apply(
                config, PubSubConnectorOptions.SCAN_AUTO_CREATE_ACK_DEADLINE, builder::ackDeadline);
        OptionSetters.apply(
                config,
                PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED,
                builder::enableMessageOrdering);
        OptionSetters.apply(
                config,
                PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_RETENTION,
                builder::messageRetention);
        OptionSetters.apply(
                config,
                PubSubConnectorOptions.SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES,
                builder::retainAckedMessages);
        OptionSetters.apply(
                config, PubSubConnectorOptions.SCAN_AUTO_CREATE_FILTER, builder::filter);
        applyExpiration(config, builder);
        applyDeadLetterPolicy(config, project, builder);
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
        OptionSetters.accept(
                PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL.key(),
                ttl.orElse(null),
                builder::expirationTtl);
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
            TopicDestination deadLetterTopic =
                    OptionSetters.convert(
                            PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC.key(),
                            topic.get(),
                            value -> TopicDestination.of(project, value));
            OptionSetters.accept(
                    PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS.key(),
                    maxDeliveryAttempts.get(),
                    attempts -> builder.deadLetterPolicy(deadLetterTopic, attempts));
        }
    }

    /**
     * Builds the topic binding, naming the map entry a rejection belongs to: the base key is what a
     * packed DDL wrote and the prefix of what a prefixed DDL wrote, so the entry name is the half
     * both spellings need.
     */
    private static TopicDestination topicDestination(
            String project, String subscription, String topic) {
        try {
            return TopicDestination.of(project, topic);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("entry '%s': %s", subscription, e.getMessage()), e);
        }
    }
}
