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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link SubscriptionCreateOptionsMapper}. */
class SubscriptionCreateOptionsMapperTest {

    /** Every builder setter and the option or options that feed it. */
    private static final Map<String, List<ConfigOption<?>>> SETTER_TO_OPTIONS =
            new LinkedHashMap<>();

    static {
        SETTER_TO_OPTIONS.put(
                "topic", Collections.singletonList(PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS));
        SETTER_TO_OPTIONS.put(
                "ackDeadline",
                Collections.singletonList(PubSubConnectorOptions.SCAN_AUTO_CREATE_ACK_DEADLINE));
        SETTER_TO_OPTIONS.put(
                "enableMessageOrdering",
                Collections.singletonList(
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED));
        SETTER_TO_OPTIONS.put(
                "messageRetention",
                Collections.singletonList(
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_RETENTION));
        SETTER_TO_OPTIONS.put(
                "retainAckedMessages",
                Collections.singletonList(
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES));
        SETTER_TO_OPTIONS.put(
                "expirationTtl",
                Collections.singletonList(PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL));
        SETTER_TO_OPTIONS.put(
                "neverExpire",
                Collections.singletonList(PubSubConnectorOptions.SCAN_AUTO_CREATE_NEVER_EXPIRE));
        SETTER_TO_OPTIONS.put(
                "deadLetterPolicy",
                Arrays.asList(
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS));
        SETTER_TO_OPTIONS.put(
                "filter",
                Collections.singletonList(PubSubConnectorOptions.SCAN_AUTO_CREATE_FILTER));
    }

    @Test
    void everyCreationKnobHasAnOption() {
        Set<String> setters =
                Arrays.stream(SubscriptionCreateOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == SubscriptionCreateOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        assertThat(setters).isEqualTo(SETTER_TO_OPTIONS.keySet());
    }

    @Test
    void noOptionFeedsTwoSetters() {
        List<String> keys =
                SETTER_TO_OPTIONS.values().stream()
                        .flatMap(List::stream)
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());

        // Ten: every scan.auto-create.* option. The scan.startup.* pair is the other mapper's.
        assertThat(keys).doesNotHaveDuplicates();
        assertThat(new HashSet<>(keys)).hasSize(10);
    }

    private static String key(String setter) {
        List<ConfigOption<?>> options = SETTER_TO_OPTIONS.get(setter);
        assertThat(options).as("%s is fed by more than one option", setter).hasSize(1);
        return options.get(0).key();
    }

    private static String key(String setter, int index) {
        return SETTER_TO_OPTIONS.get(setter).get(index).key();
    }

    private static String topicKey(String subscription) {
        return key("topic") + "." + subscription;
    }

    /** A configuration with the two options every source needs, plus whatever is added to it. */
    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(PubSubConnectorOptions.PROJECT.key(), "my-project");
        options.put(PubSubConnectorOptions.SUBSCRIPTION.key(), "my-sub");
        return options;
    }

    private static Map<SubscriptionDestination, SubscriptionCreateOptions> map(
            Map<String, String> options) {
        return SubscriptionCreateOptionsMapper.map(Configuration.fromMap(options));
    }

    private static SubscriptionCreateOptions onlyValue(
            Map<SubscriptionDestination, SubscriptionCreateOptions> mapped) {
        assertThat(mapped).hasSize(1);
        return mapped.values().iterator().next();
    }

    @Test
    void anAbsentTopicMapLeavesEverySubscriptionRequiredToExist() {
        assertThat(map(baseOptions())).isEmpty();
        assertThat(SubscriptionCreateOptionsMapper.map(new Configuration())).isEmpty();
    }

    @Test
    void aSinglePrefixEntryCreatesWithNothingButItsBinding() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");

        assertThat(map(options))
                .containsExactly(
                        entry(
                                SubscriptionDestination.of("my-project", "my-sub"),
                                SubscriptionCreateOptions.builder()
                                        .topic(TopicDestination.of("my-project", "my-topic"))
                                        .build()));
    }

    @Test
    void packedMapSyntaxSeparatesSeveralEntriesWithCommas() {
        Map<String, String> options = baseOptions();
        options.put(PubSubConnectorOptions.SUBSCRIPTION.key(), "orders;refunds");
        options.put(key("topic"), "orders:orders-topic,refunds:refunds-topic");

        Map<SubscriptionDestination, SubscriptionCreateOptions> mapped = map(options);

        assertThat(mapped)
                .extractingByKey(SubscriptionDestination.of("my-project", "orders"))
                .extracting(SubscriptionCreateOptions::getTopic)
                .isEqualTo(TopicDestination.of("my-project", "orders-topic"));
        assertThat(mapped)
                .extractingByKey(SubscriptionDestination.of("my-project", "refunds"))
                .extracting(SubscriptionCreateOptions::getTopic)
                .isEqualTo(TopicDestination.of("my-project", "refunds-topic"));
    }

    @Test
    void eachSubscriptionGetsItsOwnTopicAndTheSharedSettings() {
        Map<String, String> options = baseOptions();
        options.put(PubSubConnectorOptions.SUBSCRIPTION.key(), "orders;refunds");
        options.put(topicKey("orders"), "orders-topic");
        options.put(topicKey("refunds"), "refunds-topic");
        options.put(key("ackDeadline"), "60 s");
        options.put(key("filter"), "attributes.kind = \"event\"");

        Map<SubscriptionDestination, SubscriptionCreateOptions> mapped = map(options);

        assertThat(mapped)
                .extractingByKey(SubscriptionDestination.of("my-project", "orders"))
                .satisfies(
                        creation -> {
                            assertThat(creation.getTopic())
                                    .isEqualTo(TopicDestination.of("my-project", "orders-topic"));
                            assertThat(creation.getAckDeadline()).isEqualTo(Duration.ofSeconds(60));
                            assertThat(creation.getFilter())
                                    .isEqualTo("attributes.kind = \"event\"");
                        });
        assertThat(mapped)
                .extractingByKey(SubscriptionDestination.of("my-project", "refunds"))
                .satisfies(
                        creation -> {
                            assertThat(creation.getTopic())
                                    .isEqualTo(TopicDestination.of("my-project", "refunds-topic"));
                            assertThat(creation.getAckDeadline()).isEqualTo(Duration.ofSeconds(60));
                            assertThat(creation.getFilter())
                                    .isEqualTo("attributes.kind = \"event\"");
                        });
    }

    @Test
    void resolvesBothTopicNamesAgainstTheProject() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("deadLetterPolicy", 0), "my-dlq");
        options.put(key("deadLetterPolicy", 1), "7");

        SubscriptionCreateOptions mapped = onlyValue(map(options));

        assertThat(mapped.getTopic()).isEqualTo(TopicDestination.of("my-project", "my-topic"));
        assertThat(mapped.getDeadLetterTopic())
                .isEqualTo(TopicDestination.of("my-project", "my-dlq"));
        assertThat(mapped.getDeadLetterMaxDeliveryAttempts()).isEqualTo(7);
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("ackDeadline"), "60 s");
        options.put(key("enableMessageOrdering"), "true");
        options.put(key("messageRetention"), "3 d");
        options.put(key("retainAckedMessages"), "true");
        options.put(key("expirationTtl"), "31 d");
        options.put(key("deadLetterPolicy", 0), "my-dlq");
        options.put(key("deadLetterPolicy", 1), "5");
        options.put(key("filter"), "attributes.kind = \"order\"");

        SubscriptionCreateOptions mapped = onlyValue(map(options));

        assertThat(mapped.getTopic()).isEqualTo(TopicDestination.of("my-project", "my-topic"));
        assertThat(mapped.getAckDeadline()).isEqualTo(Duration.ofSeconds(60));
        assertThat(mapped.isEnableMessageOrdering()).isTrue();
        assertThat(mapped.getMessageRetention()).isEqualTo(Duration.ofDays(3));
        assertThat(mapped.isRetainAckedMessages()).isTrue();
        assertThat(mapped.getExpirationTtl()).isEqualTo(Duration.ofDays(31));
        assertThat(mapped.isNeverExpire()).isFalse();
        assertThat(mapped.getDeadLetterTopic())
                .isEqualTo(TopicDestination.of("my-project", "my-dlq"));
        assertThat(mapped.getDeadLetterMaxDeliveryAttempts()).isEqualTo(5);
        assertThat(mapped.getFilter()).isEqualTo("attributes.kind = \"order\"");
    }

    @Test
    void anOptionLeftOutStaysUnsetRatherThanTakingAValue() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("ackDeadline"), "45 s");

        SubscriptionCreateOptions mapped = onlyValue(map(options));

        assertThat(mapped.getAckDeadline()).isEqualTo(Duration.ofSeconds(45));
        assertThat(mapped.getMessageRetention()).isNull();
        assertThat(mapped.getExpirationTtl()).isNull();
        assertThat(mapped.getFilter()).isNull();
        assertThat(mapped.isEnableMessageOrdering()).isFalse();
        assertThat(mapped.isRetainAckedMessages()).isFalse();
        assertThat(mapped.isNeverExpire()).isFalse();
    }

    @Test
    void neverExpireIsAppliedOnlyWhenItIsTrue() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("neverExpire"), "true");

        assertThat(onlyValue(map(options)).isNeverExpire()).isTrue();

        options.put(key("neverExpire"), "false");

        assertThat(onlyValue(map(options)).isNeverExpire()).isFalse();
    }

    @Test
    void aTtlAlongsideNeverExpireIsRejected() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("expirationTtl"), "31 d");
        options.put(key("neverExpire"), "true");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(key("expirationTtl"))
                .hasMessageContaining(key("neverExpire"));
    }

    @Test
    void aTtlAlongsideAnExplicitlyFalseNeverExpireIsFine() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("expirationTtl"), "31 d");
        options.put(key("neverExpire"), "false");

        assertThat(onlyValue(map(options)).getExpirationTtl()).isEqualTo(Duration.ofDays(31));
    }

    @Test
    void aDeadLetterTopicWithoutAnAttemptCountIsRejected() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("deadLetterPolicy", 0), "my-dlq");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(key("deadLetterPolicy", 1));
    }

    @Test
    void anAttemptCountWithoutADeadLetterTopicIsRejected() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(key("deadLetterPolicy", 1), "5");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(key("deadLetterPolicy", 0));
    }

    @Test
    void aMissingSubscriptionKeyIsRejected() {
        Map<String, String> options = baseOptions();
        options.put(PubSubConnectorOptions.SUBSCRIPTION.key(), "orders;refunds");
        options.put(topicKey("orders"), "orders-topic");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Missing keys: [refunds]")
                .hasMessageContaining("Unexpected keys: []");
    }

    @Test
    void anUnexpectedSubscriptionKeyIsRejected() {
        Map<String, String> options = baseOptions();
        options.put(topicKey("my-sub"), "my-topic");
        options.put(topicKey("other-sub"), "other-topic");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Missing keys: []")
                .hasMessageContaining("Unexpected keys: [other-sub]");
    }

    @Test
    void anExplicitlyEmptyTopicMapIsRejected() {
        Configuration config = new Configuration();
        config.set(PubSubConnectorOptions.PROJECT, "my-project");
        config.set(PubSubConnectorOptions.SUBSCRIPTION, Collections.singletonList("my-sub"));
        config.set(PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS, Collections.emptyMap());

        assertThatThrownBy(() -> SubscriptionCreateOptionsMapper.map(config))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must map at least one subscription");
    }

    @Test
    void aCreationKnobWithoutTopicsIsRejectedRatherThanIgnored() {
        for (Map.Entry<String, List<ConfigOption<?>>> entry : SETTER_TO_OPTIONS.entrySet()) {
            if ("topic".equals(entry.getKey())) {
                continue;
            }
            for (ConfigOption<?> option : entry.getValue()) {
                Map<String, String> options = baseOptions();
                options.put(option.key(), valueFor(option));

                assertThatThrownBy(() -> map(options))
                        .as("'%s' set without auto-create topics", option.key())
                        .isInstanceOf(ValidationException.class)
                        .hasMessageContaining(option.key())
                        .hasMessageContaining(PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS.key());
            }
        }
    }

    /** A parseable value per shared option, kept explicit so a new one needs a decision here. */
    private static final Map<String, String> SAMPLE_VALUES = new HashMap<>();

    static {
        SAMPLE_VALUES.put(PubSubConnectorOptions.SCAN_AUTO_CREATE_ACK_DEADLINE.key(), "60 s");
        SAMPLE_VALUES.put(
                PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED.key(), "true");
        SAMPLE_VALUES.put(PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_RETENTION.key(), "3 d");
        SAMPLE_VALUES.put(
                PubSubConnectorOptions.SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES.key(), "true");
        SAMPLE_VALUES.put(PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL.key(), "31 d");
        SAMPLE_VALUES.put(PubSubConnectorOptions.SCAN_AUTO_CREATE_NEVER_EXPIRE.key(), "true");
        SAMPLE_VALUES.put(
                PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC.key(), "my-dlq");
        SAMPLE_VALUES.put(
                PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS.key(),
                "5");
        SAMPLE_VALUES.put(
                PubSubConnectorOptions.SCAN_AUTO_CREATE_FILTER.key(),
                "attributes.kind = \"order\"");
    }

    private static String valueFor(ConfigOption<?> option) {
        String value = SAMPLE_VALUES.get(option.key());
        assertThat(value).as("no sample value for '%s'", option.key()).isNotNull();
        return value;
    }
}
