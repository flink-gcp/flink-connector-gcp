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

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubscriptionCreateOptions}. */
class SubscriptionCreateOptionsTest {

    private static final TopicDestination TOPIC = TopicDestination.of("project", "topic");
    private static final TopicDestination DEAD_LETTER =
            TopicDestination.of("project", "dead-letter");

    /** Options with every knob set, for round-trip and equality tests. */
    static SubscriptionCreateOptions fullyPopulated() {
        return SubscriptionCreateOptions.builder()
                .topic(TOPIC)
                .ackDeadline(Duration.ofSeconds(30))
                .enableMessageOrdering(true)
                .messageRetention(Duration.ofHours(12))
                .retainAckedMessages(true)
                .expirationTtl(Duration.ofDays(2))
                .deadLetterPolicy(DEAD_LETTER, 10)
                .filter("attributes.kind = \"order\"")
                .build();
    }

    @Test
    void everyKnobButTheTopicIsUnsetByDefault() {
        SubscriptionCreateOptions options =
                SubscriptionCreateOptions.builder().topic(TOPIC).build();

        assertThat(options.getTopic()).isEqualTo(TOPIC);
        assertThat(options.getAckDeadline()).isNull();
        assertThat(options.isEnableMessageOrdering()).isFalse();
        assertThat(options.getMessageRetention()).isNull();
        assertThat(options.isRetainAckedMessages()).isFalse();
        assertThat(options.getExpirationTtl()).isNull();
        assertThat(options.isNeverExpire()).isFalse();
        assertThat(options.getDeadLetterTopic()).isNull();
        assertThat(options.getDeadLetterMaxDeliveryAttempts()).isZero();
        assertThat(options.getFilter()).isNull();
    }

    @Test
    void carriesEveryConfiguredKnob() {
        SubscriptionCreateOptions options = fullyPopulated();

        assertThat(options.getAckDeadline()).isEqualTo(Duration.ofSeconds(30));
        assertThat(options.isEnableMessageOrdering()).isTrue();
        assertThat(options.getMessageRetention()).isEqualTo(Duration.ofHours(12));
        assertThat(options.isRetainAckedMessages()).isTrue();
        assertThat(options.getExpirationTtl()).isEqualTo(Duration.ofDays(2));
        assertThat(options.getDeadLetterTopic()).isEqualTo(DEAD_LETTER);
        assertThat(options.getDeadLetterMaxDeliveryAttempts()).isEqualTo(10);
        assertThat(options.getFilter()).isEqualTo("attributes.kind = \"order\"");
    }

    @Test
    void requiresATopic() {
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("topic(...) is required");
    }

    @Test
    void neverExpireAndAnExpirationTtlOverrideEachOther() {
        SubscriptionCreateOptions never =
                SubscriptionCreateOptions.builder()
                        .topic(TOPIC)
                        .expirationTtl(Duration.ofDays(2))
                        .neverExpire()
                        .build();
        assertThat(never.isNeverExpire()).isTrue();
        assertThat(never.getExpirationTtl()).isNull();

        SubscriptionCreateOptions ttl =
                SubscriptionCreateOptions.builder()
                        .topic(TOPIC)
                        .neverExpire()
                        .expirationTtl(Duration.ofDays(2))
                        .build();
        assertThat(ttl.isNeverExpire()).isFalse();
        assertThat(ttl.getExpirationTtl()).isEqualTo(Duration.ofDays(2));
    }

    @Test
    void rejectsAnAckDeadlineWithASubSecondRemainder() {
        // Pub/Sub stores whole seconds, so the remainder would vanish without a word.
        assertThatThrownBy(
                        () ->
                                SubscriptionCreateOptions.builder()
                                        .ackDeadline(Duration.ofMillis(1_500)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whole number of seconds");
    }

    @Test
    void rejectsNonPositiveDurations() {
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().ackDeadline(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ackDeadline must be positive");
        assertThatThrownBy(
                        () ->
                                SubscriptionCreateOptions.builder()
                                        .messageRetention(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messageRetention must be positive");
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().expirationTtl(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("expirationTtl must be positive");
    }

    @Test
    void rejectsANonPositiveDeliveryAttemptLimit() {
        assertThatThrownBy(
                        () -> SubscriptionCreateOptions.builder().deadLetterPolicy(DEAD_LETTER, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDeliveryAttempts must be positive");
    }

    @Test
    void rejectsABlankFilter() {
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().filter("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("filter must not be blank");
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().topic(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("topic must not be null");
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().deadLetterPolicy(null, 5))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("deadLetterTopic must not be null");
        assertThatThrownBy(() -> SubscriptionCreateOptions.builder().filter(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("filter must not be null");
    }

    @Test
    void optionsWithTheSameKnobsAreEqual() {
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated())
                .isNotEqualTo(SubscriptionCreateOptions.builder().topic(TOPIC).build());
        assertThat(fullyPopulated().toString())
                .startsWith("SubscriptionCreateOptions{topic=")
                .contains("deadLetterMaxDeliveryAttempts=10");
    }
}
