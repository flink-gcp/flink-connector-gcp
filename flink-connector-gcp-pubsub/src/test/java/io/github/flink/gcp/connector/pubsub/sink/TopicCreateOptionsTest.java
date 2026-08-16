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

package io.github.flink.gcp.connector.pubsub.sink;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TopicCreateOptions}. */
class TopicCreateOptionsTest {

    /** Options with every knob set, for round-trip and equality tests. */
    static TopicCreateOptions fullyPopulated() {
        return TopicCreateOptions.builder()
                .messageRetention(Duration.ofDays(7))
                .kmsKeyName("projects/p/locations/l/keyRings/r/cryptoKeys/k")
                .allowedPersistenceRegions(Arrays.asList("europe-west1", "europe-west4"))
                .enforceInTransit(true)
                .build();
    }

    @Test
    void everyKnobIsUnsetByDefault() {
        TopicCreateOptions options = TopicCreateOptions.builder().build();

        assertThat(options.getMessageRetention()).isNull();
        assertThat(options.getKmsKeyName()).isNull();
        assertThat(options.getAllowedPersistenceRegions()).isNull();
        assertThat(options.isEnforceInTransit()).isFalse();
    }

    @Test
    void carriesEveryConfiguredKnob() {
        TopicCreateOptions options = fullyPopulated();

        assertThat(options.getMessageRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(options.getKmsKeyName())
                .isEqualTo("projects/p/locations/l/keyRings/r/cryptoKeys/k");
        assertThat(options.getAllowedPersistenceRegions())
                .containsExactly("europe-west1", "europe-west4");
        assertThat(options.isEnforceInTransit()).isTrue();
    }

    @Test
    void theRegionListIsCopiedAndUnmodifiable() {
        List<String> regions = new ArrayList<>(Arrays.asList("us-central1"));
        TopicCreateOptions options =
                TopicCreateOptions.builder().allowedPersistenceRegions(regions).build();

        regions.add("added-later");
        assertThat(options.getAllowedPersistenceRegions()).containsExactly("us-central1");
        assertThatThrownBy(() -> options.getAllowedPersistenceRegions().add("nope"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsEnforcingInTransitWithoutRegions() {
        assertThatThrownBy(() -> TopicCreateOptions.builder().enforceInTransit(true).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires allowedPersistenceRegions(...)");
    }

    @Test
    void rejectsANonPositiveRetention() {
        assertThatThrownBy(() -> TopicCreateOptions.builder().messageRetention(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageRetention must be positive");
        assertThatThrownBy(
                        () -> TopicCreateOptions.builder().messageRetention(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageRetention must be positive");
    }

    @Test
    void rejectsBlankValues() {
        assertThatThrownBy(() -> TopicCreateOptions.builder().kmsKeyName("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("kmsKeyName must not be blank");
        assertThatThrownBy(
                        () ->
                                TopicCreateOptions.builder()
                                        .allowedPersistenceRegions(
                                                Arrays.asList("us-central1", "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not contain a blank region");
    }

    @Test
    void rejectsAnEmptyRegionList() {
        assertThatThrownBy(
                        () ->
                                TopicCreateOptions.builder()
                                        .allowedPersistenceRegions(new ArrayList<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("allowedPersistenceRegions must not be empty");
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> TopicCreateOptions.builder().messageRetention(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageRetention must not be null");
        assertThatThrownBy(() -> TopicCreateOptions.builder().kmsKeyName(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("kmsKeyName must not be null");
        assertThatThrownBy(() -> TopicCreateOptions.builder().allowedPersistenceRegions(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("allowedPersistenceRegions must not be null");
    }

    @Test
    void optionsWithTheSameKnobsAreEqual() {
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated())
                .isNotEqualTo(TopicCreateOptions.builder().build());
        assertThat(fullyPopulated().toString())
                .startsWith("TopicCreateOptions{messageRetention=")
                .contains("enforceInTransit=true");
    }
}
