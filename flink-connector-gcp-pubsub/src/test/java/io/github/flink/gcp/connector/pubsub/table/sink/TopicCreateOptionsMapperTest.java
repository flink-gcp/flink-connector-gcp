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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TopicCreateOptionsMapper}. */
class TopicCreateOptionsMapperTest {

    /**
     * Every {@code TopicCreateOptions.Builder} setter and the option that feeds it, written out
     * because the key names are grouped and no naming rule derives one from the other. The
     * reflection test below is what makes the table exhaustive.
     */
    private static final Map<String, ConfigOption<?>> SETTER_TO_OPTION = new LinkedHashMap<>();

    static {
        SETTER_TO_OPTION.put(
                "messageRetention", PubSubConnectorOptions.SINK_AUTO_CREATE_MESSAGE_RETENTION);
        SETTER_TO_OPTION.put("kmsKeyName", PubSubConnectorOptions.SINK_AUTO_CREATE_KMS_KEY_NAME);
        SETTER_TO_OPTION.put(
                "allowedPersistenceRegions",
                PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS);
        SETTER_TO_OPTION.put(
                "enforceInTransit",
                PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT);
    }

    @Test
    void everyCreationKnobHasAnOption() {
        Set<String> setters =
                java.util.Arrays.stream(TopicCreateOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == TopicCreateOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        assertThat(setters).isEqualTo(SETTER_TO_OPTION.keySet());
    }

    private static String key(String setter) {
        return SETTER_TO_OPTION.get(setter).key();
    }

    private static TopicCreateOptions map(Map<String, String> options) {
        return TopicCreateOptionsMapper.map(Configuration.fromMap(options));
    }

    @Test
    void noCreationOptionMeansNoObject() {
        assertThat(TopicCreateOptionsMapper.map(new Configuration())).isNull();

        // Unrelated sink options do not conjure one either.
        Map<String, String> options = new HashMap<>();
        options.put(PubSubConnectorOptions.SINK_CREATE_DISPOSITION.key(), "create-if-needed");
        assertThat(map(options)).isNull();
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = new HashMap<>();
        options.put(key("messageRetention"), "7 d");
        options.put(key("kmsKeyName"), "projects/p/locations/l/keyRings/r/cryptoKeys/k");
        options.put(key("allowedPersistenceRegions"), "europe-west1;europe-west4");
        options.put(key("enforceInTransit"), "true");

        TopicCreateOptions mapped = map(options);

        assertThat(mapped.getMessageRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(mapped.getKmsKeyName())
                .isEqualTo("projects/p/locations/l/keyRings/r/cryptoKeys/k");
        assertThat(mapped.getAllowedPersistenceRegions())
                .containsExactly("europe-west1", "europe-west4");
        assertThat(mapped.isEnforceInTransit()).isTrue();
    }

    @Test
    void anOptionLeftOutStaysUnsetRatherThanTakingAValue() {
        Map<String, String> options = new HashMap<>();
        options.put(key("messageRetention"), "31 d");

        TopicCreateOptions mapped = map(options);

        assertThat(mapped.getMessageRetention()).isEqualTo(Duration.ofDays(31));
        assertThat(mapped.getKmsKeyName()).isNull();
        assertThat(mapped.getAllowedPersistenceRegions()).isNull();
        assertThat(mapped.isEnforceInTransit()).isFalse();
    }

    @Test
    void settingsAlongsideAnExplicitCreateNeverAreRejected() {
        // The disposition defaults to create-if-needed, so the settings alone are meaningful —
        // only saying "never create" while configuring what a created topic looks like is the
        // contradiction.
        Map<String, String> options = new HashMap<>();
        options.put(PubSubConnectorOptions.SINK_CREATE_DISPOSITION.key(), "create-never");
        options.put(key("messageRetention"), "7 d");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(key("messageRetention"))
                .hasMessageContaining(PubSubConnectorOptions.SINK_CREATE_DISPOSITION.key())
                .hasMessageContaining("create-never");
    }

    @Test
    void settingsWithoutADispositionRideTheCreateIfNeededDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(key("messageRetention"), "7 d");

        assertThat(map(options)).isNotNull();

        options.put(PubSubConnectorOptions.SINK_CREATE_DISPOSITION.key(), "create-if-needed");
        assertThat(map(options)).isNotNull();
    }

    @Test
    void enforcingInTransitWithoutRegionsIsRejectedInOptionKeys() {
        Map<String, String> options = new HashMap<>();
        options.put(key("enforceInTransit"), "true");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(key("enforceInTransit"))
                .hasMessageContaining(key("allowedPersistenceRegions"));
    }

    @Test
    void anExplicitlyFalseEnforceInTransitNeedsNoRegions() {
        Map<String, String> options = new HashMap<>();
        options.put(key("enforceInTransit"), "false");

        assertThat(map(options)).isEqualTo(TopicCreateOptions.builder().build());
    }

    @Test
    void namesTheOptionKeyWhenAValueIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.auto-create.message-retention", "0 s");

        assertThatThrownBy(() -> TopicCreateOptionsMapper.map(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.auto-create.message-retention' is invalid")
                .hasMessageContaining("messageRetention must be positive");
    }
}
