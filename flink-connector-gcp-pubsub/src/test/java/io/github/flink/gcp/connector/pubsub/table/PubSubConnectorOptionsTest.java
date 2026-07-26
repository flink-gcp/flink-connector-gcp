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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards on the option set as a whole.
 *
 * <p>{@code PublisherOptionsMapperTest} checks that every publisher knob has an option; this checks
 * the other half of the round trip — that every option the connector declares is one the factory
 * accepts. An option missing from {@code optionalOptions()} is rejected as unknown in a {@code
 * CREATE TABLE}, which no other test would notice: the mapper tests read a {@code Configuration}
 * directly, and the factory tests only ever set a handful of keys.
 */
class PubSubConnectorOptionsTest {

    private static List<ConfigOption<?>> declaredOptions() {
        List<ConfigOption<?>> options = new ArrayList<>();
        for (Field field : PubSubConnectorOptions.class.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers())
                    && Modifier.isStatic(field.getModifiers())
                    && ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    options.add((ConfigOption<?>) field.get(null));
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }
        return options;
    }

    @Test
    void everyDeclaredOptionIsAcceptedByTheFactory() {
        PubSubDynamicTableFactory factory = new PubSubDynamicTableFactory();
        Set<String> accepted = new HashSet<>();
        factory.requiredOptions().forEach(o -> accepted.add(o.key()));
        factory.optionalOptions().forEach(o -> accepted.add(o.key()));

        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertThat(accepted)
                                        .as(
                                                "option '%s' is declared but the factory would"
                                                        + " reject it as unknown",
                                                option.key())
                                        .contains(option.key()));
    }

    @Test
    void theFactoryDeclaresNoOptionThatDoesNotExist() {
        PubSubDynamicTableFactory factory = new PubSubDynamicTableFactory();
        Set<String> declared =
                declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toSet());
        // The two Flink-owned options the factory borrows rather than declaring itself.
        declared.add("format");
        declared.add("sink.parallelism");

        Set<String> fromFactory = new HashSet<>();
        factory.requiredOptions().forEach(o -> fromFactory.add(o.key()));
        factory.optionalOptions().forEach(o -> fromFactory.add(o.key()));

        assertThat(fromFactory).isSubsetOf(declared);
    }

    @Test
    void everyOptionKeyIsUnique() {
        List<String> keys =
                declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toList());

        assertThat(keys).doesNotHaveDuplicates();
    }

    @Test
    void noOptionCarriesADefault() {
        // The connector's defaults live in PubSubPublisherOptions, and are applied by not calling a
        // setter. A default here would be a second copy that nothing keeps in step.
        assertThat(declaredOptions())
                .allSatisfy(option -> assertThat(option.hasDefaultValue()).isFalse());
    }
}
