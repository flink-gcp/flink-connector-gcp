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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.HtmlFormatter;

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
        // The Flink-owned options the factory borrows rather than declaring itself.
        declared.add("format");
        declared.add("sink.parallelism");
        declared.add("scan.parallelism");

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

    @Test
    void noDescriptionRestatesADefault() {
        // The other half of the rule above, and the half a ConfigOption cannot express: a default
        // written into prose is the same second copy, just out of reach of hasDefaultValue(). Three
        // of these had accumulated by the time #778 read the file, all three deleted by #838, and
        // every check stayed green throughout — which is how they arrived.
        //
        // The first two phrases are the ones those three used ("Off by default:", "Defaults to
        // twice the effective flow-control message limit", "Defaults to twice the effective
        // flow-control byte limit"). A later review found two descriptions that still stated the
        // absent-value behaviour as "when unset" and "Unset means" while this test passed. Match
        // those forms too, but not the bare word: "application-default" can name a credential kind
        // without saying that it is the option's default.
        //
        // When this fires, the description is what changes. reference/pubsub.md is where a default
        // is written — a derived one included, carrying both its derivation and its resolved
        // value — and the table page's "Maps to" column is the pointer a DDL author follows there.
        HtmlFormatter formatter = new HtmlFormatter();
        // allSatisfy passes vacuously on an empty list, and declaredOptions() is reflective, so a
        // rename of the field shape it looks for would leave this test green rather than fire.
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertThat(formatter.format(option.description()))
                                        .as(
                                                "option '%s' restates a default;"
                                                        + " reference/pubsub.md is where a default"
                                                        + " is written",
                                                option.key())
                                        .doesNotContainIgnoringCase("by default")
                                        .doesNotContainIgnoringCase("defaults to")
                                        .doesNotContainIgnoringCase("when unset")
                                        .doesNotContainIgnoringCase("unset means"));
    }
}
