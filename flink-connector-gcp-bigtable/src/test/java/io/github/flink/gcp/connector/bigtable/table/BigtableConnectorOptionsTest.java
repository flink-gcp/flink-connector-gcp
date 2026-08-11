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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards on the option set as a whole.
 *
 * <p>{@code BigtableOptionParityTest} checks that every builder knob has an option; this checks the
 * other half of the round trip — that every option the connector declares is one the factory
 * accepts. An option missing from {@code optionalOptions()} is rejected as unknown in a {@code
 * CREATE TABLE}, which no other test would notice: the mapper tests read a {@code Configuration}
 * directly, and the factory tests only ever set a handful of keys.
 */
class BigtableConnectorOptionsTest {

    /**
     * The options this layer owns rather than maps, which therefore may — and must — carry a
     * default: there is no connector-side default for them to be a second copy of.
     */
    private static final Set<String> TABLE_OWNED =
            new HashSet<>(Arrays.asList(BigtableConnectorOptions.NULL_STRING_LITERAL.key()));

    private static List<ConfigOption<?>> declaredOptions() {
        return DeclaredOptions.all();
    }

    @Test
    void everyDeclaredOptionIsAcceptedByTheFactory() {
        BigtableDynamicTableFactory factory = new BigtableDynamicTableFactory();
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
        BigtableDynamicTableFactory factory = new BigtableDynamicTableFactory();
        Set<String> declared =
                declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toSet());
        // The Flink-owned options the factory borrows rather than declaring itself.
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
    void onlyATableOwnedOptionCarriesADefault() {
        // A mapped option's default lives on the connector's own builder and is applied by not
        // calling a setter, so one here would be a second copy that nothing keeps in step. An
        // option this layer owns has no such original, so its default belongs here — and the
        // assertion is an exact partition rather than an exemption list, so it also fails if one
        // of those loses its default and starts reading as unset.
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertThat(option.hasDefaultValue())
                                        .as("option '%s' carries a default", option.key())
                                        .isEqualTo(TABLE_OWNED.contains(option.key())));
    }

    @Test
    void thereIsNoFormatOption() {
        // A Bigtable row is a schema this DDL describes, cell by cell, and the encoding is the
        // HBase ecosystem's rather than a choice. There is nothing for a format factory to decide,
        // so 'format' is not accepted and the factory discovers none.
        BigtableDynamicTableFactory factory = new BigtableDynamicTableFactory();
        Set<String> accepted = new HashSet<>();
        factory.requiredOptions().forEach(o -> accepted.add(o.key()));
        factory.optionalOptions().forEach(o -> accepted.add(o.key()));

        assertThat(accepted).doesNotContain("format", "key.format", "value.format");
    }
}
