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

package io.github.flink.gcp.connector.bigquery.table;

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

import static io.github.flink.gcp.connector.testutils.OptionDescriptionAssertions.assertNoDefaultRestatement;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds {@link BigQueryConnectorOptions} and {@link BigQueryDynamicTableFactory} to each other.
 *
 * <p>The mapper tests check that each option reaches its setter; these check the set of options
 * itself — that the factory accepts every one declared, declares none that does not exist, and that
 * none carries a default the connector would then own a second copy of.
 */
class BigQueryConnectorOptionsTest {

    /** The Flink-owned keys the factory borrows rather than declaring itself. */
    private static final Set<String> FLINK_OWNED =
            new HashSet<>(java.util.Arrays.asList("scan.parallelism", "sink.parallelism"));

    private static List<ConfigOption<?>> declaredOptions() {
        List<ConfigOption<?>> options = new ArrayList<>();
        for (Field field : BigQueryConnectorOptions.class.getDeclaredFields()) {
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

    private static Set<String> factoryKeys() {
        BigQueryDynamicTableFactory factory = new BigQueryDynamicTableFactory();
        Set<ConfigOption<?>> all = new HashSet<>(factory.requiredOptions());
        all.addAll(factory.optionalOptions());
        return all.stream().map(ConfigOption::key).collect(Collectors.toSet());
    }

    @Test
    void everyDeclaredOptionIsAcceptedByTheFactory() {
        // Guards the reflection helper itself: an empty list would make every assertion vacuous.
        assertThat(declaredOptions()).isNotEmpty();
        Set<String> fromFactory = factoryKeys();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertThat(fromFactory)
                                        .as(
                                                "option '%s' is declared but the factory would"
                                                        + " reject it as unknown",
                                                option.key())
                                        .contains(option.key()));
    }

    @Test
    void theFactoryDeclaresNoOptionThatDoesNotExist() {
        Set<String> declared =
                declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toSet());
        declared.addAll(FLINK_OWNED);
        assertThat(factoryKeys()).isSubsetOf(declared);
    }

    @Test
    void everyOptionKeyIsUnique() {
        List<String> keys =
                declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toList());
        assertThat(keys).doesNotHaveDuplicates();
    }

    @Test
    void readSideOptionsUseScanVocabulary() {
        List<ConfigOption<?>> scanOptions =
                java.util.Arrays.asList(
                        BigQueryConnectorOptions.SCAN_PARENT_PROJECT,
                        BigQueryConnectorOptions.SCAN_QUERY,
                        BigQueryConnectorOptions.SCAN_MATERIALIZE_VIEWS,
                        BigQueryConnectorOptions.SCAN_QUERY_LOCATION,
                        BigQueryConnectorOptions.SCAN_QUERY_RESULT_DATASET,
                        BigQueryConnectorOptions.SCAN_REUSE_QUERY_RESULT_WITHIN,
                        BigQueryConnectorOptions.SCAN_ROW_RESTRICTION,
                        BigQueryConnectorOptions.SCAN_SNAPSHOT_TIME,
                        BigQueryConnectorOptions.SCAN_MAX_STREAM_COUNT,
                        BigQueryConnectorOptions.SCAN_PREFERRED_MIN_STREAM_COUNT,
                        BigQueryConnectorOptions.SCAN_MAX_RECORDS_PER_FETCH,
                        BigQueryConnectorOptions.SCAN_RETRY_MAX_ATTEMPTS);

        assertThat(scanOptions)
                .extracting(ConfigOption::key)
                .containsExactlyInAnyOrder(
                        "scan.parent-project",
                        "scan.query",
                        "scan.materialize-views",
                        "scan.query-location",
                        "scan.query-result-dataset",
                        "scan.reuse-query-result-within",
                        "scan.row-restriction",
                        "scan.snapshot-time",
                        "scan.max-stream-count",
                        "scan.preferred-min-stream-count",
                        "scan.max-records-per-fetch",
                        "scan.retry.max-attempts");
        assertThat(declaredOptions())
                .extracting(ConfigOption::key)
                .noneMatch(key -> key.startsWith("source."));
    }

    @Test
    void noOptionCarriesADefault() {
        // The connector's defaults live in its own options objects and are applied by not calling a
        // setter. A default here would be a second copy that nothing keeps in step.
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(option -> assertThat(option.hasDefaultValue()).isFalse());
    }

    @Test
    void noDescriptionRestatesADefault() {
        // The other half of the rule above, and the half a ConfigOption cannot express: a default
        // written into prose — a declared one, a derived one, or the value absence selects — is
        // the same second copy, just out of reach of hasDefaultValue(). The shared assertion owns
        // the #1045 cross-module sweep's recorded phrases.
        //
        // When this fires, the description is what changes. reference/bigquery.md is where a
        // default is written — a derived one included, carrying both its derivation and its
        // resolved value.
        HtmlFormatter formatter = new HtmlFormatter();
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertNoDefaultRestatement(
                                        option.key(),
                                        formatter.format(option.description()),
                                        "reference/bigquery.md"));
    }

    @Test
    void thereIsNoFormatOption() {
        // A BigQuery row is structured and the DDL schema is the schema, so unlike the Pub/Sub
        // connector this one supplies its own converter and serializer and takes no format.
        assertThat(factoryKeys()).doesNotContain("format");
    }
}
