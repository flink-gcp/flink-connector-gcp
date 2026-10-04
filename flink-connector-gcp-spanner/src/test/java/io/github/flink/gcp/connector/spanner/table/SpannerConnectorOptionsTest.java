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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.github.flink.gcp.connector.testutils.OptionDescriptionAssertions.assertNoDefaultRestatement;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards on the option set as a whole.
 *
 * <p>{@code SpannerOptionParityTest} holds the option set and the builder setters to each other;
 * this class holds the two halves of the no-restated-default rule — which options may carry a
 * {@code defaultValue()}, and that no description states one in prose — and that every option
 * survives a catalog that persists the table as properties.
 */
class SpannerConnectorOptionsTest {

    private static List<ConfigOption<?>> declaredOptions() {
        return DeclaredOptions.all();
    }

    @Test
    void onlyTheRecordedOptionsCarryADefault() {
        // A mapped option's default lives on the connector's own builder and is applied by not
        // calling a setter, so a copy here would usually be a second copy that nothing keeps in
        // step. The recorded exceptions are of two kinds: table-owned selectors the factory reads
        // with get() (dialect, scan.mode, scan.startup.mode, lookup.async), and three change-stream
        // knobs whose defaultValue() references the builder's own constant, so the compiler keeps
        // the two in step. Anything joining this list does so deliberately.
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .filteredOn(ConfigOption::hasDefaultValue)
                .containsExactlyInAnyOrder(
                        SpannerConnectorOptions.DIALECT,
                        SpannerConnectorOptions.SCAN_MODE,
                        SpannerConnectorOptions.SCAN_STARTUP_MODE,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL,
                        SpannerConnectorOptions.SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK,
                        SpannerConnectorOptions.LOOKUP_ASYNC);
    }

    @Test
    void noDescriptionRestatesADefault() {
        // The half of the rule above a ConfigOption cannot express: a default written into prose —
        // a builder's, an option's own defaultValue(), or the value absence selects — is a second
        // copy that nothing keeps in step. "Unset fails the source instead" is not in that class:
        // absence selecting a failure is the option's contract, not a default. The shared
        // assertion owns the #1045 cross-module sweep's recorded phrases.
        //
        // When this fires, the description is what changes. reference/spanner.md is where a
        // mapped option's default is written — a derived one included, carrying both its
        // derivation and its resolved value — and the table page's option row is where a
        // table-owned option's default is written.
        HtmlFormatter formatter = new HtmlFormatter();
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertNoDefaultRestatement(
                                        option.key(),
                                        formatter.format(option.description()),
                                        "the spanner reference or table docs page"));
    }

    @Test
    void noOptionKeyStartsWithSchema() {
        // Flink's CatalogPropertiesUtil drops every option whose key starts with "schema" (2.2.1)
        // or "schema." (1.20.4) when it rebuilds a table from a catalog's stored properties, so
        // such an option would vanish from a persisted table without an error (issue #1617).
        assertThat(declaredOptions())
                .extracting(ConfigOption::key)
                .noneMatch(key -> key.startsWith("schema"));
    }

    @Test
    void theDeprecatedKeysAreTheSpellingsReleasesPublished() {
        // 1.0.0 and 1.1.0 published these five spellings; generated-columns had none released.
        Map<String, List<String>> deprecated = new HashMap<>();
        for (ConfigOption<?> option : declaredOptions()) {
            List<String> keys =
                    StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                            .filter(FallbackKey::isDeprecated)
                            .map(FallbackKey::getKey)
                            .collect(Collectors.toList());
            if (!keys.isEmpty()) {
                deprecated.put(option.key(), keys);
            }
        }

        assertThat(deprecated)
                .containsOnly(
                        Map.entry("named-schema", List.of("schema")),
                        Map.entry("json-field-paths", List.of("schema.json-field-paths")),
                        Map.entry("uuid-field-paths", List.of("schema.uuid-field-paths")),
                        Map.entry("proto-type-names", List.of("schema.proto-type-names")),
                        Map.entry("enum-type-names", List.of("schema.enum-type-names")));
    }

    @Test
    void everyOptionSurvivesACatalogThatPersistsTheTable() {
        // The round trip HiveCatalog and every catalog storing a table as properties performs,
        // through the public API. It runs against whichever Flink version the build selects, over
        // the connector's own options and the Flink-owned ones the factory registers.
        SpannerDynamicTableFactory factory = new SpannerDynamicTableFactory();
        Map<String, String> options = new HashMap<>();
        for (ConfigOption<?> option : declaredOptions()) {
            options.put(option.key(), "value");
        }
        for (ConfigOption<?> option : factory.requiredOptions()) {
            options.put(option.key(), "value");
        }
        for (ConfigOption<?> option : factory.optionalOptions()) {
            options.put(option.key(), "value");
        }

        assertThat(roundTrip(options)).containsAllEntriesOf(options);
    }

    @Test
    void aPersistingCatalogLosesTheDeprecatedSchemaPrefixedMarkerKeys() {
        // The limitation the docs state for the old spellings. Flink 1.20 keeps a bare "schema",
        // so only the dotted spellings are lost on every supported version.
        Map<String, String> options =
                Map.of(
                        "connector", "spanner",
                        "schema.json-field-paths", "a",
                        "schema.uuid-field-paths", "b",
                        "schema.proto-type-names", "c:example.Event",
                        "schema.enum-type-names", "d:example.Status");

        assertThat(roundTrip(options)).containsOnlyKeys("connector");
    }

    private static Map<String, String> roundTrip(Map<String, String> options) {
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("id", DataTypes.BIGINT()).build())
                        .options(options)
                        .build();
        ResolvedCatalogTable resolved =
                new ResolvedCatalogTable(
                        table, ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT())));
        return CatalogTable.fromProperties(resolved.toProperties()).getOptions();
    }
}
