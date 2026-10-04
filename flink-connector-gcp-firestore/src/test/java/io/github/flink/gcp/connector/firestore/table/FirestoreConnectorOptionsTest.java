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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.HtmlFormatter;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.testutils.OptionDescriptionAssertions.assertNoDefaultRestatement;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards on the option set as a whole: which options may carry a {@code defaultValue()}, that no
 * description states one in prose, and that each enum value parses from its DDL spelling. {@code
 * FirestoreOptionParityTest} holds the option set and the builder setters to each other.
 */
class FirestoreConnectorOptionsTest {

    @Test
    void onlyTheRecordedOptionsCarryADefault() {
        // A mapped option's default lives on the connector's own builder and is applied by not
        // calling a setter. These four are table-owned: no builder setter takes them, and the
        // factory reads each with get().
        assertThat(DeclaredOptions.all()).isNotEmpty();
        assertThat(DeclaredOptions.all())
                .filteredOn(ConfigOption::hasDefaultValue)
                .containsExactlyInAnyOrder(
                        FirestoreConnectorOptions.SINK_WRITE_MODE,
                        FirestoreConnectorOptions.TYPE_MISMATCH_POLICY,
                        FirestoreConnectorOptions.SCAN_COLLECTION_GROUP,
                        FirestoreConnectorOptions.LOOKUP_ASYNC);
    }

    @Test
    void noDescriptionRestatesADefault() {
        // When this fires, the description is what changes: reference/firestore.md carries a
        // mapped option's default, and the table page's option row a table-owned one's.
        HtmlFormatter formatter = new HtmlFormatter();
        assertThat(DeclaredOptions.all())
                .allSatisfy(
                        option ->
                                assertNoDefaultRestatement(
                                        option.key(),
                                        formatter.format(option.description()),
                                        "the firestore reference or table docs page"));
    }

    @Test
    void noOptionKeyStartsWithSchema() {
        // Flink's CatalogPropertiesUtil drops every option whose key starts with "schema" (2.2.1)
        // or "schema." (1.20.4) when it rebuilds a table from a catalog's stored properties, so
        // such an option would vanish from a persisted table without an error, and a marker
        // with it (ADR-0179).
        assertThat(DeclaredOptions.all())
                .extracting(ConfigOption::key)
                .noneMatch(key -> key.startsWith("schema"));
    }

    @Test
    void everyWriteModeParsesFromItsDdlSpelling() {
        ConfigOption<WriteMode> option =
                ConfigOptions.key("k").enumType(WriteMode.class).noDefaultValue();
        for (WriteMode mode : WriteMode.values()) {
            assertThat(Configuration.fromMap(Map.of("k", mode.toString())).get(option))
                    .isEqualTo(mode);
        }
        assertThat(List.of(WriteMode.values()))
                .extracting(WriteMode::toString)
                .containsExactly("set", "merge", "update");
    }

    @Test
    void everyTypeMismatchPolicyParsesFromItsDdlSpelling() {
        ConfigOption<TypeMismatchPolicy> option =
                ConfigOptions.key("k").enumType(TypeMismatchPolicy.class).noDefaultValue();
        for (TypeMismatchPolicy policy : TypeMismatchPolicy.values()) {
            assertThat(Configuration.fromMap(Map.of("k", policy.toString())).get(option))
                    .isEqualTo(policy);
        }
        assertThat(List.of(TypeMismatchPolicy.values()))
                .extracting(TypeMismatchPolicy::toString)
                .containsExactly("fail", "null");
    }

    @Test
    void theMarkerListsSplitOnSemicolons() {
        Configuration options =
                Configuration.fromMap(
                        Map.of(
                                "geo-point-field-paths",
                                "location;stops.position",
                                "reference-field-paths",
                                "author;items.product"));

        assertThat(options.get(FirestoreConnectorOptions.GEO_POINT_FIELD_PATHS))
                .containsExactly("location", "stops.position");
        assertThat(options.get(FirestoreConnectorOptions.REFERENCE_FIELD_PATHS))
                .containsExactly("author", "items.product");
    }
}
