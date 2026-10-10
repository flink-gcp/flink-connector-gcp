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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.HtmlFormatter;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.github.flink.gcp.connector.testutils.CatalogOptionAssertions.assertOptionsSurviveCatalogRoundTrip;
import static io.github.flink.gcp.connector.testutils.OptionDescriptionAssertions.assertNoDefaultRestatement;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards on the option set as a whole: that no option carries a {@code defaultValue()}, that no
 * description states one in prose, and that a persisting catalog keeps every option. {@code
 * DatastoreOptionParityTest} holds the option set and the builder setters to each other.
 */
class DatastoreConnectorOptionsTest {

    @Test
    void noOptionCarriesADefault() {
        // A mapped option's default lives on the connector's own builder and is applied by not
        // calling a setter; the table layer owns no selector with a default of its own.
        assertThat(DeclaredOptions.all()).isNotEmpty();
        assertThat(DeclaredOptions.all()).filteredOn(ConfigOption::hasDefaultValue).isEmpty();
    }

    @Test
    void noDescriptionRestatesADefault() {
        // When this fires, the description is what changes: reference/firestore.md carries a
        // mapped option's default.
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
    void everyOptionSurvivesACatalogThatPersistsTheTable() {
        DatastoreDynamicTableFactory factory = new DatastoreDynamicTableFactory();
        assertOptionsSurviveCatalogRoundTrip(factory.requiredOptions(), factory.optionalOptions());
    }

    @Test
    void theUnindexedColumnsSplitOnSemicolons() {
        Configuration options =
                Configuration.fromMap(Map.of("sink.unindexed-columns", "body;thumbnail"));

        assertThat(options.get(DatastoreConnectorOptions.SINK_UNINDEXED_COLUMNS))
                .containsExactly("body", "thumbnail");
    }
}
