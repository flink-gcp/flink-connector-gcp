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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Assertions on option preservation in catalogs that store tables as properties. */
@Internal
public final class CatalogOptionAssertions {

    /**
     * Asserts that every registered option survives a persisting catalog's round trip.
     *
     * <p>Only the current option keys are checked, not fallback or deprecated spellings. The
     * assertion supplies placeholder values without asking the factory to parse them. The public
     * catalog API determines which keys are reserved on the Flink version running the test.
     *
     * @param requiredOptions the factory's required options
     * @param optionalOptions the factory's optional options, including borrowed Flink options
     */
    public static void assertOptionsSurviveCatalogRoundTrip(
            Set<ConfigOption<?>> requiredOptions, Set<ConfigOption<?>> optionalOptions) {
        Map<String, String> options = new HashMap<>();
        for (ConfigOption<?> option : requiredOptions) {
            options.put(option.key(), "value");
        }
        for (ConfigOption<?> option : optionalOptions) {
            options.put(option.key(), "value");
        }

        assertThat(options).as("factory option set must not be empty").isNotEmpty();
        assertThat(roundTripOptions(options))
                .as("catalog round trip must preserve every registered option and its value")
                .containsAllEntriesOf(options);
    }

    /**
     * Returns the options recovered after storing a resolved table as properties.
     *
     * <p>The minimal physical schema supplies the serialized schema properties alongside the
     * caller's options. This also supports tests of deprecated spellings that a catalog loses.
     *
     * @param options the table options to store
     * @return the options recovered by the public catalog API
     */
    public static Map<String, String> roundTripOptions(Map<String, String> options) {
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

    private CatalogOptionAssertions() {}
}
