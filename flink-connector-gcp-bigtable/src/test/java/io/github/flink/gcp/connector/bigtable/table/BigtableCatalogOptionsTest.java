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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.HtmlFormatter;

import io.github.flink.gcp.connector.bigtable.table.catalog.BigtableCatalogFactory;
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

/** The {@code bigtable} catalog's option surface, held against the factory and the connector. */
class BigtableCatalogOptionsTest {

    private static List<ConfigOption<?>> declaredOptions(Class<?> options) {
        List<ConfigOption<?>> declared = new ArrayList<>();
        for (Field field : options.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers())
                    && Modifier.isStatic(field.getModifiers())
                    && ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    declared.add((ConfigOption<?>) field.get(null));
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }
        return declared;
    }

    private static Set<String> keys(List<ConfigOption<?>> options) {
        return options.stream().map(ConfigOption::key).collect(Collectors.toSet());
    }

    @Test
    void theFactoryAcceptsExactlyTheDeclaredOptions() {
        BigtableCatalogFactory factory = new BigtableCatalogFactory();
        Set<ConfigOption<?>> accepted = new HashSet<>(factory.requiredOptions());
        accepted.addAll(factory.optionalOptions());

        assertThat(declaredOptions(BigtableCatalogOptions.class)).isNotEmpty();
        assertThat(keys(new ArrayList<>(accepted)))
                .isEqualTo(keys(declaredOptions(BigtableCatalogOptions.class)));
    }

    @Test
    void everyKeyButTheKeyTypeIsTheConnectorsSpelling() {
        // A resolved table carries these values under the connector's keys, the factory's
        // credential check reads them through the connector's options, and a hint overrides them
        // there; one spelling is what makes that the same option in every place.
        Set<String> catalogKeys = keys(declaredOptions(BigtableCatalogOptions.class));
        catalogKeys.remove(BigtableCatalogOptions.KEY_TYPE.key());

        assertThat(keys(declaredOptions(BigtableConnectorOptions.class))).containsAll(catalogKeys);
        assertThat(keys(declaredOptions(BigtableConnectorOptions.class)))
                .doesNotContain(BigtableCatalogOptions.KEY_TYPE.key());
    }

    /** Only the schema-deciding key type has a default, and it is the type that cannot misread. */
    @Test
    void onlyTheKeyTypeCarriesADefaultAndItIsBytes() {
        assertThat(declaredOptions(BigtableCatalogOptions.class))
                .filteredOn(ConfigOption::hasDefaultValue)
                .containsExactly(BigtableCatalogOptions.KEY_TYPE);
        assertThat(BigtableCatalogOptions.KEY_TYPE.defaultValue()).isEqualTo(CatalogKeyType.BYTES);
    }

    @Test
    void noDescriptionRestatesADefault() {
        HtmlFormatter formatter = new HtmlFormatter();
        assertThat(declaredOptions(BigtableCatalogOptions.class))
                .allSatisfy(
                        option ->
                                assertNoDefaultRestatement(
                                        option.key(),
                                        formatter.format(option.description()),
                                        "connectors/table/bigtable.md"));
    }
}
