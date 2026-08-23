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

package io.github.flink.gcp.connector.cloudtasks.table;

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

/** Guards on the Cloud Tasks table option inventory. */
class CloudTasksConnectorOptionsTest {

    private static List<ConfigOption<?>> declaredOptions() {
        List<ConfigOption<?>> options = new ArrayList<>();
        for (Field field : CloudTasksConnectorOptions.class.getDeclaredFields()) {
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
        CloudTasksDynamicTableFactory factory = new CloudTasksDynamicTableFactory();
        Set<String> accepted = new HashSet<>();
        factory.requiredOptions().forEach(option -> accepted.add(option.key()));
        factory.optionalOptions().forEach(option -> accepted.add(option.key()));

        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(option -> assertThat(accepted).contains(option.key()));
    }

    @Test
    void theFactoryDeclaresNoUnknownProjectOption() {
        Set<String> declared =
                declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toSet());
        declared.add("format");
        declared.add("sink.parallelism");
        CloudTasksDynamicTableFactory factory = new CloudTasksDynamicTableFactory();
        Set<String> fromFactory = new HashSet<>();
        factory.requiredOptions().forEach(option -> fromFactory.add(option.key()));
        factory.optionalOptions().forEach(option -> fromFactory.add(option.key()));

        assertThat(fromFactory).isSubsetOf(declared);
    }

    @Test
    void everyOptionKeyIsUnique() {
        assertThat(declaredOptions().stream().map(ConfigOption::key).collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @Test
    void onlyTargetSelectionAndMethodsCarryTableDefaults() {
        assertThat(declaredOptions())
                .filteredOn(ConfigOption::hasDefaultValue)
                .containsExactlyInAnyOrder(
                        CloudTasksConnectorOptions.TARGET_TYPE,
                        CloudTasksConnectorOptions.HTTP_METHOD,
                        CloudTasksConnectorOptions.APP_ENGINE_METHOD);
    }

    @Test
    void noDescriptionRestatesADefault() {
        // The half of the rule above a ConfigOption cannot express: a default written into prose —
        // a mapped setter's, a table-owned option's own defaultValue(), or the value absence
        // selects — is a second copy that nothing keeps in step. "The default HTTP method" naming
        // a per-row-overridable option's role is not in that class and matches no phrase. The
        // phrases are the restatement forms the #1045 cross-module sweep found, shared by every
        // connector's guard; a regression guard over those forms, not a semantic parser for
        // arbitrary prose.
        //
        // When this fires, the description is what changes. reference/cloudtasks.md is where a
        // mapped option's default is written — a derived one included, carrying both its
        // derivation and its resolved value — and the table page's option row is where a
        // table-owned option's default is written.
        HtmlFormatter formatter = new HtmlFormatter();
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertThat(formatter.format(option.description()))
                                        .as(
                                                "option '%s' restates a default; the cloudtasks"
                                                        + " reference or table docs page is where"
                                                        + " a default is written",
                                                option.key())
                                        .doesNotContainIgnoringCase("by default")
                                        .doesNotContainIgnoringCase("defaults to")
                                        .doesNotContainIgnoringCase("when unset")
                                        .doesNotContainIgnoringCase("unset means")
                                        .doesNotContainIgnoringCase("when absent")
                                        .doesNotContainIgnoringCase("absent uses")
                                        .doesNotContainIgnoringCase("absent,")
                                        .doesNotContainIgnoringCase("unset uses")
                                        .doesNotContainIgnoringCase("unset keeps")
                                        .doesNotContainIgnoringCase("unset leaves")
                                        .doesNotContainIgnoringCase("is the default")
                                        .doesNotContainIgnoringCase("and the default"));
    }
}
