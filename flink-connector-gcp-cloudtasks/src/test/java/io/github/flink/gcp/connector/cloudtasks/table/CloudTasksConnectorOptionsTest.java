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

package io.github.flink.gcp.connector.cloudtasks.table;

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
    void onlyTheHttpMethodCarriesATableDefault() {
        assertThat(declaredOptions())
                .filteredOn(ConfigOption::hasDefaultValue)
                .containsExactly(CloudTasksConnectorOptions.HTTP_METHOD);
    }
}
