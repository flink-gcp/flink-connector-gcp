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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.configuration.ConfigOption;

import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

/**
 * The declared side of each mapper test's "every option of the family feeds a knob" guard.
 *
 * <p>Read out of {@link BigQueryConnectorOptions} rather than written into each test, because a
 * literal list would only restate the test's own setter-to-option table and could never disagree
 * with it. Shared once four mappers needed it; each test keeps its own vacuity guard and its own
 * assertion, so what a test claims stays where the test is.
 */
final class OptionFamilies {

    private OptionFamilies() {}

    /**
     * Returns every connector option key beginning with the given prefix.
     *
     * @param prefix the family's key prefix, for example {@code "sink.file-loads."}
     * @return the declared keys
     */
    static Set<String> declaredKeysUnder(String prefix) {
        Set<String> declared = new HashSet<>();
        for (Field field : BigQueryConnectorOptions.class.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers())
                    && Modifier.isStatic(field.getModifiers())
                    && ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    String key = ((ConfigOption<?>) field.get(null)).key();
                    if (key.startsWith(prefix)) {
                        declared.add(key);
                    }
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }
        return declared;
    }
}
