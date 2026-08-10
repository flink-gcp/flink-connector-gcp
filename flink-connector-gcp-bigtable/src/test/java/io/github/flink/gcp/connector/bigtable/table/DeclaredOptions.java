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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * The options {@link BigtableConnectorOptions} declares, read by reflection.
 *
 * <p>Shared by the two guards that need the whole set — the factory round trip and the parity
 * accounting — rather than written twice, because two copies of the same reflection can drift into
 * asking slightly different questions.
 */
final class DeclaredOptions {

    private DeclaredOptions() {}

    /** Returns every public static {@code ConfigOption} the connector declares. */
    static List<ConfigOption<?>> all() {
        List<ConfigOption<?>> options = new ArrayList<>();
        for (Field field : BigtableConnectorOptions.class.getDeclaredFields()) {
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
}
