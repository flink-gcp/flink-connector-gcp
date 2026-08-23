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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * The options {@link SpannerConnectorOptions} declares, read by reflection.
 *
 * <p>Shared by the guards that need the whole set — the parity accounting and the
 * no-restated-default pair — rather than written twice, because two copies of the same reflection
 * can drift into asking slightly different questions. Deliberately no {@code isPublic} filter,
 * keeping the parity test's original net: a package-private option would still be counted rather
 * than silently escaping every guard.
 */
final class DeclaredOptions {

    private DeclaredOptions() {}

    /** Returns every static {@code ConfigOption} the connector declares. */
    static List<ConfigOption<?>> all() {
        List<ConfigOption<?>> options = new ArrayList<>();
        for (Field field : SpannerConnectorOptions.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
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
