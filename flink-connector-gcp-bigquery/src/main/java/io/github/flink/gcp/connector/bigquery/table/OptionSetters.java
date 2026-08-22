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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import javax.annotation.Nullable;

import java.util.function.Consumer;

/**
 * Applies Table API option values to DataStream builder setters, renaming a rejected value's
 * failure to the option key the SQL caller actually wrote (issue #1030, ADR-0133).
 *
 * <p>A builder setter's {@code Preconditions} message names the setter — the right name for a
 * DataStream caller, and one that appears nowhere in a {@code WITH} clause. {@code FactoryUtil}
 * wraps whatever a factory throws in a {@code ValidationException} whose own message says only
 * "Unable to create a …" line for the table, leaving the actionable sentence in the cause, so the
 * rename here is what makes the failure name the option. The builder's sentence stays as the
 * detail: the bound itself is defined and tested there, never restated here.
 */
@Internal
public final class OptionSetters {

    private OptionSetters() {}

    /**
     * Applies the option's value to the setter when present; a value the setter rejects fails as a
     * {@link ValidationException} naming the option key, with the setter's own message as detail.
     */
    public static <T> void apply(
            ReadableConfig config, ConfigOption<T> option, Consumer<T> setter) {
        config.getOptional(option).ifPresent(value -> accept(option.key(), value, setter));
    }

    /**
     * Applies an already-extracted value to the setter under the given option key, for a site whose
     * value no longer sits in a {@link ReadableConfig}. A {@code null} value applies nothing,
     * mirroring an absent option.
     */
    public static <T> void accept(String optionKey, @Nullable T value, Consumer<T> setter) {
        if (value == null) {
            return;
        }
        try {
            setter.accept(value);
        } catch (IllegalArgumentException e) {
            throw rename(optionKey, e);
        }
    }

    private static ValidationException rename(String optionKey, IllegalArgumentException cause) {
        return new ValidationException(
                String.format("Option '%s' is invalid: %s", optionKey, cause.getMessage()), cause);
    }
}
