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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** JSON values describing the changes to one watched table row. */
@Public
public final class Mod implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String keysJson;
    @Nullable private final String newValuesJson;
    @Nullable private final String oldValuesJson;

    public Mod(String keysJson, @Nullable String newValuesJson, @Nullable String oldValuesJson) {
        this.keysJson =
                SpannerChangeStreamJsonNormalizer.normalizeObject(
                        Preconditions.checkNotNull(keysJson, "keysJson must not be null"),
                        "keysJson");
        this.newValuesJson =
                newValuesJson == null
                        ? null
                        : SpannerChangeStreamJsonNormalizer.normalizeValue(newValuesJson);
        this.oldValuesJson =
                oldValuesJson == null
                        ? null
                        : SpannerChangeStreamJsonNormalizer.normalizeValue(oldValuesJson);
    }

    /**
     * Returns the JSON object containing the primary-key values with members sorted recursively.
     */
    public String getKeysJson() {
        return keysJson;
    }

    /**
     * Returns the normalized JSON value reported for {@code new_values}.
     *
     * <p>An empty optional means that the member was absent. A present {@code "null"} means that
     * Spanner explicitly reported JSON {@code null}; the distinction is retained.
     */
    public Optional<String> getNewValuesJson() {
        return Optional.ofNullable(newValuesJson);
    }

    /**
     * Returns the normalized JSON value reported for {@code old_values}, preserving absent versus
     * explicit JSON {@code null} in the same way as {@link #getNewValuesJson()}.
     */
    public Optional<String> getOldValuesJson() {
        return Optional.ofNullable(oldValuesJson);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Mod)) {
            return false;
        }
        Mod mod = (Mod) o;
        return keysJson.equals(mod.keysJson)
                && Objects.equals(newValuesJson, mod.newValuesJson)
                && Objects.equals(oldValuesJson, mod.oldValuesJson);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keysJson, newValuesJson, oldValuesJson);
    }
}
