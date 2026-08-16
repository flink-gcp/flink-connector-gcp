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

import org.apache.flink.annotation.Internal;

import java.util.UUID;

/** Parses the table layer's string carrier as a native Spanner UUID value. */
@Internal
public final class UuidStringParser {

    private UuidStringParser() {}

    /** Parses the canonical UUID shape without accepting Java's shortened component forms. */
    public static UUID parse(String value, String columnName) {
        UUID uuid;
        try {
            uuid = UUID.fromString(value);
        } catch (IllegalArgumentException ignored) {
            throw invalidUuid(columnName);
        }
        if (!uuid.toString().equalsIgnoreCase(value)) {
            throw invalidUuid(columnName);
        }
        return uuid;
    }

    private static IllegalArgumentException invalidUuid(String columnName) {
        return new IllegalArgumentException(
                "Spanner UUID column '"
                        + columnName
                        + "' requires a 36-character UUID string in 8-4-4-4-12 hexadecimal form.");
    }
}
