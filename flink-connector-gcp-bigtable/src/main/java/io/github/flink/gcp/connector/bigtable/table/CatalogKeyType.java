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

import org.apache.flink.annotation.PublicEvolving;

/**
 * The Flink type the {@code bigtable} catalog gives a table's row key and the keys of its column
 * family maps, which are the qualifiers.
 */
@PublicEvolving
public enum CatalogKeyType {

    /** {@code BYTES}: the stored bytes as they are, which is GoogleSQL for Bigtable's shape. */
    BYTES("bytes"),

    /**
     * {@code STRING}: the stored bytes read as UTF-8 without validating them, so a key compares
     * with a string literal.
     */
    STRING("string");

    private final String value;

    CatalogKeyType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
