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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Public;

/**
 * Whether the sink may create the destination table when it does not exist.
 *
 * <p>Unlike the Pub/Sub sink's disposition, {@link #CREATE_IF_NEEDED} cannot stand alone: a
 * Bigtable table's schema is its column families and their garbage-collection policies, which the
 * sink cannot guess, so the disposition requires {@link
 * BigtableSinkBuilder#tableCreateOptions(TableCreateOptions)} naming at least one family.
 *
 * <p>{@link #toString()} returns the hyphenated spelling rather than the constant name, because
 * that spelling is what a SQL {@code WITH} clause is written in: Flink resolves an enum {@code
 * ConfigOption} by matching the configured value against {@code toString()}, case-insensitively and
 * with no other normalization. Flink's own {@code DeliveryGuarantee} carries its option spelling
 * the same way.
 */
@Public
public enum CreateDisposition {

    /**
     * Create the destination table — or the column families it is missing — from {@link
     * TableCreateOptions} when a mutation finds them absent.
     */
    CREATE_IF_NEEDED("create-if-needed"),

    /** Never create the table or a column family; writing to a missing one fails. */
    CREATE_NEVER("create-never");

    private final String value;

    CreateDisposition(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
