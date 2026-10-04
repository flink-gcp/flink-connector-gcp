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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.annotation.PublicEvolving;

/** The document operation {@code sink.write-mode} selects for a table with a PRIMARY KEY. */
@PublicEvolving
public enum WriteMode {
    /** Replaces the document with the row, creating it if it is missing. */
    SET("set"),
    /**
     * Merges the row into the document, creating it if it is missing; fields the table does not
     * declare keep their values, and a ROW or MAP column is merged key by key.
     */
    MERGE("merge"),
    /** Replaces the table's fields of an existing document; a missing document fails the job. */
    UPDATE("update");

    private final String value;

    WriteMode(String value) {
        this.value = value;
    }

    /** Returns the spelling a {@code WITH} clause uses. */
    @Override
    public String toString() {
        return value;
    }
}
