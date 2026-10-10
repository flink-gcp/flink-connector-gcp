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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.annotation.PublicEvolving;

/**
 * What {@code type-mismatch-policy} does with a stored value whose Datastore type does not match
 * the declared column: a kind has no schema, so its entities can hold, under one property name,
 * values the table's type cannot represent.
 */
@PublicEvolving
public enum TypeMismatchPolicy {
    /** Fail the read, naming the entity and the property. */
    FAIL("fail"),
    /**
     * Read the innermost nullable field around the value as NULL; a NOT NULL column, which has no
     * nullable field around it, still fails the read.
     */
    NULL("null");

    private final String value;

    TypeMismatchPolicy(String value) {
        this.value = value;
    }

    /** Returns the spelling a {@code WITH} clause uses. */
    @Override
    public String toString() {
        return value;
    }
}
