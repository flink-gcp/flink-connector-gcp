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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;

/** Whether the sink may create destination tables that do not exist. */
@PublicEvolving
public enum CreateDisposition {

    /**
     * Create the destination table when it does not exist, deriving the table schema from the
     * record's protobuf descriptor.
     */
    CREATE_IF_NEEDED("create-if-needed"),

    /** Never create destination tables; writing to a missing table fails. */
    CREATE_NEVER("create-never");

    private final String value;

    CreateDisposition(String value) {
        this.value = value;
    }

    /**
     * Returns the hyphenated lower-case spelling this constant takes in a {@code
     * sink.create-disposition} DDL option, for example {@code create-if-needed}.
     *
     * <p>Flink resolves an enum-valued {@code ConfigOption} by matching this string
     * case-insensitively and normalizing nothing else. Use {@link #name()} where a message means
     * the Java constant.
     */
    @Override
    public String toString() {
        return value;
    }
}
