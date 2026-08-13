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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.PublicEvolving;

/** The relational changelog emitted from Spanner data-change records. */
@PublicEvolving
public enum ChangeStreamChangelogMode {

    /** Emit complete insert, update-before, update-after, and delete rows. */
    FULL("full"),

    /** Emit inserts, update-after rows, and key-only deletes. */
    UPSERT("upsert");

    private final String value;

    ChangeStreamChangelogMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
