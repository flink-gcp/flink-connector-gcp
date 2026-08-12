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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.PublicEvolving;

/** What the Bigtable table sink tells the planner when the requested changelog is insert-only. */
@PublicEvolving
public enum InsertOnlyInputMode {

    /** Advertise the sink's physical upsert behavior and enable Flink conflict strategies. */
    UPSERT("upsert"),

    /** Advertise an append sink so a plain insert remains portable across supported Flink lines. */
    INSERT_ONLY("insert-only");

    private final String value;

    InsertOnlyInputMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
