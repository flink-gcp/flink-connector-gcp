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

/** Destination-side write operation selected by sink.write-mode. */
@PublicEvolving
public enum WriteMode {
    /** Writes cells through the ordinary mutation batcher. */
    UPSERT("upsert"),
    /** Atomically writes input cells only when the entire stored row has no cell. */
    INSERT_IF_ABSENT("insert-if-absent");

    private final String value;

    WriteMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
