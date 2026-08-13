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

/** The Spanner read path selected by a table DDL. */
@PublicEvolving
public enum ScanMode {

    /** Read the current table contents through a bounded batch read. */
    BOUNDED("bounded"),

    /** Read row changes through Spanner Change Streams. */
    CHANGE_STREAM("change-stream");

    private final String value;

    ScanMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
