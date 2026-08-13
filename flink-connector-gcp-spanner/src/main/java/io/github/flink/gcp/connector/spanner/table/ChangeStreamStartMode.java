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

/** A Table API spelling for one shared Change Streams start position. */
@PublicEvolving
public enum ChangeStreamStartMode {

    /** Start at the oldest instant the stream can safely serve. */
    EARLIEST("earliest"),

    /** Start at the instant the source coordinator starts. */
    LATEST("latest"),

    /** Start at the absolute instant supplied by the companion timestamp option. */
    TIMESTAMP("timestamp");

    private final String value;

    ChangeStreamStartMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
