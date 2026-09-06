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

/** The value type of a column family created by the connector. */
@Public
public enum ColumnFamilyType {
    /** Untyped cell bytes used by ordinary SetCell writes. */
    RAW("raw"),
    /** The sum of signed 64-bit integer contributions. */
    INT64_SUM("int64-sum"),
    /** The minimum signed 64-bit integer contribution. */
    INT64_MIN("int64-min"),
    /** The maximum signed 64-bit integer contribution. */
    INT64_MAX("int64-max"),
    /** A HyperLogLog++ sketch of distinct signed 64-bit integer contributions. */
    INT64_HLL("int64-hll");

    private final String value;

    ColumnFamilyType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
