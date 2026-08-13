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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.LinkedHashMap;
import java.util.Map;

/** Sequence sources a BigQuery CDC table sink can consume as writable metadata. */
@Internal
enum WritableMetadata {

    /** An already formatted BigQuery {@code _CHANGE_SEQUENCE_NUMBER}. */
    CHANGE_SEQUENCE_NUMBER("change-sequence-number", DataTypes.STRING().nullable()),

    /** The map exposed by a Debezium format as {@code value.source.properties}. */
    DEBEZIUM_SOURCE_PROPERTIES(
            "debezium-source-properties",
            DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable());

    private final String key;
    private final DataType dataType;

    WritableMetadata(String key, DataType dataType) {
        this.key = key;
        this.dataType = dataType;
    }

    String getKey() {
        return key;
    }

    /** Returns the stable metadata inventory in planner iteration order. */
    static Map<String, DataType> listAll() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        for (WritableMetadata value : values()) {
            metadata.put(value.key, value.dataType);
        }
        return metadata;
    }

    /** Returns the metadata source with the given key. */
    static WritableMetadata of(String key) {
        for (WritableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown BigQuery writable metadata key '" + key + "'.");
    }
}
