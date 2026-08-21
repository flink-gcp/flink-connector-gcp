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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;

import java.util.LinkedHashMap;
import java.util.Map;

/** Stable scalar fields of a Bigtable mutation exposed as readable metadata. */
@Internal
enum ChangeStreamReadableMetadata {

    /** Whether the service produced a user or garbage-collection mutation. */
    MUTATION_TYPE("mutation-type", DataTypes.STRING().notNull()) {
        @Override
        Object read(BigtableChangeStreamMutation mutation) {
            return StringData.fromString(mutation.getType().name());
        }
    },

    /** The originating cluster, or {@code null} for a garbage-collection mutation. */
    SOURCE_CLUSTER_ID("source-cluster-id", DataTypes.STRING()) {
        @Override
        Object read(BigtableChangeStreamMutation mutation) {
            return mutation.getSourceClusterId().isEmpty()
                    ? null
                    : StringData.fromString(mutation.getSourceClusterId());
        }
    },

    /** The service commit time, retaining its nanosecond precision. */
    COMMIT_TIMESTAMP("commit-timestamp", DataTypes.TIMESTAMP_LTZ(9).notNull()) {
        @Override
        Object read(BigtableChangeStreamMutation mutation) {
            return TimestampData.fromInstant(mutation.getCommitTime());
        }
    },

    /** The service tie breaker for mutations committed at the same time. */
    TIE_BREAKER("tie-breaker", DataTypes.INT().notNull()) {
        @Override
        Object read(BigtableChangeStreamMutation mutation) {
            return mutation.getTieBreaker();
        }
    },

    /** The partition's estimated low watermark at this mutation. */
    ESTIMATED_LOW_WATERMARK("estimated-low-watermark", DataTypes.TIMESTAMP_LTZ(9).notNull()) {
        @Override
        Object read(BigtableChangeStreamMutation mutation) {
            return TimestampData.fromInstant(mutation.getEstimatedLowWatermarkTime());
        }
    };

    private final String key;
    private final DataType dataType;

    ChangeStreamReadableMetadata(String key, DataType dataType) {
        this.key = key;
        this.dataType = dataType;
    }

    /** Returns all readable metadata in the order used for produced rows. */
    static Map<String, DataType> listAll() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        for (ChangeStreamReadableMetadata value : values()) {
            metadata.put(value.key, value.dataType);
        }
        return metadata;
    }

    /** Returns the metadata field for a planner-selected key. */
    static ChangeStreamReadableMetadata of(String key) {
        for (ChangeStreamReadableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        throw new IllegalArgumentException(
                "Unknown Bigtable Change Streams readable metadata key '" + key + "'.");
    }

    /** Reads this field from a mutation using Flink's internal data structures. */
    abstract Object read(BigtableChangeStreamMutation mutation);
}
