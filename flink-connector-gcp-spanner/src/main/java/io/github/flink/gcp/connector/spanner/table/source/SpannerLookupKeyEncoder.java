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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.table.data.RowData;

import com.google.cloud.spanner.Key;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import java.io.Serializable;

/** Encodes a planner lookup-key row in declared Spanner primary-key order. */
final class SpannerLookupKeyEncoder implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SpannerTableSchemaConverter schema;
    private final int[] keyPositions;

    SpannerLookupKeyEncoder(SpannerTableSchemaConverter schema, int[] keyPositions) {
        this.schema = schema;
        this.keyPositions = keyPositions;
    }

    Key encode(RowData row) {
        Key.Builder key = Key.newBuilder();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            SpannerTableSchemaConverter.Column column =
                    schema.getColumns().get(primaryKeyIndexes[i]);
            key.appendObject(SpannerKeyValueEncoder.rowValue(row, keyPositions[i], column));
        }
        return key.build();
    }
}
