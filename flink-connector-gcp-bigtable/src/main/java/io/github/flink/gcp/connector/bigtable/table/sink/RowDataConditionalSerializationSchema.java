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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalSerializationSchema;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;

import java.io.IOException;

/** Uses the ordinary Table cell encoding for an atomic whole-row insert-if-absent request. */
@Internal
final class RowDataConditionalSerializationSchema
        implements ConditionalSerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private final RowDataSerializationSchema cells;

    RowDataConditionalSerializationSchema(
            BigtableTableSchema schema,
            String nullStringLiteral,
            WritableMetadata[] metadata,
            boolean truncateCellTimestampToMillis) {
        cells =
                new RowDataSerializationSchema(
                        schema, nullStringLiteral, metadata, truncateCellTimestampToMillis);
    }

    @Override
    public ConditionalRequest serialize(RowData input, SinkWriter.Context context)
            throws IOException {
        return cells.insertIfAbsent(input);
    }
}
