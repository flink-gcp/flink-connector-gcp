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

package io.github.flink.gcp.connector.spanner.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import java.io.IOException;

/** Serializes table-runtime rows into mutations for one configured table. */
@Internal
public final class RowDataSerializationSchema
        implements SpannerMutationSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SpannerTableSchemaConverter schema;
    private final String table;

    public RowDataSerializationSchema(SpannerTableSchemaConverter schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public Mutation serialize(RowData row, SinkWriter.Context context) throws IOException {
        if (row.getRowKind() == RowKind.DELETE) {
            if (!schema.hasPrimaryKey()) {
                throw new IOException(
                        "A DELETE cannot reach a Spanner table without a declared PRIMARY KEY.");
            }
            return Mutation.delete(table, primaryKey(row));
        }
        if (row.getRowKind() == RowKind.UPDATE_BEFORE) {
            throw new IOException("UPDATE_BEFORE is not part of the Spanner table sink changelog.");
        }

        Mutation.WriteBuilder builder =
                schema.hasPrimaryKey()
                        ? Mutation.newInsertOrUpdateBuilder(table)
                        : Mutation.newInsertBuilder(table);
        for (SpannerTableSchemaConverter.Column column : schema.getColumns()) {
            builder.set(column.getName())
                    .to(
                            RowDataToSpannerValueConverter.convert(
                                    row,
                                    column.getIndex(),
                                    column.getLogicalType(),
                                    column.getSpannerType(),
                                    column.getName()));
        }
        return builder.build();
    }

    private Key primaryKey(RowData row) {
        Key.Builder key = Key.newBuilder();
        for (int index : schema.getPrimaryKeyIndexes()) {
            SpannerTableSchemaConverter.Column column = schema.getColumns().get(index);
            key.appendObject(
                    RowDataToSpannerValueConverter.keyPart(
                            row,
                            index,
                            column.getLogicalType(),
                            column.getSpannerType(),
                            column.getName()));
        }
        return key.build();
    }
}
