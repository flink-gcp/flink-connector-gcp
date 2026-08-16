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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import java.io.IOException;

/**
 * The table layer's deserializer: every Bigtable row is exactly one {@code RowData}.
 *
 * <p>The type information is handed in from {@code ScanContext.createTypeInformation(DataType)} —
 * the {@code InternalTypeInfo} the planner would have built, reached through a {@code
 * PublicEvolving} interface, which is what keeps {@code flink-table-runtime} off this module's
 * dependencies.
 */
final class RowDataDeserializationSchema implements BigtableRowDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowToRowDataConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;

    RowDataDeserializationSchema(
            RowToRowDataConverter converter, TypeInformation<RowData> producedTypeInfo) {
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void deserialize(Row row, Collector<RowData> out) throws IOException {
        out.collect(converter.convert(row));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
