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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.spanner.table.ChangeStreamChangelogMode;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import java.io.IOException;

/** Collects the atomic row batch produced from one Spanner data-change record. */
@Internal
final class SpannerChangeStreamRowDataDeserializationSchema
        implements SpannerChangeStreamDeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final DataChangeRecordToRowDataConverter converter;
    private final TypeInformation<RowData> producedType;

    SpannerChangeStreamRowDataDeserializationSchema(
            SpannerTableSchemaConverter schema,
            SpannerTableName table,
            ChangeStreamChangelogMode changelogMode,
            TypeInformation<RowData> producedType) {
        this.converter = new DataChangeRecordToRowDataConverter(schema, table, changelogMode);
        this.producedType = producedType;
    }

    @Override
    public void deserialize(DataChangeRecord record, Collector<RowData> out) throws IOException {
        for (RowData row : converter.convert(record)) {
            out.collect(row);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
