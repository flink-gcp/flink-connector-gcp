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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

/** Converts Storage Read Avro rows to the planner's internal row type. */
@Internal
final class RowDataDeserializer implements BigQueryRowDeserializer<RowData> {

    private static final long serialVersionUID = 1L;

    private final GenericRecordToRowDataConverter converter;
    private final TypeInformation<RowData> producedType;

    RowDataDeserializer(
            RowType physicalRowType,
            @Nullable int[] projectedFields,
            TypeInformation<RowData> producedType) {
        this.converter = new GenericRecordToRowDataConverter(physicalRowType, projectedFields);
        this.producedType = producedType;
    }

    @Override
    public void deserialize(GenericRecord row, Collector<RowData> out) {
        out.collect(converter.convert(row));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
