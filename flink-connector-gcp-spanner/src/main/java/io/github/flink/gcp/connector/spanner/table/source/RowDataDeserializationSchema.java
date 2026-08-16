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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

/** Delegates Spanner row conversion while retaining the planner-produced type information. */
@Internal
final class RowDataDeserializationSchema implements SpannerStructDeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private final StructToRowDataConverter converter;
    private final TypeInformation<RowData> producedType;

    RowDataDeserializationSchema(
            StructToRowDataConverter converter, TypeInformation<RowData> producedType) {
        this.converter = converter;
        this.producedType = producedType;
    }

    @Override
    public void deserialize(Struct row, Collector<RowData> out) {
        out.collect(converter.convert(row));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
