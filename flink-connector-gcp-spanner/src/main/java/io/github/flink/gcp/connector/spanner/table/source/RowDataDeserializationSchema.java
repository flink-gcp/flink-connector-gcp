/* Copyright 2026 laughingman7743. Licensed under the Apache License, Version 2.0. */

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

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
    public RowData deserialize(Struct row) {
        return converter.convert(row);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
