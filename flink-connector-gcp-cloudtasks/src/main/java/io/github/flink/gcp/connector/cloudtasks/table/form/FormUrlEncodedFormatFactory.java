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

package io.github.flink.gcp.connector.cloudtasks.table.form;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.cloudtasks.table.HttpContentTypeEncodingFormat;

import java.util.Collections;
import java.util.Set;

/** Table format factory for UTF-8 {@code application/x-www-form-urlencoded} bodies. */
@Internal
public final class FormUrlEncodedFormatFactory implements SerializationFormatFactory {

    public static final String IDENTIFIER = "form-urlencoded";
    public static final String CONTENT_TYPE = "application/x-www-form-urlencoded";

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return new FormUrlEncodedEncodingFormat();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    private static final class FormUrlEncodedEncodingFormat
            implements HttpContentTypeEncodingFormat {

        @Override
        public String getContentType() {
            return CONTENT_TYPE;
        }

        @Override
        public void validatePhysicalDataType(DataType physicalDataType) {
            LogicalType logicalType = physicalDataType.getLogicalType();
            if (!(logicalType instanceof RowType)) {
                throw new ValidationException(
                        String.format(
                                "Format '%s' requires a physical ROW type, but found %s.",
                                IDENTIFIER, logicalType.asSummaryString()));
            }
            RowType rowType = (RowType) logicalType;
            for (RowType.RowField field : rowType.getFields()) {
                if (!isSupported(field.getType())) {
                    throw new ValidationException(
                            String.format(
                                    "Format '%s' supports only STRING and ARRAY<STRING> physical"
                                            + " columns, but column '%s' has type %s. Cast the value"
                                            + " to STRING explicitly in SQL.",
                                    IDENTIFIER,
                                    field.getName(),
                                    field.getType().asSummaryString()));
                }
            }
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType physicalDataType) {
            validatePhysicalDataType(physicalDataType);
            return new FormUrlEncodedRowDataSerializationSchema(
                    (RowType) physicalDataType.getLogicalType());
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        private static boolean isSupported(LogicalType type) {
            if (type.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                return true;
            }
            return type.getTypeRoot() == LogicalTypeRoot.ARRAY
                    && ((ArrayType) type).getElementType().getTypeRoot() == LogicalTypeRoot.VARCHAR;
        }
    }
}
