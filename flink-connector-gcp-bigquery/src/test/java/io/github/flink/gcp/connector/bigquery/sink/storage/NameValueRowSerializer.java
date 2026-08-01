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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

/**
 * Test serializer for rows travelling as {@code "name|value"} strings, written into {@link #SCHEMA}
 * ({@code name STRING REQUIRED, value INT64 NULLABLE}). Shared by the storage-family real-GCP
 * ITCases, whose destination tables are created from {@link #SCHEMA} up front.
 */
final class NameValueRowSerializer extends BigQueryProtoSerializer<String> {
    private static final long serialVersionUID = 1L;

    static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("value")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private transient Descriptors.Descriptor descriptor;

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return SCHEMA;
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        return descriptor();
    }

    private Descriptors.Descriptor descriptor() {
        if (descriptor == null) {
            try {
                descriptor =
                        BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                SCHEMA);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new IllegalStateException(e);
            }
        }
        return descriptor;
    }

    @Override
    public ByteString serialize(String element) {
        String[] parts = element.split("\\|", -1);
        DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
        row.setField(descriptor().findFieldByName("name"), parts[0]);
        row.setField(descriptor().findFieldByName("value"), Long.parseLong(parts[1]));
        return row.build().toByteString();
    }
}
