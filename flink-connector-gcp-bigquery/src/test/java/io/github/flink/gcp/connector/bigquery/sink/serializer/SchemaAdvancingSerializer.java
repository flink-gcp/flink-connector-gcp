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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

/**
 * Test serializer whose schema changes while it serializes a record, which the Storage Write API
 * writers detect through the fingerprint. A record is written as the STRING column {@code value}.
 * After {@link #addColumnOnNextSerialize(String)}, the next {@link #serialize(String)} first
 * appends the named STRING column and advances the fingerprint, so the writer's {@code
 * prepareWriteSchema} has already run against the previous schema when the new one appears.
 */
public final class SchemaAdvancingSerializer extends BigQueryProtoSerializationSchema<String> {
    private static final long serialVersionUID = 1L;

    private TableSchema schema = TableSchema.newBuilder().addFields(column("value")).build();
    private int version;
    @Nullable private String pendingColumn;
    private transient Descriptors.Descriptor descriptor;

    private static TableFieldSchema column(String name) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRING)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    /** Makes the next {@link #serialize(String)} append the column before it writes the record. */
    public void addColumnOnNextSerialize(String name) {
        pendingColumn = name;
    }

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return schema;
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        if (descriptor == null) {
            try {
                descriptor =
                        BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                schema);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new AssertionError(e);
            }
        }
        return descriptor;
    }

    @Override
    public Object getSchemaFingerprint(TableDestination destination) {
        return version;
    }

    @Override
    public ByteString serialize(String element) {
        if (pendingColumn != null) {
            schema = schema.toBuilder().addFields(column(pendingColumn)).build();
            pendingColumn = null;
            version++;
            descriptor = null;
        }
        Descriptors.Descriptor d = getDescriptor(null);
        return DynamicMessage.newBuilder(d)
                .setField(d.findFieldByName("value"), element)
                .build()
                .toByteString();
    }
}
