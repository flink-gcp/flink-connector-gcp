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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

/**
 * Test serializer with a fixed one-column ({@code name STRING}) schema, writing rows via {@link
 * DynamicMessage}; shared by the emulator integration tests.
 */
final class NameColumnSerializer extends BigQueryProtoSerializer<String> {
    private static final long serialVersionUID = 1L;

    private transient Descriptors.Descriptor descriptor;

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return TableSchema.newBuilder()
                .addFields(
                        TableFieldSchema.newBuilder()
                                .setName("name")
                                .setType(TableFieldSchema.Type.STRING)
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .build())
                .build();
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        if (descriptor == null) {
            descriptor = super.getDescriptor(destination);
        }
        return descriptor;
    }

    @Override
    public ByteString serialize(String element) {
        Descriptors.Descriptor d = getDescriptor(null);
        return DynamicMessage.newBuilder(d)
                .setField(d.findFieldByName("name"), element)
                .build()
                .toByteString();
    }
}
