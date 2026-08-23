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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;

/**
 * Test serializer over a mutable schema, writing rows via {@link DynamicMessage}. Records are
 * {@code "name"} or {@code "name:note"}; the note is written only while the schema has the column.
 * The schema fingerprint is a version counter bumped by {@link #evolveTo(TableSchema)}, which is
 * what lets the writer detect the evolution mid-stream.
 *
 * <p>Shared by the emulator schema-evolution ITCase ({@link BigQuerySchemaEvolutionITCase}) and the
 * default- and buffered-stream real-GCP cases.
 */
final class EvolvingSerializer extends BigQueryProtoSerializationSchema<String> {
    private static final long serialVersionUID = 1L;

    private TableSchema schema;
    private int version;
    private transient Descriptors.Descriptor descriptor;

    EvolvingSerializer(TableSchema schema) {
        this.schema = schema;
    }

    void evolveTo(TableSchema newSchema) {
        this.schema = newSchema;
        this.version++;
        this.descriptor = null;
    }

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return schema;
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        if (descriptor == null) {
            descriptor = super.getDescriptor(destination);
        }
        return descriptor;
    }

    @Override
    public Object getSchemaFingerprint(TableDestination destination) {
        return version;
    }

    @Override
    public ByteString serialize(String element) {
        String[] parts = element.split(":", 2);
        Descriptors.Descriptor d = getDescriptor(null);
        DynamicMessage.Builder message =
                DynamicMessage.newBuilder(d).setField(d.findFieldByName("name"), parts[0]);
        Descriptors.FieldDescriptor note = d.findFieldByName("note");
        if (note != null && parts.length > 1) {
            message.setField(note, parts[1]);
        }
        return message.build().toByteString();
    }
}
