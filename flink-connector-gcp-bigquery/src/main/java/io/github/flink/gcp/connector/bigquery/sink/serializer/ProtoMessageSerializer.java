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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;

/**
 * A {@link BigQueryProtoSerializer} for records that already are protobuf messages.
 *
 * <p>The BigQuery table schema is derived from the message's descriptor (see {@link
 * ProtoSchemaConverter} for the type mapping), and each record is rewritten into a BigQuery-storage
 * compatible row: {@code google.protobuf.Timestamp} fields become {@code TIMESTAMP} (microseconds),
 * enums become their value names, and message fields configured via {@link ProtoSchemaOptions} are
 * written as {@code JSON} columns.
 *
 * <p>All destinations share the schema of {@code T}; the message class (not its non-serializable
 * descriptor) is stored, so instances survive Flink's job-graph serialization and rebuild their
 * conversion state lazily.
 *
 * @param <T> the protobuf message type of the records
 */
@PublicEvolving
public final class ProtoMessageSerializer<T extends Message> extends BigQueryProtoSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> messageClass;
    private final ProtoSchemaOptions options;

    private transient Descriptors.Descriptor rowDescriptor;
    private transient ProtoRowConverter rowConverter;

    private ProtoMessageSerializer(Class<T> messageClass, ProtoSchemaOptions options) {
        this.messageClass =
                Preconditions.checkNotNull(messageClass, "messageClass must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
    }

    /**
     * Creates a serializer for the given generated protobuf message class.
     *
     * @param messageClass the generated message class
     * @param <T> the message type
     * @return the serializer
     */
    public static <T extends Message> ProtoMessageSerializer<T> of(Class<T> messageClass) {
        return of(messageClass, ProtoSchemaOptions.defaults());
    }

    /**
     * Creates a serializer for the given generated protobuf message class with schema mapping
     * options.
     *
     * @param messageClass the generated message class
     * @param options the schema mapping options
     * @param <T> the message type
     * @return the serializer
     */
    public static <T extends Message> ProtoMessageSerializer<T> of(
            Class<T> messageClass, ProtoSchemaOptions options) {
        return new ProtoMessageSerializer<>(messageClass, options);
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        initialize();
        return rowDescriptor;
    }

    @Override
    public ByteString serialize(T element) throws IOException {
        initialize();
        return rowConverter.convert(element).toByteString();
    }

    private synchronized void initialize() {
        if (rowConverter != null) {
            return;
        }
        Descriptors.Descriptor sourceDescriptor = sourceDescriptor();
        TableSchema tableSchema = ProtoSchemaConverter.convert(sourceDescriptor, options);
        try {
            rowDescriptor =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            tableSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor for "
                            + messageClass.getName(),
                    e);
        }
        rowConverter = new ProtoRowConverter(rowDescriptor, options);
    }

    private Descriptors.Descriptor sourceDescriptor() {
        try {
            return (Descriptors.Descriptor) messageClass.getMethod("getDescriptor").invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to obtain the protobuf descriptor from "
                            + messageClass.getName()
                            + "; a generated message class is required",
                    e);
        }
    }
}
