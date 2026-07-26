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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

import java.io.IOException;

/**
 * A {@link BigQueryProtoSerializer} for records that already are protobuf messages.
 *
 * <p>The BigQuery table schema is derived from the message's descriptor (see {@link
 * ProtoToTableSchemaConverter} for the type mapping), and each record is rewritten into a
 * BigQuery-storage compatible row: {@code google.protobuf.Timestamp} fields become {@code
 * TIMESTAMP} (microseconds) and enums become their value names.
 *
 * <p>{@link ProtoSchemaOptions} adjusts two things about the derived schema: which fields become
 * {@code JSON} columns, and whether column modes come from field presence instead of every
 * non-repeated column being {@code NULLABLE} (see {@link
 * ProtoSchemaOptions.Builder#deriveRequiredColumns()}).
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

    private transient volatile ConversionState state;

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
    public TableSchema getTableSchema(TableDestination destination) {
        return state().tableSchema;
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        return state().rowDescriptor;
    }

    @Override
    public ByteString serialize(T element) throws IOException {
        return state().rowConverter.convert(element).toByteString();
    }

    private ConversionState state() {
        ConversionState localState = state;
        if (localState == null) {
            localState = initialize();
        }
        return localState;
    }

    private synchronized ConversionState initialize() {
        ConversionState localState = state;
        if (localState != null) {
            return localState;
        }
        Descriptors.Descriptor sourceDescriptor = sourceDescriptor();
        TableSchema tableSchema = ProtoToTableSchemaConverter.convert(sourceDescriptor, options);
        Descriptors.Descriptor rowDescriptor;
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
        localState =
                new ConversionState(
                        tableSchema,
                        rowDescriptor,
                        new ProtoRowConverter(sourceDescriptor, rowDescriptor, options));
        state = localState;
        return localState;
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

    /** Immutable holder published through one volatile read on the per-record path. */
    private static final class ConversionState {
        private final TableSchema tableSchema;
        private final Descriptors.Descriptor rowDescriptor;
        private final ProtoRowConverter rowConverter;

        ConversionState(
                TableSchema tableSchema,
                Descriptors.Descriptor rowDescriptor,
                ProtoRowConverter rowConverter) {
            this.tableSchema = tableSchema;
            this.rowDescriptor = rowDescriptor;
            this.rowConverter = rowConverter;
        }
    }
}
