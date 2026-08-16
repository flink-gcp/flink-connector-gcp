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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * A {@link BigQueryProtoSerializer} for Avro records.
 *
 * <p>The BigQuery table schema is derived from the Avro writer schema (see {@link
 * AvroToTableSchemaConverter} for the type mapping) and each record is rewritten into a
 * BigQuery-storage compatible row: logical types become their BigQuery counterparts, enums become
 * their symbol names, maps become repeated {@code STRUCT<key, value>}, and string fields marked
 * through {@link AvroSchemaOptions} become {@code JSON} or {@code GEOGRAPHY} columns.
 *
 * <p>Records are accepted as {@link IndexedRecord}, so this serves both {@code GenericRecord}
 * streams and generated {@code SpecificRecord} streams; values are read in whichever representation
 * the record carries.
 *
 * <p>All destinations share the one schema. Its JSON text is stored, while the derived conversion
 * state remains transient and is rebuilt on the task manager. A parsed Avro {@link Schema} is also
 * serializable, so the JSON field is not required by Flink's job-graph serialization. The schema is
 * nonetheless derived when the serializer is created, so an unmappable schema fails where the
 * pipeline is built rather than on the first record.
 *
 * <p>Conversion costs one pass over each record, unlike {@link
 * io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializer
 * ProtoMessageSerializer} on an already-protobuf stream. Where the input is under your control and
 * throughput matters, a native protobuf record avoids it.
 */
@Public
public final class AvroRecordSerializer extends BigQueryProtoSerializer<IndexedRecord> {

    private static final long serialVersionUID = 1L;

    private final String avroSchemaJson;
    private final AvroSchemaOptions options;

    private transient volatile ConversionState state;

    private AvroRecordSerializer(String avroSchemaJson, AvroSchemaOptions options) {
        this.avroSchemaJson =
                Preconditions.checkNotNull(avroSchemaJson, "avroSchemaJson must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
        // Derived here so an unmappable schema fails while the job graph is built. Left to the
        // lazy path it would first be derived from serialize(), whose exceptions the writers route
        // to the FailureHandler: one misconfiguration would look like a poison record, and a
        // log-and-drop or DLQ policy would swallow it once per record for the life of the job. The
        // state is transient, so a task manager rebuilds it after deserialization regardless.
        state();
    }

    /**
     * Creates a serializer for records of the given Avro schema.
     *
     * @param avroSchema the Avro record schema of the incoming records
     * @return the serializer
     */
    public static AvroRecordSerializer of(Schema avroSchema) {
        return of(avroSchema, AvroSchemaOptions.defaults());
    }

    /**
     * Creates a serializer for records of the given Avro schema with schema mapping options.
     *
     * @param avroSchema the Avro record schema of the incoming records
     * @param options the schema mapping options
     * @return the serializer
     */
    public static AvroRecordSerializer of(Schema avroSchema, AvroSchemaOptions options) {
        return new AvroRecordSerializer(
                Preconditions.checkNotNull(avroSchema, "avroSchema must not be null").toString(),
                options);
    }

    /**
     * Creates a serializer for records of the Avro schema given as JSON text, for jobs that carry
     * the schema as a string (from a schema registry, a resource or a configuration option).
     *
     * @param avroSchemaJson the Avro record schema, as JSON text
     * @return the serializer
     */
    public static AvroRecordSerializer of(String avroSchemaJson) {
        return of(avroSchemaJson, AvroSchemaOptions.defaults());
    }

    /**
     * Creates a serializer for records of the Avro schema given as JSON text, with schema mapping
     * options.
     *
     * @param avroSchemaJson the Avro record schema, as JSON text
     * @param options the schema mapping options
     * @return the serializer
     */
    public static AvroRecordSerializer of(String avroSchemaJson, AvroSchemaOptions options) {
        // Parsed here rather than kept as text: a malformed schema is worth reporting from the
        // call that supplied it, and both entry points then reach the constructor the same way.
        return of(
                new Schema.Parser()
                        .parse(
                                Preconditions.checkNotNull(
                                        avroSchemaJson, "avroSchemaJson must not be null")),
                options);
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
    public ByteString serialize(IndexedRecord element) throws IOException {
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
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        TableSchema tableSchema = AvroToTableSchemaConverter.convert(avroSchema, options);
        Descriptors.Descriptor rowDescriptor;
        try {
            rowDescriptor =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            tableSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor for "
                            + avroSchema.getFullName(),
                    e);
        }
        localState =
                new ConversionState(
                        tableSchema,
                        rowDescriptor,
                        new AvroRowConverter(avroSchema, tableSchema, rowDescriptor));
        state = localState;
        return localState;
    }

    /** Immutable holder published through one volatile read on the per-record path. */
    private static final class ConversionState {
        private final TableSchema tableSchema;
        private final Descriptors.Descriptor rowDescriptor;
        private final AvroRowConverter rowConverter;

        ConversionState(
                TableSchema tableSchema,
                Descriptors.Descriptor rowDescriptor,
                AvroRowConverter rowConverter) {
            this.tableSchema = tableSchema;
            this.rowDescriptor = rowDescriptor;
            this.rowConverter = rowConverter;
        }
    }
}
