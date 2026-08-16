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

package io.github.flink.gcp.connector.bigquery.sink.serializer.json;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.JsonToProtoMessage;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQuerySchemaConverter;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;

/**
 * A {@link BigQueryProtoSerializer} for records that are JSON documents, as text.
 *
 * <p>JSON carries no schema, so unlike the protobuf and Avro serializers this one cannot derive the
 * destination schema — it is supplied, either in the Storage API form the sink uses internally or
 * as the REST client's {@link Schema}, whichever the surrounding code already has. That schema is
 * the source of truth for table auto-creation, the write stream and load jobs, and every column
 * type it names is honoured, {@code JSON} columns included.
 *
 * <p>Conversion is the Storage Write API client's own {@code JsonToProtoMessage}, the same one
 * {@code JsonStreamWriter} uses, so field naming, timestamp parsing and the {@code DATETIME}/{@code
 * TIME}/{@code NUMERIC}/{@code BIGNUMERIC} encodings behave exactly as they do there.
 *
 * <p>Anything wrong with a single document — malformed JSON, a value that will not convert, a
 * missing {@code REQUIRED} column, or a field the schema does not have unless {@link
 * JsonDocumentSerializerOptions.Builder#ignoreUnknownFields()} is set — is an {@link IOException},
 * which the sink routes to the configured failure handler.
 *
 * <p>Conversion costs a JSON parse plus a pass over each record. Where the input format is yours to
 * choose and throughput matters, a native protobuf record avoids both.
 */
@Public
public final class JsonDocumentSerializer extends BigQueryProtoSerializer<String> {

    private static final long serialVersionUID = 1L;

    /**
     * The destination schema. Protobuf messages are Java-serializable, so this travels in the job
     * graph as it is; the descriptor derived from it is not, and is rebuilt on the task manager.
     */
    private final TableSchema tableSchema;

    private final JsonDocumentSerializerOptions options;

    private transient volatile Descriptors.Descriptor rowDescriptor;

    private JsonDocumentSerializer(TableSchema tableSchema, JsonDocumentSerializerOptions options) {
        this.tableSchema = Preconditions.checkNotNull(tableSchema, "tableSchema must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
        Preconditions.checkArgument(
                tableSchema.getFieldsCount() > 0, "tableSchema must have at least one field");
        // Derived here so an unusable schema fails while the job graph is built rather than on the
        // first record, where the sink would route it to the FailureHandler once per record.
        descriptor();
    }

    /**
     * Creates a serializer writing JSON documents against the given schema.
     *
     * @param tableSchema the destination schema, in the Storage API form
     * @return the serializer
     */
    public static JsonDocumentSerializer of(TableSchema tableSchema) {
        return of(tableSchema, JsonDocumentSerializerOptions.defaults());
    }

    /**
     * Creates a serializer writing JSON documents against the given schema, with options.
     *
     * @param tableSchema the destination schema, in the Storage API form
     * @param options the conversion options
     * @return the serializer
     */
    public static JsonDocumentSerializer of(
            TableSchema tableSchema, JsonDocumentSerializerOptions options) {
        return new JsonDocumentSerializer(tableSchema, options);
    }

    /**
     * Creates a serializer writing JSON documents against the given schema, for code holding the
     * REST client's schema type — a {@code Table} read back through {@code BigQuery.getTable}, for
     * example.
     *
     * @param schema the destination schema, in the REST client form
     * @return the serializer
     */
    public static JsonDocumentSerializer of(Schema schema) {
        return of(schema, JsonDocumentSerializerOptions.defaults());
    }

    /**
     * Creates a serializer writing JSON documents against the given REST client schema, with
     * options.
     *
     * @param schema the destination schema, in the REST client form
     * @param options the conversion options
     * @return the serializer
     */
    public static JsonDocumentSerializer of(Schema schema, JsonDocumentSerializerOptions options) {
        return of(
                BigQuerySchemaConverter.toStorageSchema(
                        Preconditions.checkNotNull(schema, "schema must not be null")),
                options);
    }

    @Override
    public TableSchema getTableSchema(TableDestination destination) {
        return tableSchema;
    }

    @Override
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        return descriptor();
    }

    @Override
    public ByteString serialize(String element) throws IOException {
        Preconditions.checkNotNull(element, "element must not be null");
        // Derived outside the try below, so that a schema this descriptor cannot express is not
        // reported as a bad record. On a task manager the constructor never runs — it is the
        // deserialized instance that serves records — and every writer calls serialize() before
        // getDescriptor(), so this is where the first build actually happens there.
        Descriptors.Descriptor descriptor = descriptor();
        JSONObject json = parse(element);
        if (json.isEmpty()) {
            // JsonToProtoMessage rejects this with a bare "JSONObject is empty." carrying no hint
            // of which record it was; say what happened instead.
            throw new IOException("Record is an empty JSON object, which has no columns to write");
        }
        try {
            return JsonToProtoMessage.INSTANCE
                    .convertToProtoMessage(
                            descriptor, tableSchema, json, options.isIgnoreUnknownFields())
                    .toByteString();
        } catch (RuntimeException e) {
            // The client library reports every per-row problem — an unconvertible value, a missing
            // REQUIRED column, an unknown field — as an unchecked exception. The serializer
            // contract is a checked one, so that the sink can route the row rather than fail.
            throw new IOException("Failed to convert a JSON record: " + e.getMessage(), e);
        }
    }

    /**
     * Parses one record, insisting that it is a single JSON object and nothing else.
     *
     * <p>{@code new JSONObject(String)} stops at the end of the first value and ignores whatever
     * follows, so two concatenated documents — a mis-split newline-delimited stream, the shape this
     * serializer is most likely to meet — would silently become one row and drop the rest. The
     * trailing content is checked for explicitly to make that a row-level failure instead.
     */
    private static JSONObject parse(String element) throws IOException {
        try {
            JSONTokener tokener = new JSONTokener(element);
            JSONObject json = new JSONObject(tokener);
            if (tokener.nextClean() != 0) {
                throw new IOException(
                        "Record carries more than one JSON value; a record must be a single JSON"
                                + " object");
            }
            return json;
        } catch (RuntimeException e) {
            throw new IOException("Record is not a JSON object: " + e.getMessage(), e);
        }
    }

    private Descriptors.Descriptor descriptor() {
        Descriptors.Descriptor local = rowDescriptor;
        if (local == null) {
            local = buildDescriptor();
        }
        return local;
    }

    private synchronized Descriptors.Descriptor buildDescriptor() {
        Descriptors.Descriptor local = rowDescriptor;
        if (local != null) {
            return local;
        }
        try {
            local =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            tableSchema);
        } catch (Descriptors.DescriptorValidationException e) {
            // Not reachable through any schema found so far: the library rejects what it cannot
            // express with IllegalArgumentException first (a RANGE column, for instance). Kept
            // because it is a checked exception and swallowing it would be worse than a branch no
            // test covers.
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor from the supplied"
                            + " schema",
                    e);
        }
        rowDescriptor = local;
        return local;
    }
}
