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
import com.google.cloud.bigquery.storage.v1.JsonToProtoMessage;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.LazyDerivedState;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowMessageProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.RowDescriptors;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQuerySchemaConverter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Locale;

/**
 * A {@link BigQueryProtoSerializationSchema} for records that are JSON documents, as text.
 *
 * <p>JSON carries no schema, so unlike the protobuf and Avro serializers this one cannot derive the
 * destination schema — it is supplied, either in the Storage API form the sink uses internally or
 * as the REST client's {@link Schema}, whichever the surrounding code already has. That schema is
 * the source of truth for table auto-creation, the write stream and load jobs, and every column
 * type it names is honoured, {@code JSON} columns included.
 *
 * <p>Conversion is the Storage Write API client's own {@code JsonToProtoMessage}, the same one
 * {@code JsonStreamWriter} uses, so field naming, timestamp parsing and the {@code DATETIME}/{@code
 * TIME}/{@code NUMERIC}/{@code BIGNUMERIC} encodings behave exactly as they do there. Keys are
 * lower-cased as there too, with the JVM's default locale, but matched against a descriptor whose
 * names are lower-cased under {@link Locale#ROOT}. Under a Turkish default locale the key {@code
 * userId} therefore becomes {@code userıd} and names no column, so the record fails or, with {@link
 * JsonDocumentOptions.Builder#ignoreUnknownFields()}, loses the value, where the client alone would
 * send a field BigQuery rejects. {@link
 * JsonDocumentOptions.Builder#normalizeKeysForDefaultLocale()} renames such keys first, so that
 * keys are matched to columns case-insensitively whatever the default locale.
 *
 * <p>Anything wrong with a single document — malformed JSON, a value that will not convert, a
 * missing {@code REQUIRED} column, or a field the schema does not have unless {@link
 * JsonDocumentOptions.Builder#ignoreUnknownFields()} is set — is an {@link IOException}, which the
 * sink routes to the configured failure handler.
 *
 * <p>Conversion costs a JSON parse plus a pass over each record. Where the input format is yours to
 * choose and throughput matters, a native protobuf record avoids both.
 */
@Public
public final class JsonDocumentSerializationSchema extends BigQueryProtoSerializationSchema<String>
        implements ProtoRowMessageProvider<String> {

    private static final long serialVersionUID = 1L;

    /**
     * The destination schema. Protobuf messages are Java-serializable, so this travels in the job
     * graph as it is; the descriptor derived from it is not, and is rebuilt on the task manager.
     */
    private final TableSchema tableSchema;

    private final JsonDocumentOptions options;

    private final LazyDerivedState<ConversionState> conversionState = new LazyDerivedState<>();

    private JsonDocumentSerializationSchema(TableSchema tableSchema, JsonDocumentOptions options) {
        this.tableSchema = Preconditions.checkNotNull(tableSchema, "tableSchema must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
        Preconditions.checkArgument(
                tableSchema.getFieldsCount() > 0, "tableSchema must have at least one field");
        // Derived here so an unusable schema fails while the job graph is built rather than on the
        // first record, where the sink would route it to the FailureHandler once per record.
        state();
    }

    /**
     * Creates a serializer writing JSON documents against the given schema.
     *
     * @param tableSchema the destination schema, in the Storage API form
     * @return the serializer
     */
    public static JsonDocumentSerializationSchema of(TableSchema tableSchema) {
        return of(tableSchema, JsonDocumentOptions.defaults());
    }

    /**
     * Creates a serializer writing JSON documents against the given schema, with options.
     *
     * @param tableSchema the destination schema, in the Storage API form
     * @param options the conversion options
     * @return the serializer
     */
    public static JsonDocumentSerializationSchema of(
            TableSchema tableSchema, JsonDocumentOptions options) {
        return new JsonDocumentSerializationSchema(tableSchema, options);
    }

    /**
     * Creates a serializer writing JSON documents against the given schema, for code holding the
     * REST client's schema type — a {@code Table} read back through {@code BigQuery.getTable}, for
     * example.
     *
     * @param schema the destination schema, in the REST client form
     * @return the serializer
     */
    public static JsonDocumentSerializationSchema of(Schema schema) {
        return of(schema, JsonDocumentOptions.defaults());
    }

    /**
     * Creates a serializer writing JSON documents against the given REST client schema, with
     * options.
     *
     * @param schema the destination schema, in the REST client form
     * @param options the conversion options
     * @return the serializer
     */
    public static JsonDocumentSerializationSchema of(Schema schema, JsonDocumentOptions options) {
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
        return state().descriptor;
    }

    @Override
    public ByteString serialize(String element) throws IOException {
        return serializeToMessage(element).toByteString();
    }

    /**
     * Converts a record into its protobuf row message, the row {@link #serialize} encodes.
     *
     * <p>The sink calls this to hand the row to a write method that walks the message rather than
     * its bytes. A serializer that delegates to this one forwards it to take the same path.
     *
     * @param element the record
     * @return the row message
     * @throws IOException if the record cannot be converted
     */
    @Override
    public Message serializeToMessage(String element) throws IOException {
        Preconditions.checkNotNull(element, "element must not be null");
        // Derived outside the try below, so that a schema no wire descriptor can be derived from
        // fails as itself rather than being relabelled "Failed to convert a JSON record" — it is a
        // configuration error, and the record is not what is wrong with it. On a task manager the
        // constructor never runs (the deserialized instance serves records), so the first build
        // happens here unless a writer resolved it earlier. Two writer paths do, and both are
        // outside per-row handling: the buffered writer resolves a descriptor per restored
        // destination while it is being constructed, and BigQuerySinkConfig#prepareWriteSchema
        // resolves one ahead of every record whenever additional fields or CDC put an augmenting
        // serializer in front of this one.
        ConversionState state = state();
        JSONObject json = parse(element);
        if (json.isEmpty()) {
            // JsonToProtoMessage rejects this with a bare "JSONObject is empty." carrying no hint
            // of which record it was; say what happened instead.
            throw new IOException("Record is an empty JSON object, which has no columns to write");
        }
        // JsonToProtoMessage lower-cases each key with the default locale, read here per record as
        // the library reads it; see JsonKeyNormalizer.
        if (options.isNormalizeKeysForDefaultLocale()) {
            state.keyNormalizer.normalize(json, Locale.getDefault());
        }
        try {
            return JsonToProtoMessage.INSTANCE.convertToProtoMessage(
                    state.descriptor,
                    state.conversionSchema,
                    json,
                    options.isIgnoreUnknownFields());
        } catch (RuntimeException e) {
            // The client library reports every per-row problem — an unconvertible value, a missing
            // REQUIRED column, an unknown field — as an unchecked exception. The serializer
            // contract is a checked one, so that the sink can route the row rather than fail.
            throw new IOException(
                    "Failed to convert a JSON record: " + e.getMessage() + localeHint(json), e);
        }
    }

    /**
     * Points at {@link JsonDocumentOptions.Builder#normalizeKeysForDefaultLocale()} when it is off
     * and the document holds a key the default locale lower-cases differently from {@link
     * Locale#ROOT}, which the library may have reported as unknown although it spells a column.
     * Only a record that already failed is searched, so no other record pays for it.
     */
    private String localeHint(JSONObject json) {
        if (options.isNormalizeKeysForDefaultLocale()) {
            return "";
        }
        Locale locale = Locale.getDefault();
        String key = keyLowerCasedDifferently(json, locale);
        return key == null
                ? ""
                : " (the JVM's default locale "
                        + locale.toLanguageTag()
                        + " lower-cases the key '"
                        + key
                        + "' differently from Locale.ROOT; JsonDocumentOptions.builder()"
                        + ".normalizeKeysForDefaultLocale() renames such keys)";
    }

    /** Returns a key, at any depth, that {@code locale} lower-cases differently from ROOT. */
    @Nullable
    private static String keyLowerCasedDifferently(@Nullable Object value, Locale locale) {
        if (value instanceof JSONObject) {
            JSONObject object = (JSONObject) value;
            for (String key : object.keySet()) {
                if (!key.toLowerCase(locale).equals(key.toLowerCase(Locale.ROOT))) {
                    return key;
                }
                String nested = keyLowerCasedDifferently(object.opt(key), locale);
                if (nested != null) {
                    return nested;
                }
            }
        } else if (value instanceof JSONArray) {
            for (Object element : (JSONArray) value) {
                String nested = keyLowerCasedDifferently(element, locale);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
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

    private ConversionState state() {
        return conversionState.get(this, JsonDocumentSerializationSchema::deriveConversionState);
    }

    private ConversionState deriveConversionState() {
        return new ConversionState(
                RowDescriptors.derive(tableSchema, "the supplied schema"),
                RowDescriptors.lowerCased(tableSchema),
                JsonKeyNormalizer.of(tableSchema));
    }

    /** What {@link LazyDerivedState} holds for this serializer. */
    private static final class ConversionState {
        private final Descriptors.Descriptor descriptor;
        private final TableSchema conversionSchema;
        private final JsonKeyNormalizer keyNormalizer;

        ConversionState(
                Descriptors.Descriptor descriptor,
                TableSchema conversionSchema,
                JsonKeyNormalizer keyNormalizer) {
            this.descriptor = descriptor;
            this.conversionSchema = conversionSchema;
            this.keyNormalizer = keyNormalizer;
        }
    }
}
