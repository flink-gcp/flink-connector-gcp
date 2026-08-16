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

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JsonDocumentSerializer}. */
class JsonDocumentSerializerTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "dataset", "table");

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    private static TableSchema schema() {
        return TableSchema.newBuilder()
                .addFields(
                        field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED))
                .addFields(field("n", TableFieldSchema.Type.INT64, TableFieldSchema.Mode.NULLABLE))
                .build();
    }

    private static DynamicMessage row(JsonDocumentSerializer serializer, String json)
            throws IOException {
        return DynamicMessage.parseFrom(
                serializer.getDescriptor(DESTINATION), serializer.serialize(json));
    }

    private static Object value(DynamicMessage message, String field) {
        return message.getField(message.getDescriptorForType().findFieldByName(field));
    }

    @Test
    void convertsJsonDocumentsAgainstTheSuppliedSchema() throws Exception {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        DynamicMessage row = row(serializer, "{\"name\":\"alice\",\"n\":3}");

        assertThat(value(row, "name")).isEqualTo("alice");
        assertThat(value(row, "n")).isEqualTo(3L);
    }

    @Test
    void theSuppliedSchemaIsTheSchemaTheSinkSees() {
        assertThat(JsonDocumentSerializer.of(schema()).getTableSchema(DESTINATION))
                .isEqualTo(schema());
    }

    @Test
    void acceptsTheRestClientSchemaForm() {
        JsonDocumentSerializer serializer =
                JsonDocumentSerializer.of(
                        com.google.cloud.bigquery.Schema.of(
                                Field.newBuilder("name", StandardSQLTypeName.STRING)
                                        .setMode(Field.Mode.REQUIRED)
                                        .build(),
                                Field.newBuilder("n", StandardSQLTypeName.INT64)
                                        .setMode(Field.Mode.NULLABLE)
                                        .build()));

        assertThat(serializer.getTableSchema(DESTINATION)).isEqualTo(schema());
    }

    @Test
    void honoursEveryColumnTypeTheSchemaNames() throws Exception {
        TableSchema wide =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "ts",
                                        TableFieldSchema.Type.TIMESTAMP,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "dt",
                                        TableFieldSchema.Type.DATETIME,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "num",
                                        TableFieldSchema.Type.NUMERIC,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "payload",
                                        TableFieldSchema.Type.JSON,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(wide);

        DynamicMessage row =
                row(
                        serializer,
                        "{\"ts\":\"2026-07-26T01:02:03.456789Z\",\"dt\":\"2026-07-26 01:02:03\","
                                + "\"num\":\"1.25\",\"payload\":\"{\\\"k\\\":1}\"}");

        Instant ts = Instant.parse("2026-07-26T01:02:03.456789Z");
        assertThat(value(row, "ts"))
                .isEqualTo(ts.getEpochSecond() * 1_000_000L + ts.getNano() / 1_000L);
        assertThat(
                        CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                (Long) value(row, "dt")))
                .isEqualTo(LocalDateTime.of(2026, 7, 26, 1, 2, 3));
        assertThat(
                        BigDecimalByteStringEncoder.decodeNumericByteString(
                                (ByteString) value(row, "num")))
                .isEqualByComparingTo(new BigDecimal("1.25"));
        assertThat(value(row, "payload")).isEqualTo("{\"k\":1}");
    }

    @Test
    void jsonNullLeavesTheColumnUnset() throws Exception {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        DynamicMessage row = row(serializer, "{\"name\":\"alice\",\"n\":null}");

        assertThat(row.hasField(row.getDescriptorForType().findFieldByName("n"))).isFalse();
    }

    @Test
    void malformedJsonIsRowLevelFailure() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        assertThatThrownBy(() -> serializer.serialize("{not json"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a JSON object");
        assertThatThrownBy(() -> serializer.serialize("[1,2]"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a JSON object");
    }

    @Test
    void emptyJsonObjectIsRowLevelFailureWithItsOwnMessage() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        // The client library's own message for this is a bare "JSONObject is empty."
        assertThatThrownBy(() -> serializer.serialize("{}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty JSON object");
    }

    @Test
    void missingRequiredColumnIsRowLevelFailure() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        assertThatThrownBy(() -> serializer.serialize("{\"n\":1}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("name");
    }

    @Test
    void valueThatWillNotConvertIsRowLevelFailure() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        // "n" alone would match the wrapper prefix and every other failure this class throws.
        assertThatThrownBy(() -> serializer.serialize("{\"name\":\"a\",\"n\":\"not a number\"}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("root.n")
                .hasMessageContaining("INT64");
    }

    @Test
    void unknownFieldFailsTheRecordByDefault() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        assertThatThrownBy(() -> serializer.serialize("{\"name\":\"a\",\"extra\":1}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("extra");
    }

    @Test
    void unknownFieldIsDroppedWhenAskedFor() throws Exception {
        JsonDocumentSerializer serializer =
                JsonDocumentSerializer.of(
                        schema(),
                        JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());

        DynamicMessage row = row(serializer, "{\"name\":\"a\",\"extra\":1}");

        assertThat(value(row, "name")).isEqualTo("a");
    }

    @Test
    void cachesTheDerivedDescriptor() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        assertThat(serializer.getDescriptor(DESTINATION))
                .isSameAs(serializer.getDescriptor(DESTINATION));
    }

    @Test
    void schemaIsStaticSoThereIsNoFingerprint() {
        assertThat(JsonDocumentSerializer.of(schema()).getSchemaFingerprint(DESTINATION)).isNull();
    }

    @Test
    void survivesJobGraphSerializationCarryingItsOptions() throws Exception {
        JsonDocumentSerializer original =
                JsonDocumentSerializer.of(
                        schema(),
                        JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());
        // Use it first, so the transient descriptor exists and has to be rebuilt.
        original.serialize("{\"name\":\"a\"}");

        JsonDocumentSerializer copy = InstantiationUtil.clone(original);

        assertThat(copy.getTableSchema(DESTINATION)).isEqualTo(schema());
        assertThat(copy.serialize("{\"name\":\"b\",\"extra\":1}"))
                .isEqualTo(original.serialize("{\"name\":\"b\",\"extra\":1}"));
    }

    @Test
    void unusableSchemasFailWhenTheSerializerIsCreated() {
        TableSchema unsupported =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "r",
                                        TableFieldSchema.Type.RANGE,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();

        assertThatThrownBy(() -> JsonDocumentSerializer.of(unsupported))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEmptyAndNullSchemas() {
        assertThatThrownBy(() -> JsonDocumentSerializer.of(TableSchema.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one field");
        assertThatThrownBy(() -> JsonDocumentSerializer.of((TableSchema) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> JsonDocumentSerializer.of((com.google.cloud.bigquery.Schema) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> JsonDocumentSerializer.of(schema(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullRecords() {
        assertThatThrownBy(() -> JsonDocumentSerializer.of(schema()).serialize(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void descriptorFieldsCarryTheSchemaFieldNames() {
        Descriptors.Descriptor descriptor =
                JsonDocumentSerializer.of(schema()).getDescriptor(DESTINATION);

        assertThat(descriptor.getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("name", "n");
    }

    @Test
    void bytesColumnsTakeAJsonArrayOfByteValuesAndNotBase64() throws Exception {
        TableSchema withBytes =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "blob",
                                        TableFieldSchema.Type.BYTES,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(withBytes);

        DynamicMessage row = row(serializer, "{\"blob\":[104,105]}");
        assertThat(value(row, "blob")).isEqualTo(ByteString.copyFromUtf8("hi"));

        // Pinned deliberately: protobuf's own canonical JSON mapping encodes bytes as a base64
        // string, and the client library's converter does not accept one. A reader hitting this
        // should find the limitation stated rather than guess.
        assertThatThrownBy(() -> serializer.serialize("{\"blob\":\"aGk=\"}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("blob");
    }

    @Test
    void aRecordCarryingMoreThanOneJsonValueIsRowLevelFailure() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        // Two concatenated documents — a mis-split newline-delimited stream. Parsing only the
        // first and dropping the rest would be silent data loss.
        assertThatThrownBy(() -> serializer.serialize("{\"name\":\"a\"}{\"name\":\"b\"}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("more than one JSON value");
        assertThatThrownBy(() -> serializer.serialize("{\"name\":\"a\"} trailing"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("more than one JSON value");
    }

    @Test
    void trailingWhitespaceIsFine() throws Exception {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        assertThat(value(row(serializer, "  {\"name\":\"a\"}\n\n"), "name")).isEqualTo("a");
    }

    @Test
    void aJsonColumnTakesJsonTextAndNotANestedObject() throws Exception {
        TableSchema withJson =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "payload",
                                        TableFieldSchema.Type.JSON,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(withJson);

        assertThat(value(row(serializer, "{\"payload\":\"{\\\"k\\\":1}\"}"), "payload"))
                .isEqualTo("{\"k\":1}");

        // Pinned deliberately: nesting the object is the obvious thing to write in a JSON
        // document, and the client library's converter wants the text of it instead.
        assertThatThrownBy(() -> serializer.serialize("{\"payload\":{\"k\":1}}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("root.payload");
    }

    /**
     * A {@code GEOGRAPHY} column needs no marker option here — the supplied schema already says so,
     * which is why this serializer was never part of the gap the marker options close. Untested
     * until now all the same.
     */
    @Test
    void aGeographyColumnTakesItsTextForm() throws Exception {
        TableSchema withGeography =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "boundary",
                                        TableFieldSchema.Type.GEOGRAPHY,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(withGeography);

        assertThat(value(row(serializer, "{\"boundary\":\"POINT(1 2)\"}"), "boundary"))
                .isEqualTo("POINT(1 2)");

        // Pinned for the same reason as the JSON column above, and the trap is sharper here: the
        // accepted forms include GeoJSON, so nesting the object is the obvious thing to write in a
        // JSON document — and it is not what the column takes.
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        "{\"boundary\":{\"type\":\"Point\",\"coordinates\":[1,2]}}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("root.boundary");
    }

    @Test
    void aNumberInATemporalColumnIsTakenAsItsStorageEncoding() throws Exception {
        TableSchema withTimestamp =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "t",
                                        TableFieldSchema.Type.TIMESTAMP,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(withTimestamp);

        // Pinned deliberately, and the trap worth knowing about: a TIMESTAMP column reads a bare
        // number as epoch *microseconds*, so the two encodings a JSON document usually carries —
        // epoch seconds and epoch millis — are accepted and stored as some other instant.
        assertThat(value(row(serializer, "{\"t\":1700000000}"), "t")).isEqualTo(1_700_000_000L);
        assertThat(value(row(serializer, "{\"t\":\"2023-11-14T22:13:20Z\"}"), "t"))
                .isEqualTo(1_700_000_000_000_000L);
    }

    @Test
    void keysAreMatchedToColumnsWithoutRegardToCase() throws Exception {
        TableSchema mixedCase =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "userName",
                                        TableFieldSchema.Type.STRING,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(mixedCase);

        // The descriptor lowercases, and the converter matches case-insensitively — so a key that
        // does not match the column's spelling is not an "unknown field".
        assertThat(serializer.getDescriptor(DESTINATION).getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("username");
        assertThat(value(row(serializer, "{\"USERNAME\":\"a\"}"), "username")).isEqualTo("a");
        assertThat(value(row(serializer, "{\"userName\":\"b\"}"), "username")).isEqualTo("b");
    }

    @Test
    void aConvertedFailureCarriesTheLibrarysOwnDiagnostic() {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        // Pins the shape of what a FailedRow carries. The client library's message is a map keyed
        // by row index, which reads oddly for a serializer handling one record at a time — but it
        // is where the field path and the reason live, so it is passed through rather than parsed.
        assertThatThrownBy(() -> serializer.serialize("{\"n\":1}"))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Failed to convert a JSON record: The map of row index to error message"
                                + " is {0=JSONObject does not have the required field root.name.}");
    }

    @Test
    void optionsReachConversionThroughTheRestSchemaForm() throws Exception {
        JsonDocumentSerializer serializer =
                JsonDocumentSerializer.of(
                        com.google.cloud.bigquery.Schema.of(
                                Field.newBuilder("name", StandardSQLTypeName.STRING)
                                        .setMode(Field.Mode.REQUIRED)
                                        .build()),
                        JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());

        assertThat(value(row(serializer, "{\"name\":\"a\",\"extra\":1}"), "name")).isEqualTo("a");
    }

    @Test
    void keysDifferingOnlyByCaseCollapseIntoOneColumn() throws Exception {
        JsonDocumentSerializer serializer = JsonDocumentSerializer.of(schema());

        // Pinned deliberately: matching is case-insensitive, so these are not two fields and
        // neither is "unknown". One value wins and which one is not defined — org.json holds the
        // keys in a HashMap, so it is not even document order. Nothing here can detect it.
        assertThat(value(row(serializer, "{\"name\":\"a\",\"NAME\":\"b\"}"), "name"))
                .isIn("a", "b");
    }
}
