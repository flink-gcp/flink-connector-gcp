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
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JsonDocumentSerializationSchema}. */
class JsonDocumentSerializationSchemaTest {

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

    private static DynamicMessage row(JsonDocumentSerializationSchema serializer, String json)
            throws IOException {
        return DynamicMessage.parseFrom(
                serializer.getDescriptor(DESTINATION), serializer.serialize(json));
    }

    private static Object value(DynamicMessage message, String field) {
        return message.getField(message.getDescriptorForType().findFieldByName(field));
    }

    @Test
    void convertsJsonDocumentsAgainstTheSuppliedSchema() throws Exception {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        DynamicMessage row = row(serializer, "{\"name\":\"alice\",\"n\":3}");

        assertThat(value(row, "name")).isEqualTo("alice");
        assertThat(value(row, "n")).isEqualTo(3L);
    }

    @Test
    void theSuppliedSchemaIsTheSchemaTheSinkSees() {
        assertThat(JsonDocumentSerializationSchema.of(schema()).getTableSchema(DESTINATION))
                .isEqualTo(schema());
    }

    @Test
    void acceptsTheRestClientSchemaForm() {
        JsonDocumentSerializationSchema serializer =
                JsonDocumentSerializationSchema.of(
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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(wide);

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        DynamicMessage row = row(serializer, "{\"name\":\"alice\",\"n\":null}");

        assertThat(row.hasField(row.getDescriptorForType().findFieldByName("n"))).isFalse();
    }

    @Test
    void malformedJsonIsRowLevelFailure() {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        assertThatThrownBy(() -> serializer.serialize("{not json"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a JSON object");
        assertThatThrownBy(() -> serializer.serialize("[1,2]"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a JSON object");
    }

    @Test
    void emptyJsonObjectIsRowLevelFailureWithItsOwnMessage() {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        // The client library's own message for this is a bare "JSONObject is empty."
        assertThatThrownBy(() -> serializer.serialize("{}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty JSON object");
    }

    @Test
    void missingRequiredColumnIsRowLevelFailure() {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        assertThatThrownBy(() -> serializer.serialize("{\"n\":1}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("name");
    }

    @Test
    void valueThatWillNotConvertIsRowLevelFailure() {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        // "n" alone would match the wrapper prefix and every other failure this class throws.
        assertThatThrownBy(() -> serializer.serialize("{\"name\":\"a\",\"n\":\"not a number\"}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("root.n")
                .hasMessageContaining("INT64");
    }

    @Test
    void unknownFieldFailsTheRecordByDefault() {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        assertThatThrownBy(() -> serializer.serialize("{\"name\":\"a\",\"extra\":1}"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("extra");
    }

    @Test
    void unknownFieldIsDroppedWhenAskedFor() throws Exception {
        JsonDocumentSerializationSchema serializer =
                JsonDocumentSerializationSchema.of(
                        schema(), JsonDocumentOptions.builder().ignoreUnknownFields().build());

        DynamicMessage row = row(serializer, "{\"name\":\"a\",\"extra\":1}");

        assertThat(value(row, "name")).isEqualTo("a");
    }

    @Test
    void cachesTheDerivedDescriptor() {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        assertThat(serializer.getDescriptor(DESTINATION))
                .isSameAs(serializer.getDescriptor(DESTINATION));
    }

    @Test
    void schemaIsStaticSoThereIsNoFingerprint() {
        assertThat(JsonDocumentSerializationSchema.of(schema()).getSchemaFingerprint(DESTINATION))
                .isNull();
    }

    @Test
    void survivesJobGraphSerializationCarryingItsOptions() throws Exception {
        JsonDocumentSerializationSchema original =
                JsonDocumentSerializationSchema.of(
                        schema(), JsonDocumentOptions.builder().ignoreUnknownFields().build());
        // Use it first, so the transient descriptor exists and has to be rebuilt.
        original.serialize("{\"name\":\"a\"}");

        JsonDocumentSerializationSchema copy = InstantiationUtil.clone(original);

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

        assertThatThrownBy(() -> JsonDocumentSerializationSchema.of(unsupported))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void columnsThatCollideOnceLowerCasedFailNamingTheSuppliedSchema() {
        // A supplied schema is not read back from a table, so it can carry two columns BigQuery
        // itself would refuse; the generated descriptor lower-cases both to one field name.
        TableSchema ambiguous =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "count",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "COUNT",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();

        assertThatThrownBy(() -> JsonDocumentSerializationSchema.of(ambiguous))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Failed to derive a BigQuery-storage compatible descriptor for the supplied"
                                + " schema");
    }

    @Test
    void rejectsEmptyAndNullSchemas() {
        assertThatThrownBy(
                        () -> JsonDocumentSerializationSchema.of(TableSchema.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one field");
        assertThatThrownBy(() -> JsonDocumentSerializationSchema.of((TableSchema) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                JsonDocumentSerializationSchema.of(
                                        (com.google.cloud.bigquery.Schema) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> JsonDocumentSerializationSchema.of(schema(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullRecords() {
        assertThatThrownBy(() -> JsonDocumentSerializationSchema.of(schema()).serialize(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void descriptorFieldsCarryTheSchemaFieldNames() {
        Descriptors.Descriptor descriptor =
                JsonDocumentSerializationSchema.of(schema()).getDescriptor(DESTINATION);

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(withBytes);

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(withJson);

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
        JsonDocumentSerializationSchema serializer =
                JsonDocumentSerializationSchema.of(withGeography);

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
        JsonDocumentSerializationSchema serializer =
                JsonDocumentSerializationSchema.of(withTimestamp);

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(mixedCase);

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
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

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
        JsonDocumentSerializationSchema serializer =
                JsonDocumentSerializationSchema.of(
                        com.google.cloud.bigquery.Schema.of(
                                Field.newBuilder("name", StandardSQLTypeName.STRING)
                                        .setMode(Field.Mode.REQUIRED)
                                        .build()),
                        JsonDocumentOptions.builder().ignoreUnknownFields().build());

        assertThat(value(row(serializer, "{\"name\":\"a\",\"extra\":1}"), "name")).isEqualTo("a");
    }

    @Test
    void keysDifferingOnlyByCaseCollapseIntoOneColumn() throws Exception {
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema());

        // Pinned deliberately: matching is case-insensitive, so these are not two fields and
        // neither is "unknown". One value wins and which one is not defined — org.json holds the
        // keys in a HashMap, so it is not even document order. Nothing here can detect it.
        assertThat(value(row(serializer, "{\"name\":\"a\",\"NAME\":\"b\"}"), "name"))
                .isIn("a", "b");
    }

    /**
     * Columns whose names hold an upper-case {@code I}, nested and repeated too, and a JSON one.
     */
    private static TableSchema turkishTrap() {
        return TableSchema.newBuilder()
                .addFields(
                        field(
                                "userId",
                                TableFieldSchema.Type.INT64,
                                TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field("Iban", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field("Items", TableFieldSchema.Type.STRUCT, TableFieldSchema.Mode.REPEATED)
                                .toBuilder()
                                .addFields(
                                        field(
                                                "innerValue",
                                                TableFieldSchema.Type.STRING,
                                                TableFieldSchema.Mode.NULLABLE))
                                .addFields(
                                        field(
                                                        "Info",
                                                        TableFieldSchema.Type.STRUCT,
                                                        TableFieldSchema.Mode.NULLABLE)
                                                .toBuilder()
                                                .addFields(
                                                        field(
                                                                "Id",
                                                                TableFieldSchema.Type.INT64,
                                                                TableFieldSchema.Mode.NULLABLE))
                                                .build())
                                .build())
                .addFields(
                        field(
                                "Payload",
                                TableFieldSchema.Type.JSON,
                                TableFieldSchema.Mode.NULLABLE))
                .build();
    }

    /** The defaults, asking for keys to be renamed for the JVM's default locale. */
    private static JsonDocumentOptions.Builder normalizing() {
        return JsonDocumentOptions.builder().normalizeKeysForDefaultLocale();
    }

    private static final String TURKISH_TRAP_DOCUMENT =
            "{\"userId\":5,\"Iban\":\"TR12AB\","
                    + "\"Items\":[{\"innerValue\":\"c\",\"Info\":{\"Id\":7}}],"
                    + "\"Payload\":\"{\\\"Id\\\":1}\"}";

    /**
     * The client library lower-cases each key with the default locale, so under {@code tr_TR} the
     * key {@code userId} becomes {@code userıd} and matches no field of the descriptor, whose names
     * are lower-cased under {@code Locale.ROOT} (#2188). Asked to normalize keys for the default
     * locale, the serializer renames such keys first, at every {@code STRUCT} level, and leaves the
     * text of a {@code JSON} column alone.
     */
    @Test
    void matchesKeysHoldingAnUpperCaseIUnderATurkishDefaultLocale() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            JsonDocumentSerializationSchema serializer =
                    JsonDocumentSerializationSchema.of(turkishTrap(), normalizing().build());

            assertTurkishTrapRow(row(serializer, TURKISH_TRAP_DOCUMENT));
            // A key spelled differently from its column is renamed the same way.
            assertThat(value(row(serializer, "{\"USERID\":6}"), "userid")).isEqualTo(6L);
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * The serializer caches what each key needs per default locale: a serializer that first met the
     * keys under English, where nothing is renamed, still renames them under Turkish.
     */
    @Test
    void renamesKeysAfterTheDefaultLocaleChanges() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            JsonDocumentSerializationSchema serializer =
                    JsonDocumentSerializationSchema.of(turkishTrap(), normalizing().build());
            for (Locale locale : new Locale[] {Locale.US, new Locale("tr", "TR")}) {
                Locale.setDefault(locale);
                assertTurkishTrapRow(row(serializer, TURKISH_TRAP_DOCUMENT));
                assertTurkishTrapRow(row(serializer, TURKISH_TRAP_DOCUMENT));
            }
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * By default keys reach the library as parsed: harmless under English, whose lower-casing is
     * {@code Locale.ROOT}'s, while under Turkish the key {@code userId} names no column, its value
     * is lost when unknown fields are ignored, and {@code USERİD} reaches {@code userid}, which is
     * what the option's documentation warns of.
     */
    @Test
    void handsKeysToTheLibraryAsParsedByDefault() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.US);
            assertTurkishTrapRow(
                    row(JsonDocumentSerializationSchema.of(turkishTrap()), TURKISH_TRAP_DOCUMENT));

            Locale.setDefault(new Locale("tr", "TR"));
            assertThatThrownBy(
                            () ->
                                    JsonDocumentSerializationSchema.of(turkishTrap())
                                            .serialize(TURKISH_TRAP_DOCUMENT))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("unknown")
                    .hasMessageContaining("normalizeKeysForDefaultLocale()");
            DynamicMessage lossy =
                    row(
                            JsonDocumentSerializationSchema.of(
                                    turkishTrap(),
                                    JsonDocumentOptions.builder().ignoreUnknownFields().build()),
                            TURKISH_TRAP_DOCUMENT);
            assertThat(lossy.hasField(lossy.getDescriptorForType().findFieldByName("userid")))
                    .isFalse();
            // And a key no English locale matches reaches a column: USERİD lower-cases to userid.
            assertThat(
                            value(
                                    row(
                                            JsonDocumentSerializationSchema.of(turkishTrap()),
                                            "{\"USER\u0130D\":7}"),
                                    "userid"))
                    .isEqualTo(7L);
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * A record that fails with the default options while holding a key the default locale
     * lower-cases differently from {@code Locale.ROOT} names that key, at any depth, and the
     * option. Under English nothing is added, and neither is it with the option set, even for a key
     * such as a top-level {@code Info}, which names no column and so is left as it is.
     */
    @Test
    void pointsAtTheRenamingOnlyWhereTheDefaultLocaleLowerCasesAKeyDifferently() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            assertThatThrownBy(
                            () ->
                                    JsonDocumentSerializationSchema.of(turkishTrap())
                                            .serialize("{\"items\":[{\"Info\":{\"Id\":7}}]}"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("tr-TR lower-cases the key 'Info'")
                    .hasMessageContaining("normalizeKeysForDefaultLocale()");
            assertThatThrownBy(
                            () ->
                                    JsonDocumentSerializationSchema.of(
                                                    turkishTrap(), normalizing().build())
                                            .serialize("{\"userId\":5,\"Info\":1}"))
                    .isInstanceOf(IOException.class)
                    .hasMessageNotContaining("normalizeKeysForDefaultLocale");

            Locale.setDefault(Locale.US);
            assertThatThrownBy(
                            () ->
                                    JsonDocumentSerializationSchema.of(turkishTrap())
                                            .serialize("{\"userId\":5,\"Bogus\":1}"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("unknown")
                    .hasMessageNotContaining("normalizeKeysForDefaultLocale");
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * The dangerous half of the same trap: with unknown fields ignored, an unmatched key is not a
     * failure but a value that silently never arrives, so a renamed key must keep its value.
     */
    @Test
    void keepsTheValuesOfSuchKeysWhenUnknownFieldsAreIgnored() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            JsonDocumentSerializationSchema serializer =
                    JsonDocumentSerializationSchema.of(
                            turkishTrap(), normalizing().ignoreUnknownFields().build());

            assertTurkishTrapRow(row(serializer, TURKISH_TRAP_DOCUMENT));
        } finally {
            Locale.setDefault(previous);
        }
    }

    /** Renaming keeps the rule {@link #keysDifferingOnlyByCaseCollapseIntoOneColumn} pins. */
    @Test
    void keysDifferingOnlyByCaseStillCollapseUnderATurkishDefaultLocale() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            JsonDocumentSerializationSchema serializer =
                    JsonDocumentSerializationSchema.of(turkishTrap(), normalizing().build());

            assertThat(value(row(serializer, "{\"userId\":1,\"USERID\":2}"), "userid"))
                    .isIn(1L, 2L);
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * Asked to normalize keys for the default locale, with two keys naming one repeated column and
     * one of them renamed: the renamed key takes a spelling of its own, so the library appends both
     * arrays to the column as it does under a locale that lower-cases them alike, and the row is
     * the same under either locale.
     */
    @Test
    void joinsTheArraysOfKeysNamingOneRepeatedColumnUnderEitherLocale() throws Exception {
        String document = "{\"Items\":[{\"innerValue\":\"a\"}],\"items\":[{\"innerValue\":\"b\"}]}";
        Locale previous = Locale.getDefault();
        try {
            for (Locale locale : new Locale[] {Locale.US, new Locale("tr", "TR")}) {
                Locale.setDefault(locale);
                JsonDocumentSerializationSchema serializer =
                        JsonDocumentSerializationSchema.of(turkishTrap(), normalizing().build());

                List<?> items = (List<?>) value(row(serializer, document), "items");
                assertThat(items)
                        .as("items under %s", locale)
                        .extracting(item -> value((DynamicMessage) item, "innervalue"))
                        .containsExactlyInAnyOrder("a", "b");
            }
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * Asked to normalize keys for the default locale, two keys naming one singular column reach the
     * library as two keys under either locale, so it handles them alike: a null never clears a
     * value, each value is validated, and a {@code BYTES} column, which takes an array of byte
     * values, is set from one array rather than from both joined.
     */
    @Test
    void handlesTwoKeysNamingOneSingularColumnAsUnderAnEnglishLocale() throws Exception {
        TableSchema singular =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "id",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "Iban",
                                        TableFieldSchema.Type.BYTES,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        Locale previous = Locale.getDefault();
        try {
            for (Locale locale : new Locale[] {Locale.US, new Locale("tr", "TR")}) {
                Locale.setDefault(locale);
                JsonDocumentSerializationSchema serializer =
                        JsonDocumentSerializationSchema.of(singular, normalizing().build());

                assertThat(value(row(serializer, "{\"ID\":null,\"id\":7}"), "id"))
                        .as("a null beside 7 under %s", locale)
                        .isEqualTo(7L);
                assertThat(value(row(serializer, "{\"ID\":7,\"id\":null}"), "id"))
                        .as("7 beside a null under %s", locale)
                        .isEqualTo(7L);
                assertThatThrownBy(() -> serializer.serialize("{\"ID\":7,\"id\":\"bad\"}"))
                        .as("an invalid value beside 7 under %s", locale)
                        .isInstanceOf(IOException.class);
                assertThat(
                                ((ByteString)
                                                value(
                                                        row(
                                                                serializer,
                                                                "{\"IBAN\":[65],\"iban\":[66]}"),
                                                        "iban"))
                                        .toStringUtf8())
                        .as("two byte arrays under %s", locale)
                        .isIn("A", "B");
            }
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * Asked to normalize keys for the default locale, the serializer leaves keys naming no column
     * to the library under a Turkish locale too, so with unknown fields ignored they are dropped as
     * under any other locale, and several keys naming one repeated column each find a spelling, so
     * every element arrives.
     */
    @Test
    @SuppressWarnings("unchecked")
    void leavesUnknownKeysToTheLibraryAndKeepsEveryElementUnderATurkishLocale() throws Exception {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "ok",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "ids",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.REPEATED))
                        .build();
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            JsonDocumentSerializationSchema ignoring =
                    JsonDocumentSerializationSchema.of(
                            schema, normalizing().ignoreUnknownFields().build());

            assertThat(value(row(ignoring, "{\"ok\":1,\"I\":2,\"i\":3}"), "ok")).isEqualTo(1L);
            assertThat(
                            (List<Object>)
                                    value(
                                            row(
                                                    ignoring,
                                                    "{\"ids\":[1],\"iDs\":[2],\"idS\":[3],\"IDS\":[4]}"),
                                            "ids"))
                    .containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * {@code İD} names no column under an English locale, since it lower-cases to {@code i̇d},
     * while the client library alone, under Turkish, lower-cases it to {@code id} and writes the
     * column {@code id}, as it does with the default options. Asked to normalize keys for the
     * default locale, the serializer keeps it unknown under either locale.
     */
    @Test
    void keepsAKeyThatNamesNoColumnUnknownUnderEitherLocale() throws Exception {
        assertStaysUnknownUnderEitherLocale("id", "{\"\u0130D\":7}");
    }

    /**
     * {@code col_5a6İ} reaches no field under an English locale, while the client library alone,
     * under Turkish, lower-cases it to {@code col_5a6i}, the placeholder it names the column {@code
     * 客} after, and writes that column, as it does with the default options. Asked to normalize
     * keys for the default locale, the serializer keeps it unknown under either locale.
     */
    @Test
    void keepsAKeyThatSpellsAPlaceholderOnlyUnderTurkishUnknownUnderEitherLocale()
            throws Exception {
        assertStaysUnknownUnderEitherLocale("\u5BA2", "{\"col_5a6\u0130\":7}");
    }

    /**
     * {@code ID} reaches no field under an English locale, while under Turkish it lower-cases to
     * the column name {@code ıd}, which the client library resolves through its placeholder, as it
     * does with the default options. Asked to normalize keys for the default locale, the serializer
     * keeps it unknown under either locale.
     */
    @Test
    void keepsAKeyThatSpellsAColumnNeedingAPlaceholderOnlyUnderTurkishUnknown() throws Exception {
        assertStaysUnknownUnderEitherLocale("\u0131d", "{\"ID\":7}");
    }

    private static void assertStaysUnknownUnderEitherLocale(String column, String json)
            throws Exception {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        column,
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();
        Locale previous = Locale.getDefault();
        try {
            for (Locale locale : new Locale[] {Locale.US, new Locale("tr", "TR")}) {
                Locale.setDefault(locale);
                DynamicMessage row =
                        row(
                                JsonDocumentSerializationSchema.of(
                                        schema, normalizing().ignoreUnknownFields().build()),
                                json);
                assertThat(row.getAllFields()).as("fields set under %s", locale).isEmpty();
                assertThatThrownBy(
                                () ->
                                        JsonDocumentSerializationSchema.of(
                                                        schema, normalizing().build())
                                                .serialize(json))
                        .as("strict under %s", locale)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("unknown");
            }
        } finally {
            Locale.setDefault(previous);
        }
    }

    private static void assertTurkishTrapRow(DynamicMessage row) {
        assertThat(value(row, "userid")).isEqualTo(5L);
        // A renamed key keeps its value as it was, letter case included.
        assertThat(value(row, "iban")).isEqualTo("TR12AB");
        List<?> items = (List<?>) value(row, "items");
        assertThat(items).hasSize(1);
        DynamicMessage item = (DynamicMessage) items.get(0);
        assertThat(value(item, "innervalue")).isEqualTo("c");
        assertThat(value((DynamicMessage) value(item, "info"), "id")).isEqualTo(7L);
        assertThat(value(row, "payload")).isEqualTo("{\"Id\":1}");
    }
}
