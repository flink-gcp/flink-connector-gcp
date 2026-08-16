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

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroToTableSchemaConverter}. */
class AvroToTableSchemaConverterTest {

    private static final AvroSchemaOptions DERIVE_REQUIRED =
            AvroSchemaOptions.builder().deriveRequiredColumns().build();

    private static Schema record(String fieldsJson) {
        return new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Row\",\"fields\":[" + fieldsJson + "]}");
    }

    private static Schema recordOf(String fieldTypeJson) {
        return record("{\"name\":\"f\",\"type\":" + fieldTypeJson + "}");
    }

    private static TableSchema convert(Schema avroSchema) {
        return AvroToTableSchemaConverter.convert(avroSchema, AvroSchemaOptions.defaults());
    }

    private static TableFieldSchema fieldOf(String fieldTypeJson) {
        return convert(recordOf(fieldTypeJson)).getFields(0);
    }

    private static TableFieldSchema.Type typeOf(String fieldTypeJson) {
        return fieldOf(fieldTypeJson).getType();
    }

    /** The mode a field gets under {@link AvroSchemaOptions.Builder#deriveRequiredColumns()}. */
    private static TableFieldSchema.Mode modeOf(String fieldTypeJson) {
        return AvroToTableSchemaConverter.convert(recordOf(fieldTypeJson), DERIVE_REQUIRED)
                .getFields(0)
                .getMode();
    }

    private static Schema logical(String baseType, String logicalType) {
        return recordOf("{\"type\":\"" + baseType + "\",\"logicalType\":\"" + logicalType + "\"}");
    }

    @Test
    void mapsScalarTypes() {
        assertThat(typeOf("\"string\"")).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(typeOf("\"bytes\"")).isEqualTo(TableFieldSchema.Type.BYTES);
        assertThat(typeOf("\"int\"")).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(typeOf("\"long\"")).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(typeOf("\"float\"")).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(typeOf("\"double\"")).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(typeOf("\"boolean\"")).isEqualTo(TableFieldSchema.Type.BOOL);
    }

    @Test
    void mapsEnumAndFixedToStringAndBytes() {
        assertThat(typeOf("{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}"))
                .isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(typeOf("{\"type\":\"fixed\",\"name\":\"Hash\",\"size\":16}"))
                .isEqualTo(TableFieldSchema.Type.BYTES);
    }

    @Test
    void mapsUuidStringToString() {
        assertThat(convert(logical("string", "uuid")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.STRING);
    }

    @Test
    void mapsTemporalLogicalTypes() {
        assertThat(convert(logical("int", "date")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.DATE);
        assertThat(convert(logical("int", "time-millis")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.TIME);
        assertThat(convert(logical("long", "time-micros")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.TIME);
        assertThat(convert(logical("long", "timestamp-millis")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.TIMESTAMP);
        assertThat(convert(logical("long", "timestamp-micros")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.TIMESTAMP);
        assertThat(convert(logical("long", "local-timestamp-millis")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.DATETIME);
        assertThat(convert(logical("long", "local-timestamp-micros")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.DATETIME);
    }

    @Test
    void mapsIntAndLongWithoutLogicalTypeToInt64() {
        // A logical type Avro rejects as invalid is dropped by the parser, and the field must then
        // land on its base type rather than on the type the annotation asked for.
        assertThat(convert(logical("long", "date")).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.INT64);
    }

    @Test
    void picksNumericOrBignumericByIntegerDigitsAndScale() {
        // BigQuery bounds NUMERIC(P, S) by S <= 9 and P - S <= 29, not by P alone.
        assertThat(typeOf(decimal("bytes", 38, 9))).isEqualTo(TableFieldSchema.Type.NUMERIC);
        assertThat(typeOf(decimal("bytes", 29, 0))).isEqualTo(TableFieldSchema.Type.NUMERIC);
        assertThat(typeOf(decimal("bytes", 30, 0))).isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(typeOf(decimal("bytes", 35, 2))).isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(typeOf(decimal("bytes", 39, 9))).isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(typeOf(decimal("bytes", 20, 10))).isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(typeOf(decimal("bytes", 76, 38))).isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
    }

    @Test
    void carriesBignumericPrecisionAndScale() {
        TableFieldSchema field = fieldOf(decimal("bytes", 50, 20));
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(field.getPrecision()).isEqualTo(50);
        assertThat(field.getScale()).isEqualTo(20);
    }

    @Test
    void carriesDecimalPrecisionAndScale() {
        TableFieldSchema field = fieldOf(decimal("bytes", 12, 4));
        assertThat(field.getPrecision()).isEqualTo(12);
        assertThat(field.getScale()).isEqualTo(4);
    }

    @Test
    void mapsDecimalOnFixedToNumeric() {
        assertThat(
                        typeOf(
                                "{\"type\":\"fixed\",\"name\":\"Dec\",\"size\":16,"
                                        + "\"logicalType\":\"decimal\",\"precision\":20,"
                                        + "\"scale\":4}"))
                .isEqualTo(TableFieldSchema.Type.NUMERIC);
    }

    @Test
    void rejectsDecimalWiderThanBignumeric() {
        // 39 integer digits, one past the limit.
        assertThatThrownBy(() -> convert(recordOf(decimal("bytes", 77, 38))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds BigQuery BIGNUMERIC");
        assertThatThrownBy(() -> convert(recordOf(decimal("bytes", 39, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds BigQuery BIGNUMERIC");
        // 39 fractional digits, likewise.
        assertThatThrownBy(() -> convert(recordOf(decimal("bytes", 60, 39))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds BigQuery BIGNUMERIC");
    }

    /**
     * By default the converter looks at no union shape at all — a field is REPEATED, or it is
     * NULLABLE. The Avro schema's own nullability only becomes a mode under {@link
     * AvroSchemaOptions.Builder#deriveRequiredColumns()}, pinned below.
     */
    @Test
    void everyNonRepeatedFieldIsNullableByDefault() {
        assertThat(fieldOf("\"string\"").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fieldOf("[\"null\",\"string\"]").getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fieldOf("[\"string\",\"null\"]").getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fieldOf("[\"string\"]").getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void deriveRequiredColumnsMakesANonUnionFieldRequired() {
        assertThat(modeOf("\"string\"")).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(modeOf("[\"null\",\"string\"]")).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(modeOf("[\"string\",\"null\"]")).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        // A degenerate one-branch union admits no null, so it is a constraint like a bare type.
        assertThat(modeOf("[\"string\"]")).isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void arrayIsRepeated() {
        TableFieldSchema field = fieldOf("{\"type\":\"array\",\"items\":\"string\"}");
        assertThat(field.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRING);
    }

    @Test
    void nullableArrayStaysRepeated() {
        assertThat(fieldOf("[\"null\",{\"type\":\"array\",\"items\":\"string\"}]").getMode())
                .isEqualTo(TableFieldSchema.Mode.REPEATED);
    }

    @Test
    void deriveRequiredColumnsTightensButLeavesRepeatedAlone() {
        Schema schema =
                record(
                        "{\"name\":\"a\",\"type\":\"string\"},"
                                + "{\"name\":\"b\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}");
        TableSchema converted = AvroToTableSchemaConverter.convert(schema, DERIVE_REQUIRED);
        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        // A BigQuery REPEATED column cannot be NULLABLE, so the option cannot reach it either way.
        assertThat(converted.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
    }

    @Test
    void deriveRequiredColumnsReachesNestedRecordFields() {
        Schema schema =
                recordOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"n\",\"type\":\"long\"},"
                                + "{\"name\":\"m\",\"type\":[\"null\",\"long\"]}]}");
        TableSchema converted = AvroToTableSchemaConverter.convert(schema, DERIVE_REQUIRED);
        assertThat(converted.getFields(0).getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("n", TableFieldSchema.Mode.REQUIRED),
                        org.assertj.core.groups.Tuple.tuple("m", TableFieldSchema.Mode.NULLABLE));
        // And the struct holding them is REQUIRED itself, the field not being a union either.
        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void mapsRecordToStruct() {
        TableFieldSchema field =
                fieldOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"n\",\"type\":\"long\"},"
                                + "{\"name\":\"s\",\"type\":[\"null\",\"string\"]}]}");
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(field.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("n", TableFieldSchema.Mode.NULLABLE),
                        org.assertj.core.groups.Tuple.tuple("s", TableFieldSchema.Mode.NULLABLE));
    }

    @Test
    void mapsMapToRepeatedStructOfKeyAndValue() {
        TableFieldSchema field = fieldOf("{\"type\":\"map\",\"values\":\"long\"}");
        assertThat(field.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(field.getFieldsList())
                .extracting(
                        TableFieldSchema::getName,
                        TableFieldSchema::getType,
                        TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "key",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.NULLABLE),
                        org.assertj.core.groups.Tuple.tuple(
                                "value",
                                TableFieldSchema.Type.INT64,
                                TableFieldSchema.Mode.NULLABLE));
    }

    @Test
    void jsonFieldPathMarksStringAsJson() {
        Schema schema =
                record(
                        "{\"name\":\"payload\",\"type\":\"string\"},"
                                + "{\"name\":\"plain\",\"type\":\"string\"}");
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        schema, AvroSchemaOptions.builder().jsonFieldPath("payload").build());
        assertThat(converted.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(converted.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.STRING);
    }

    @Test
    void jsonFieldPathReachesNestedFields() {
        Schema schema =
                recordOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"details\",\"type\":[\"null\",\"string\"]}]}");
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        schema, AvroSchemaOptions.builder().jsonFieldPath("f.details").build());
        assertThat(converted.getFields(0).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.JSON);
    }

    @Test
    void rejectsJsonFieldPathOnNonStringField() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("\"long\""),
                                        AvroSchemaOptions.builder().jsonFieldPath("f").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON mapping requires");
    }

    @Test
    void rejectsJsonFieldPathMatchingNoField() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("\"string\""),
                                        AvroSchemaOptions.builder().jsonFieldPath("nope").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("matching no field");
    }

    @Test
    void rejectsUnionWithMoreThanOneNonNullBranch() {
        assertThatThrownBy(() -> convert(recordOf("[\"null\",\"string\",\"long\"]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one non-null branch");
    }

    @Test
    void rejectsBareNullField() {
        assertThatThrownBy(() -> convert(recordOf("\"null\"")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
    }

    @Test
    void rejectsNullableArrayElements() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"array\",\"items\":"
                                                        + "[\"null\",\"string\"]}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nullable elements");
    }

    @Test
    void rejectsNestedCollections() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"array\",\"items\":"
                                                        + "{\"type\":\"array\",\"items\":\"long\"}}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not nest");
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"array\",\"items\":"
                                                        + "{\"type\":\"map\",\"values\":\"long\"}}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not nest");
    }

    @Test
    void rejectsRecursiveRecords() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"record\",\"name\":\"Node\",\"fields\":"
                                                        + "[{\"name\":\"child\",\"type\":"
                                                        + "[\"null\",\"Node\"]}]}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive record types");
    }

    @Test
    void rejectsSiblingsDifferingOnlyByCase() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        record(
                                                "{\"name\":\"a\",\"type\":\"string\"},"
                                                        + "{\"name\":\"A\",\"type\":\"string\"}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differ only by case");
    }

    @Test
    void rejectsNanosecondTimestamps() {
        assertThatThrownBy(() -> convert(logical("long", "timestamp-nanos")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("microsecond-precision");
        assertThatThrownBy(() -> convert(logical("long", "local-timestamp-nanos")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("microsecond-precision");
    }

    @Test
    void rejectsDurationAndBigDecimalLogicalTypes() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"fixed\",\"name\":\"D\",\"size\":12,"
                                                        + "\"logicalType\":\"duration\"}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
        assertThatThrownBy(() -> convert(logical("bytes", "big-decimal")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("decimal(precision, scale)");
    }

    @Test
    void rejectsUuidOnFixed() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,"
                                                        + "\"logicalType\":\"uuid\"}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supported on a string field");
    }

    @Test
    void rejectsNonRecordRootSchema() {
        assertThatThrownBy(() -> convert(Schema.create(Schema.Type.STRING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a record");
    }

    @Test
    void oneRecordTypeReusedInSiblingFieldsIsNotRecursion() {
        TableSchema converted =
                convert(
                        record(
                                "{\"name\":\"billing\",\"type\":{\"type\":\"record\","
                                        + "\"name\":\"Address\",\"fields\":"
                                        + "[{\"name\":\"city\",\"type\":\"string\"}]}},"
                                        + "{\"name\":\"shipping\",\"type\":\"Address\"}"));

        assertThat(converted.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "billing", TableFieldSchema.Type.STRUCT),
                        org.assertj.core.groups.Tuple.tuple(
                                "shipping", TableFieldSchema.Type.STRUCT));
    }

    @Test
    void rejectsRecursionThroughArraysAndMaps() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"record\",\"name\":\"Node\",\"fields\":"
                                                        + "[{\"name\":\"kids\",\"type\":"
                                                        + "{\"type\":\"array\",\"items\":\"Node\"}}]}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive record types");
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"record\",\"name\":\"Node\",\"fields\":"
                                                        + "[{\"name\":\"kids\",\"type\":"
                                                        + "{\"type\":\"map\",\"values\":\"Node\"}}]}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive record types");
    }

    @Test
    void rejectsUnionWithoutANonNullBranch() {
        assertThatThrownBy(() -> convert(recordOf("[\"null\"]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no non-null branch");
    }

    @Test
    void rejectsArrayOfAMultiBranchUnion() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"array\",\"items\":"
                                                        + "[\"string\",\"long\"]}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one non-null branch");
    }

    @Test
    void rejectsNestedSiblingsDifferingOnlyByCase() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        recordOf(
                                                "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                                        + "[{\"name\":\"a\",\"type\":\"string\"},"
                                                        + "{\"name\":\"A\",\"type\":\"string\"}]}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("(at f)");
    }

    /**
     * A singular {@code JSON} column is never {@code REQUIRED}, matching the protobuf side. The two
     * options share a name, so they must not diverge on which columns they constrain — and a JSON
     * column is a poor thing to make mandatory, an empty string being a row-level error in one
     * either way.
     */
    @Test
    void deriveRequiredColumnsLeavesJsonColumnsNullable() {
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        record(
                                "{\"name\":\"a\",\"type\":\"string\"},"
                                        + "{\"name\":\"b\",\"type\":\"string\"}"),
                        AvroSchemaOptions.builder()
                                .jsonFieldPath("a")
                                .deriveRequiredColumns()
                                .build());

        assertThat(converted.getFieldsList())
                .extracting(
                        TableFieldSchema::getName,
                        TableFieldSchema::getType,
                        TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "a", TableFieldSchema.Type.JSON, TableFieldSchema.Mode.NULLABLE),
                        // The identically shaped field beside it shows the option is otherwise on.
                        org.assertj.core.groups.Tuple.tuple(
                                "b", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED));
    }

    /**
     * A {@code REQUIRED} column inside a {@code NULLABLE} struct — the one opt-in shape that only
     * arises when the outer record is a {@code ["null", record]} union, so the recursion has to
     * keep deriving after relaxing the parent.
     */
    @Test
    void deriveRequiredColumnsReachesInsideANullableStruct() {
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        recordOf(
                                "[\"null\",{\"type\":\"record\",\"name\":\"Inner\","
                                        + "\"fields\":[{\"name\":\"n\",\"type\":\"long\"}]}]"),
                        DERIVE_REQUIRED);

        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(converted.getFields(0).getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void deriveRequiredColumnsReachesMapKeyAndValue() {
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        recordOf("{\"type\":\"map\",\"values\":\"long\"}"), DERIVE_REQUIRED);

        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        // An Avro map key is never a union and an entry always has one, so it is REQUIRED under the
        // option — the proto path converges here, a proto3 map entry's key having no presence.
        assertThat(converted.getFields(0).getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("key", TableFieldSchema.Mode.REQUIRED),
                        org.assertj.core.groups.Tuple.tuple(
                                "value", TableFieldSchema.Mode.REQUIRED));
        // A nullable map value stays NULLABLE even under the option.
        assertThat(
                        AvroToTableSchemaConverter.convert(
                                        recordOf(
                                                "{\"type\":\"map\",\"values\":[\"null\","
                                                        + "\"long\"]}"),
                                        DERIVE_REQUIRED)
                                .getFields(0)
                                .getFields(1)
                                .getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void mapsWithCollectionAndRecordValues() {
        TableFieldSchema arrayValued =
                fieldOf(
                        "{\"type\":\"map\",\"values\":{\"type\":\"array\","
                                + "\"items\":\"long\"}}");
        assertThat(arrayValued.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);

        TableFieldSchema nestedMap =
                fieldOf("{\"type\":\"map\",\"values\":{\"type\":\"map\",\"values\":\"long\"}}");
        assertThat(nestedMap.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(nestedMap.getFields(1).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("key", "value");
    }

    @Test
    void jsonFieldPathReachesRepeatedAndMapValueFields() {
        TableFieldSchema repeated =
                AvroToTableSchemaConverter.convert(
                                recordOf("{\"type\":\"array\",\"items\":\"string\"}"),
                                AvroSchemaOptions.builder().jsonFieldPath("f").build())
                        .getFields(0);
        assertThat(repeated.getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(repeated.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);

        TableFieldSchema mapValue =
                AvroToTableSchemaConverter.convert(
                                recordOf("{\"type\":\"map\",\"values\":\"string\"}"),
                                AvroSchemaOptions.builder().jsonFieldPath("f.value").build())
                        .getFields(0);
        assertThat(mapValue.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.JSON);
    }

    @Test
    void geographyFieldPathMarksStringAsGeography() {
        Schema schema =
                record(
                        "{\"name\":\"boundary\",\"type\":\"string\"},"
                                + "{\"name\":\"plain\",\"type\":\"string\"}");
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        schema, AvroSchemaOptions.builder().geographyFieldPath("boundary").build());
        assertThat(converted.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
        assertThat(converted.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.STRING);
    }

    @Test
    void geographyFieldPathReachesNestedFields() {
        Schema schema =
                recordOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"boundary\",\"type\":[\"null\",\"string\"]}]}");
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        schema,
                        AvroSchemaOptions.builder().geographyFieldPath("f.boundary").build());
        assertThat(converted.getFields(0).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
    }

    @Test
    void geographyFieldPathReachesRepeatedAndMapValueFields() {
        TableFieldSchema repeated =
                AvroToTableSchemaConverter.convert(
                                recordOf("{\"type\":\"array\",\"items\":\"string\"}"),
                                AvroSchemaOptions.builder().geographyFieldPath("f").build())
                        .getFields(0);
        assertThat(repeated.getType()).isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
        assertThat(repeated.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);

        TableFieldSchema mapValue =
                AvroToTableSchemaConverter.convert(
                                recordOf("{\"type\":\"map\",\"values\":\"string\"}"),
                                AvroSchemaOptions.builder().geographyFieldPath("f.value").build())
                        .getFields(0);
        assertThat(mapValue.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
    }

    @Test
    void rejectsGeographyFieldPathOnNonStringField() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("\"long\""),
                                        AvroSchemaOptions.builder()
                                                .geographyFieldPath("f")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GEOGRAPHY mapping requires");
    }

    @Test
    void rejectsGeographyFieldPathOnAMapField() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("{\"type\":\"map\",\"values\":\"string\"}"),
                                        AvroSchemaOptions.builder()
                                                .geographyFieldPath("f")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GEOGRAPHY mapping requires");
    }

    @Test
    void rejectsGeographyFieldPathMatchingNoField() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("\"string\""),
                                        AvroSchemaOptions.builder()
                                                .geographyFieldPath("nope")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("matching no field");
    }

    /** A column has one type, so a path claimed by both markers is a configuration error. */
    @Test
    void rejectsAFieldMarkedAsBothJsonAndGeography() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("\"string\""),
                                        AvroSchemaOptions.builder()
                                                .jsonFieldPath("f")
                                                .geographyFieldPath("f")
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both a JSON and a GEOGRAPHY");
    }

    /** The JSON carve-out above, for a geography column: same rule, stated about the marking. */
    @Test
    void deriveRequiredColumnsLeavesGeographyColumnsNullable() {
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        record(
                                "{\"name\":\"a\",\"type\":\"string\"},"
                                        + "{\"name\":\"b\",\"type\":\"string\"}"),
                        AvroSchemaOptions.builder()
                                .geographyFieldPath("a")
                                .deriveRequiredColumns()
                                .build());

        assertThat(converted.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(converted.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void rejectsJsonFieldPathOnAMapField() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        recordOf("{\"type\":\"map\",\"values\":\"string\"}"),
                                        AvroSchemaOptions.builder().jsonFieldPath("f").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON mapping requires");
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(
                        () ->
                                AvroToTableSchemaConverter.convert(
                                        null, AvroSchemaOptions.defaults()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroToTableSchemaConverter.convert(recordOf("\"string\""), null))
                .isInstanceOf(NullPointerException.class);
    }

    private static String decimal(String baseType, int precision, int scale) {
        return "{\"type\":\""
                + baseType
                + "\",\"logicalType\":\"decimal\",\"precision\":"
                + precision
                + ",\"scale\":"
                + scale
                + "}";
    }
}
