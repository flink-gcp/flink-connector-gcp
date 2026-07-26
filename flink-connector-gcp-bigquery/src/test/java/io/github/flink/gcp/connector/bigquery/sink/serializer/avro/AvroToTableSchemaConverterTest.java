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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroToTableSchemaConverter}. */
class AvroToTableSchemaConverterTest {

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

    @Test
    void nonUnionFieldIsRequiredAndNullableUnionIsNullable() {
        assertThat(fieldOf("\"string\"").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(fieldOf("[\"null\",\"string\"]").getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fieldOf("[\"string\",\"null\"]").getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(fieldOf("[\"string\"]").getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
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
    void allFieldsNullableRelaxesRequiredButLeavesRepeated() {
        Schema schema =
                record(
                        "{\"name\":\"a\",\"type\":\"string\"},"
                                + "{\"name\":\"b\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}");
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        schema, AvroSchemaOptions.builder().allFieldsNullable().build());
        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(converted.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
    }

    @Test
    void allFieldsNullableRelaxesNestedRecordFields() {
        Schema schema =
                recordOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"n\",\"type\":\"long\"}]}");
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        schema, AvroSchemaOptions.builder().allFieldsNullable().build());
        assertThat(converted.getFields(0).getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
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
                        org.assertj.core.groups.Tuple.tuple("n", TableFieldSchema.Mode.REQUIRED),
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
                                TableFieldSchema.Mode.REQUIRED),
                        org.assertj.core.groups.Tuple.tuple(
                                "value",
                                TableFieldSchema.Type.INT64,
                                TableFieldSchema.Mode.REQUIRED));
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

    @Test
    void allFieldsNullableRelaxesMapKeyAndValue() {
        TableSchema converted =
                AvroToTableSchemaConverter.convert(
                        recordOf("{\"type\":\"map\",\"values\":\"long\"}"),
                        AvroSchemaOptions.builder().allFieldsNullable().build());

        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(converted.getFields(0).getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("key", TableFieldSchema.Mode.NULLABLE),
                        org.assertj.core.groups.Tuple.tuple(
                                "value", TableFieldSchema.Mode.NULLABLE));
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
