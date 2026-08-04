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

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.TableSchemaToAvroConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link AvroToTableSchemaConverter} against {@link TableSchemaToAvroConverter}, the inverse
 * mapping the FILE_LOADS write method stages files with. An Avro serializer feeding FILE_LOADS goes
 * Avro → {@code TableSchema} → Avro, so the two converters drifting apart would corrupt staged
 * files rather than fail a build.
 *
 * <p>The type assertions run under {@link AvroSchemaOptions.Builder#deriveRequiredColumns()},
 * because {@code REQUIRED} is the only mode {@code TableSchemaToAvroConverter} maps back to a bare
 * type — every other mode is wrapped in {@code ["null", T]}. So the opt-in is what makes the round
 * trip an <em>identity</em> for a non-union field, and an identity is what catches drift. The
 * default's own shape is pinned separately by {@link
 * #byDefaultEveryNonRepeatedFieldStagesAsANullableUnion()} — which is a weaker guard by nature,
 * since a union round trip is not an identity to compare against; the values on that path are
 * covered by {@code ProtoToAvroConverterTest.convertsNullableColumnsThroughTheUnionPath} instead.
 */
class AvroSchemaRoundTripTest {

    private static final AvroSchemaOptions DERIVE_REQUIRED =
            AvroSchemaOptions.builder().deriveRequiredColumns().build();

    private static Schema staged(String fieldsJson, AvroSchemaOptions options) {
        Schema source =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                                        + fieldsJson
                                        + "]}");
        TableSchema tableSchema = AvroToTableSchemaConverter.convert(source, options);
        return TableSchemaToAvroConverter.convert(tableSchema);
    }

    private static Schema stagedType(String fieldTypeJson) {
        return staged("{\"name\":\"f\",\"type\":" + fieldTypeJson + "}", DERIVE_REQUIRED)
                .getField("f")
                .schema();
    }

    @Test
    void scalarsSurviveTheRoundTrip() {
        assertThat(stagedType("\"string\"").getType()).isEqualTo(Schema.Type.STRING);
        assertThat(stagedType("\"bytes\"").getType()).isEqualTo(Schema.Type.BYTES);
        assertThat(stagedType("\"long\"").getType()).isEqualTo(Schema.Type.LONG);
        assertThat(stagedType("\"double\"").getType()).isEqualTo(Schema.Type.DOUBLE);
        assertThat(stagedType("\"boolean\"").getType()).isEqualTo(Schema.Type.BOOLEAN);
    }

    @Test
    void temporalLogicalTypesSurviveTheRoundTrip() {
        assertThat(stagedType("{\"type\":\"int\",\"logicalType\":\"date\"}").getLogicalType())
                .isEqualTo(LogicalTypes.date());
        assertThat(
                        stagedType("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
                                .getLogicalType())
                .isEqualTo(LogicalTypes.timestampMicros());
        assertThat(
                        stagedType("{\"type\":\"int\",\"logicalType\":\"time-millis\"}")
                                .getLogicalType())
                .isEqualTo(LogicalTypes.timeMicros());
        assertThat(
                        stagedType("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}")
                                .getLogicalType())
                .isEqualTo(LogicalTypes.localTimestampMicros());
        assertThat(
                        stagedType("{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}")
                                .getLogicalType())
                .isEqualTo(LogicalTypes.localTimestampMicros());
    }

    @Test
    void decimalPrecisionAndScaleSurviveTheRoundTrip() {
        Schema staged =
                stagedType(
                        "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,"
                                + "\"scale\":4}");

        assertThat(staged.getLogicalType()).isEqualTo(LogicalTypes.decimal(12, 4));
    }

    @Test
    void modesSurviveTheRoundTrip() {
        assertThat(stagedType("\"string\"").getType()).isEqualTo(Schema.Type.STRING);
        assertThat(stagedType("[\"null\",\"string\"]").getType()).isEqualTo(Schema.Type.UNION);
        assertThat(stagedType("{\"type\":\"array\",\"items\":\"string\"}").getType())
                .isEqualTo(Schema.Type.ARRAY);
    }

    /**
     * The other direction, which the default takes: a non-union field derives {@code NULLABLE} and
     * so stages as {@code ["null", T]} with a null default. Not an identity, and deliberately so —
     * but it has to be the *right* non-identity, since this is the shape staged files actually
     * carry for an ordinary job. A value then costs a union branch index, and an unset field is
     * written as an explicit Avro null rather than the type default.
     */
    @Test
    void byDefaultEveryNonRepeatedFieldStagesAsANullableUnion() {
        Schema.Field field =
                staged("{\"name\":\"f\",\"type\":\"string\"}", AvroSchemaOptions.defaults())
                        .getField("f");

        assertThat(field.schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(field.schema().getTypes())
                .extracting(Schema::getType)
                .containsExactly(Schema.Type.NULL, Schema.Type.STRING);
        assertThat(field.hasDefaultValue()).isTrue();
        // A collection is REPEATED either way, so the default does not reach it.
        assertThat(
                        staged(
                                        "{\"name\":\"f\",\"type\":{\"type\":\"array\",\"items\":"
                                                + "\"string\"}}",
                                        AvroSchemaOptions.defaults())
                                .getField("f")
                                .schema()
                                .getType())
                .isEqualTo(Schema.Type.ARRAY);
    }

    @Test
    void nestedRecordsAndMapsSurviveTheRoundTrip() {
        Schema struct =
                stagedType(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"n\",\"type\":\"long\"}]}");
        assertThat(struct.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(struct.getField("n").schema().getType()).isEqualTo(Schema.Type.LONG);

        Schema map = stagedType("{\"type\":\"map\",\"values\":\"long\"}");
        assertThat(map.getType()).isEqualTo(Schema.Type.ARRAY);
        assertThat(map.getElementType().getFields())
                .extracting(Schema.Field::name)
                .containsExactly("key", "value");
    }
}
