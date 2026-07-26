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
 */
class AvroSchemaRoundTripTest {

    private static Schema staged(String fieldsJson) {
        Schema source =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                                        + fieldsJson
                                        + "]}");
        TableSchema tableSchema =
                AvroToTableSchemaConverter.convert(source, AvroSchemaOptions.defaults());
        return TableSchemaToAvroConverter.convert(tableSchema);
    }

    private static Schema stagedType(String fieldTypeJson) {
        return staged("{\"name\":\"f\",\"type\":" + fieldTypeJson + "}").getField("f").schema();
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
        // BigQuery DATETIME has no Avro logical type BigQuery loads accept, so it is staged as a
        // civil-time string. Losing the annotation here is the documented shape, not a defect.
        assertThat(
                        stagedType("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}")
                                .getType())
                .isEqualTo(Schema.Type.STRING);
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
