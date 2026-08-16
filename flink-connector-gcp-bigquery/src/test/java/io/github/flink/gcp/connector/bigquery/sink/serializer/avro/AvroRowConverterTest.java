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

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroRowConverter}. */
class AvroRowConverterTest {

    /** The three schemas a converter needs, derived the way the serializer derives them. */
    private static final class Setup {

        private final Schema avroSchema;
        private final Descriptors.Descriptor descriptor;
        private final AvroRowConverter converter;

        Setup(Schema avroSchema, AvroSchemaOptions options) {
            this.avroSchema = avroSchema;
            TableSchema tableSchema = AvroToTableSchemaConverter.convert(avroSchema, options);
            try {
                this.descriptor =
                        BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                tableSchema);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new IllegalStateException(e);
            }
            this.converter = new AvroRowConverter(avroSchema, tableSchema, descriptor);
        }

        GenericRecord record() {
            return new GenericData.Record(avroSchema);
        }

        DynamicMessage convert(GenericRecord record) throws IOException {
            return converter.convert(record);
        }

        Object value(DynamicMessage message, String field) {
            Descriptors.FieldDescriptor descriptorField = descriptor.findFieldByName(field);
            assertThat(descriptorField).as("descriptor field %s", field).isNotNull();
            return message.getField(descriptorField);
        }

        boolean isSet(DynamicMessage message, String field) {
            return message.hasField(descriptor.findFieldByName(field));
        }
    }

    private static Schema record(String fieldsJson) {
        return new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Row\",\"fields\":[" + fieldsJson + "]}");
    }

    private static Setup setup(String fieldsJson) {
        return new Setup(record(fieldsJson), AvroSchemaOptions.defaults());
    }

    private static Setup setupOf(String fieldTypeJson) {
        return setup("{\"name\":\"f\",\"type\":" + fieldTypeJson + "}");
    }

    private static String logical(String baseType, String logicalType) {
        return "{\"type\":\"" + baseType + "\",\"logicalType\":\"" + logicalType + "\"}";
    }

    private static String decimal(int precision, int scale) {
        return "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":"
                + precision
                + ",\"scale\":"
                + scale
                + "}";
    }

    @Test
    void convertsScalarValues() throws Exception {
        Setup setup =
                setup(
                        "{\"name\":\"s\",\"type\":\"string\"},"
                                + "{\"name\":\"b\",\"type\":\"bytes\"},"
                                + "{\"name\":\"i\",\"type\":\"int\"},"
                                + "{\"name\":\"l\",\"type\":\"long\"},"
                                + "{\"name\":\"fl\",\"type\":\"float\"},"
                                + "{\"name\":\"d\",\"type\":\"double\"},"
                                + "{\"name\":\"bo\",\"type\":\"boolean\"}");
        GenericRecord record = setup.record();
        record.put("s", "hello");
        record.put("b", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        record.put("i", 7);
        record.put("l", 8L);
        record.put("fl", 1.5f);
        record.put("d", 2.5d);
        record.put("bo", true);

        DynamicMessage message = setup.convert(record);

        assertThat(setup.value(message, "s")).isEqualTo("hello");
        assertThat(setup.value(message, "b")).isEqualTo(ByteString.copyFrom(new byte[] {1, 2, 3}));
        assertThat(setup.value(message, "i")).isEqualTo(7L);
        assertThat(setup.value(message, "l")).isEqualTo(8L);
        assertThat(setup.value(message, "fl")).isEqualTo(1.5d);
        assertThat(setup.value(message, "d")).isEqualTo(2.5d);
        assertThat(setup.value(message, "bo")).isEqualTo(true);
    }

    @Test
    void acceptsUtf8AsWellAsString() throws Exception {
        Setup setup = setupOf("\"string\"");
        GenericRecord record = setup.record();
        record.put("f", new Utf8("from a decoder"));

        assertThat(setup.value(setup.convert(record), "f")).isEqualTo("from a decoder");
    }

    @Test
    void doesNotConsumeTheSourceByteBuffer() throws Exception {
        Setup setup = setupOf("\"bytes\"");
        ByteBuffer buffer = ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8));
        GenericRecord record = setup.record();
        record.put("f", buffer);

        assertThat(setup.value(setup.convert(record), "f"))
                .isEqualTo(ByteString.copyFromUtf8("payload"));
        assertThat(buffer.position()).isZero();
        // A record whose failure is routed away can be converted again; a consumed buffer would
        // silently yield an empty value the second time.
        assertThat(setup.value(setup.convert(record), "f"))
                .isEqualTo(ByteString.copyFromUtf8("payload"));
    }

    @Test
    void convertsEnumSymbolToItsName() throws Exception {
        Setup setup =
                setupOf("{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}");
        GenericRecord record = setup.record();
        record.put(
                "f", new GenericData.EnumSymbol(setup.avroSchema.getField("f").schema(), "BLUE"));

        assertThat(setup.value(setup.convert(record), "f")).isEqualTo("BLUE");
    }

    @Test
    void convertsFixedToBytes() throws Exception {
        Setup setup = setupOf("{\"type\":\"fixed\",\"name\":\"Hash\",\"size\":3}");
        GenericRecord record = setup.record();
        record.put(
                "f",
                new GenericData.Fixed(
                        setup.avroSchema.getField("f").schema(), new byte[] {9, 8, 7}));

        assertThat(setup.value(setup.convert(record), "f"))
                .isEqualTo(ByteString.copyFrom(new byte[] {9, 8, 7}));
    }

    @Test
    void convertsDateFromDaysAndFromLocalDate() throws Exception {
        Setup setup = setupOf(logical("int", "date"));
        GenericRecord raw = setup.record();
        raw.put("f", 19000);
        GenericRecord converted = setup.record();
        converted.put("f", LocalDate.ofEpochDay(19000));

        assertThat(setup.value(setup.convert(raw), "f")).isEqualTo(19000);
        assertThat(setup.value(setup.convert(converted), "f")).isEqualTo(19000);
    }

    @Test
    void convertsTimeToPackedCivilTime() throws Exception {
        LocalTime expected = LocalTime.of(12, 34, 56, 789_000_000);

        Setup millis = setupOf(logical("int", "time-millis"));
        GenericRecord millisRecord = millis.record();
        millisRecord.put("f", (int) (expected.toNanoOfDay() / 1_000_000L));
        assertThat(
                        CivilTimeEncoder.decodePacked64TimeMicrosLocalTime(
                                (Long) millis.value(millis.convert(millisRecord), "f")))
                .isEqualTo(expected);

        Setup micros = setupOf(logical("long", "time-micros"));
        GenericRecord microsRecord = micros.record();
        microsRecord.put("f", expected.toNanoOfDay() / 1_000L);
        assertThat(
                        CivilTimeEncoder.decodePacked64TimeMicrosLocalTime(
                                (Long) micros.value(micros.convert(microsRecord), "f")))
                .isEqualTo(expected);

        GenericRecord localTimeRecord = micros.record();
        localTimeRecord.put("f", expected);
        assertThat(
                        CivilTimeEncoder.decodePacked64TimeMicrosLocalTime(
                                (Long) micros.value(micros.convert(localTimeRecord), "f")))
                .isEqualTo(expected);
    }

    @Test
    void convertsTimestampToEpochMicros() throws Exception {
        Instant expected = Instant.parse("2026-07-26T01:02:03.456789Z");
        long expectedMicros = expected.getEpochSecond() * 1_000_000L + expected.getNano() / 1_000L;

        Setup millis = setupOf(logical("long", "timestamp-millis"));
        GenericRecord millisRecord = millis.record();
        millisRecord.put("f", expected.toEpochMilli());
        assertThat(millis.value(millis.convert(millisRecord), "f"))
                .isEqualTo(expected.toEpochMilli() * 1_000L);

        Setup micros = setupOf(logical("long", "timestamp-micros"));
        GenericRecord microsRecord = micros.record();
        microsRecord.put("f", expectedMicros);
        assertThat(micros.value(micros.convert(microsRecord), "f")).isEqualTo(expectedMicros);

        GenericRecord instantRecord = micros.record();
        instantRecord.put("f", expected);
        assertThat(micros.value(micros.convert(instantRecord), "f")).isEqualTo(expectedMicros);
    }

    @Test
    void convertsLocalTimestampToPackedDatetime() throws Exception {
        LocalDateTime expected = LocalDateTime.of(2026, 7, 26, 1, 2, 3, 456_789_000);

        Setup micros = setupOf(logical("long", "local-timestamp-micros"));
        GenericRecord raw = micros.record();
        raw.put("f", expected.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L + 456_789L);
        assertThat(
                        CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                (Long) micros.value(micros.convert(raw), "f")))
                .isEqualTo(expected);

        GenericRecord converted = micros.record();
        converted.put("f", expected);
        assertThat(
                        CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                (Long) micros.value(micros.convert(converted), "f")))
                .isEqualTo(expected);
    }

    @Test
    void convertsDecimalFromUnscaledBytesAndFromBigDecimal() throws Exception {
        Setup setup = setupOf(decimal(20, 4));
        BigDecimal expected = new BigDecimal("1234.5678");

        GenericRecord raw = setup.record();
        raw.put("f", ByteBuffer.wrap(expected.unscaledValue().toByteArray()));
        assertThat(
                        BigDecimalByteStringEncoder.decodeNumericByteString(
                                (ByteString) setup.value(setup.convert(raw), "f")))
                .isEqualByComparingTo(expected);

        GenericRecord converted = setup.record();
        converted.put("f", expected);
        assertThat(
                        BigDecimalByteStringEncoder.decodeNumericByteString(
                                (ByteString) setup.value(setup.convert(converted), "f")))
                .isEqualByComparingTo(expected);
    }

    @Test
    void convertsNegativeDecimals() throws Exception {
        Setup setup = setupOf(decimal(20, 4));
        BigDecimal expected = new BigDecimal("-0.0001");
        GenericRecord raw = setup.record();
        raw.put("f", ByteBuffer.wrap(new BigInteger("-1").toByteArray()));

        assertThat(
                        BigDecimalByteStringEncoder.decodeNumericByteString(
                                (ByteString) setup.value(setup.convert(raw), "f")))
                .isEqualByComparingTo(expected);
    }

    @Test
    void decimalTooPreciseForTheColumnIsRowLevelFailure() {
        Setup setup = setupOf(decimal(20, 4));
        GenericRecord record = setup.record();
        record.put("f", new BigDecimal("1.234567890123"));

        assertThatThrownBy(() -> setup.convert(record))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("cannot be represented in the destination column");
    }

    /**
     * The one behaviour {@link AvroSchemaOptions.Builder#deriveRequiredColumns()} changes in the
     * value path. It is what a caller trades for a constrained table: by default the column is left
     * unset (below), and under the option the record is rejected instead.
     */
    @Test
    void deriveRequiredColumnsMakesAMissingMandatoryValueARowLevelFailure() {
        Setup setup =
                new Setup(
                        record("{\"name\":\"f\",\"type\":\"string\"}"),
                        AvroSchemaOptions.builder().deriveRequiredColumns().build());

        assertThatThrownBy(() -> setup.convert(setup.record()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("REQUIRED");
    }

    @Test
    void byDefaultAMissingMandatoryValueIsLeftUnset() throws Exception {
        // "Mandatory" per the Avro schema — a bare "string" rather than ["null","string"] — is not
        // carried into the column mode by default, so there is nothing for the record to violate.
        Setup setup = setupOf("\"string\"");

        assertThat(setup.isSet(setup.convert(setup.record()), "f")).isFalse();
    }

    @Test
    void missingNullableValueIsLeftUnset() throws Exception {
        Setup setup = setupOf("[\"null\",\"string\"]");

        assertThat(setup.isSet(setup.convert(setup.record()), "f")).isFalse();
    }

    @Test
    void convertsRepeatedFields() throws Exception {
        Setup setup = setupOf("{\"type\":\"array\",\"items\":\"long\"}");
        GenericRecord record = setup.record();
        record.put("f", Arrays.asList(1L, 2L, 3L));

        assertThat(setup.value(setup.convert(record), "f"))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void nullArrayBecomesNoElements() throws Exception {
        Setup setup = setupOf("[\"null\",{\"type\":\"array\",\"items\":\"long\"}]");

        assertThat(setup.value(setup.convert(setup.record()), "f"))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .isEmpty();
    }

    @Test
    void nullElementInARepeatedFieldIsRowLevelFailure() {
        Setup setup = setupOf("{\"type\":\"array\",\"items\":\"long\"}");
        GenericRecord record = setup.record();
        record.put("f", Arrays.asList(1L, null));

        assertThatThrownBy(() -> setup.convert(record))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("null element");
    }

    @Test
    void convertsNestedRecords() throws Exception {
        Setup setup =
                setupOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"n\",\"type\":\"long\"},"
                                + "{\"name\":\"s\",\"type\":[\"null\",\"string\"]}]}");
        GenericRecord inner = new GenericData.Record(setup.avroSchema.getField("f").schema());
        inner.put("n", 42L);
        GenericRecord record = setup.record();
        record.put("f", inner);

        DynamicMessage nested = (DynamicMessage) setup.value(setup.convert(record), "f");
        Descriptors.Descriptor innerDescriptor = nested.getDescriptorForType();
        assertThat(nested.getField(innerDescriptor.findFieldByName("n"))).isEqualTo(42L);
        assertThat(nested.hasField(innerDescriptor.findFieldByName("s"))).isFalse();
    }

    @Test
    void convertsMapsToKeyValueEntries() throws Exception {
        Setup setup = setupOf("{\"type\":\"map\",\"values\":\"long\"}");
        Map<CharSequence, Long> map = new LinkedHashMap<>();
        map.put(new Utf8("a"), 1L);
        map.put(new Utf8("b"), 2L);
        GenericRecord record = setup.record();
        record.put("f", map);

        assertThat(setup.value(setup.convert(record), "f"))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .hasSize(2)
                .extracting(
                        entry -> {
                            DynamicMessage message = (DynamicMessage) entry;
                            Descriptors.Descriptor entryDescriptor = message.getDescriptorForType();
                            return message.getField(entryDescriptor.findFieldByName("key"))
                                    + "="
                                    + message.getField(entryDescriptor.findFieldByName("value"));
                        })
                .containsExactly("a=1", "b=2");
    }

    @Test
    void nullMapBecomesNoEntries() throws Exception {
        Setup setup = setupOf("[\"null\",{\"type\":\"map\",\"values\":\"long\"}]");

        assertThat(setup.value(setup.convert(setup.record()), "f"))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .isEmpty();
    }

    @Test
    void jsonMarkedStringPassesThroughVerbatim() throws Exception {
        Setup setup =
                new Setup(
                        record("{\"name\":\"f\",\"type\":\"string\"}"),
                        AvroSchemaOptions.builder().jsonFieldPath("f").build());
        GenericRecord record = setup.record();
        // Deliberately not valid JSON: the connector does not validate, BigQuery does.
        record.put("f", "{not json");

        assertThat(setup.value(setup.convert(record), "f")).isEqualTo("{not json");
    }

    /**
     * Also the guard on {@code toKind}'s {@code GEOGRAPHY} case. Without it a marked column derives
     * a correct schema and then throws on the first record — from inside the writers' {@code
     * FailureHandler} catch, where a log-and-drop policy would swallow the whole stream.
     */
    @Test
    void geographyMarkedStringPassesThroughVerbatim() throws Exception {
        Setup setup =
                new Setup(
                        record("{\"name\":\"f\",\"type\":\"string\"}"),
                        AvroSchemaOptions.builder().geographyFieldPath("f").build());
        GenericRecord record = setup.record();
        // Deliberately not a valid geometry: the connector does not validate, BigQuery does.
        record.put("f", "POINT(oops");

        assertThat(setup.value(setup.convert(record), "f")).isEqualTo("POINT(oops");
    }

    @Test
    void valueOfTheWrongJavaTypeIsRowLevelFailure() {
        Setup setup = setupOf("\"long\"");
        GenericRecord record = setup.record();
        record.put("f", "not a number");

        assertThatThrownBy(() -> setup.convert(record))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expects an int or long");
    }

    @Test
    void convertsLocalTimestampMillisToPackedDatetime() throws Exception {
        LocalDateTime expected = LocalDateTime.of(2026, 7, 26, 1, 2, 3, 456_000_000);
        Setup setup = setupOf(logical("long", "local-timestamp-millis"));

        GenericRecord raw = setup.record();
        raw.put("f", expected.toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
        assertThat(
                        CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                (Long) setup.value(setup.convert(raw), "f")))
                .isEqualTo(expected);

        GenericRecord converted = setup.record();
        converted.put("f", expected);
        assertThat(
                        CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                (Long) setup.value(setup.convert(converted), "f")))
                .isEqualTo(expected);
    }

    @Test
    void convertsBigNumericDecimals() throws Exception {
        // 20 fractional digits puts the column past NUMERIC and onto the other encoder.
        Setup setup =
                setupOf(
                        "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":50,\"scale\":20}");
        BigDecimal expected = new BigDecimal("1.5").setScale(20);

        GenericRecord raw = setup.record();
        raw.put("f", ByteBuffer.wrap(expected.unscaledValue().toByteArray()));
        assertThat(
                        BigDecimalByteStringEncoder.decodeBigNumericByteString(
                                (ByteString) setup.value(setup.convert(raw), "f")))
                .isEqualByComparingTo(expected);
    }

    @Test
    void convertsFixedBackedDecimals() throws Exception {
        Setup setup =
                setupOf(
                        "{\"type\":\"fixed\",\"name\":\"Dec\",\"size\":8,"
                                + "\"logicalType\":\"decimal\",\"precision\":18,\"scale\":2}");
        BigDecimal expected = new BigDecimal("-12.34");
        byte[] unscaled = expected.unscaledValue().toByteArray();
        byte[] padded = new byte[8];
        java.util.Arrays.fill(padded, (byte) (expected.signum() < 0 ? -1 : 0));
        System.arraycopy(unscaled, 0, padded, padded.length - unscaled.length, unscaled.length);
        GenericRecord record = setup.record();
        record.put("f", new GenericData.Fixed(setup.avroSchema.getField("f").schema(), padded));

        assertThat(
                        BigDecimalByteStringEncoder.decodeNumericByteString(
                                (ByteString) setup.value(setup.convert(record), "f")))
                .isEqualByComparingTo(expected);
    }

    @Test
    void bigDecimalWithMoreScaleThanTheColumnIsRowLevelFailure() {
        Setup setup = setupOf(decimal(10, 2));
        GenericRecord record = setup.record();
        // The byte form of this field cannot express five fractional digits; neither should the
        // BigDecimal form, which would otherwise be rounded server-side without anyone noticing.
        record.put("f", new BigDecimal("1.23456"));

        assertThatThrownBy(() -> setup.convert(record))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    void bigDecimalWithLessScaleThanTheColumnIsScaledUp() throws Exception {
        Setup setup = setupOf(decimal(10, 2));
        GenericRecord record = setup.record();
        record.put("f", new BigDecimal("1.2"));

        assertThat(
                        BigDecimalByteStringEncoder.decodeNumericByteString(
                                (ByteString) setup.value(setup.convert(record), "f")))
                .isEqualByComparingTo(new BigDecimal("1.20"));
    }

    @Test
    void acceptsUuidEnumAndByteArrayRepresentations() throws Exception {
        Setup uuid = setupOf(logical("string", "uuid"));
        GenericRecord uuidRecord = uuid.record();
        java.util.UUID id = java.util.UUID.fromString("6f0d5f6e-3e5a-4a4d-9b8b-2f1c6b6b1a2c");
        uuidRecord.put("f", id);
        assertThat(uuid.value(uuid.convert(uuidRecord), "f")).isEqualTo(id.toString());

        Setup string = setupOf("\"string\"");
        GenericRecord enumRecord = string.record();
        enumRecord.put("f", java.time.DayOfWeek.FRIDAY);
        assertThat(string.value(string.convert(enumRecord), "f")).isEqualTo("FRIDAY");

        Setup bytes = setupOf("\"bytes\"");
        GenericRecord bytesRecord = bytes.record();
        bytesRecord.put("f", new byte[] {4, 5});
        assertThat(bytes.value(bytes.convert(bytesRecord), "f"))
                .isEqualTo(ByteString.copyFrom(new byte[] {4, 5}));
    }

    @Test
    void convertsArraysOfRecordsAndMapsOfRecords() throws Exception {
        String inner =
                "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                        + "[{\"name\":\"n\",\"type\":\"long\"}]}";

        Setup array = setupOf("{\"type\":\"array\",\"items\":" + inner + "}");
        Schema itemSchema = array.avroSchema.getField("f").schema().getElementType();
        GenericRecord item = new GenericData.Record(itemSchema);
        item.put("n", 7L);
        GenericRecord arrayRecord = array.record();
        arrayRecord.put("f", java.util.Collections.singletonList(item));
        assertThat(array.value(array.convert(arrayRecord), "f"))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .singleElement()
                .satisfies(
                        element -> {
                            DynamicMessage message = (DynamicMessage) element;
                            assertThat(
                                            message.getField(
                                                    message.getDescriptorForType()
                                                            .findFieldByName("n")))
                                    .isEqualTo(7L);
                        });

        Setup map = setupOf("{\"type\":\"map\",\"values\":" + inner + "}");
        GenericRecord mapValue =
                new GenericData.Record(map.avroSchema.getField("f").schema().getValueType());
        mapValue.put("n", 9L);
        GenericRecord mapRecord = map.record();
        mapRecord.put("f", java.util.Collections.singletonMap(new Utf8("k"), mapValue));
        assertThat(map.value(map.convert(mapRecord), "f"))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .hasSize(1);
    }

    @Test
    void nullMapKeyIsRowLevelFailure() {
        Setup setup = setupOf("{\"type\":\"map\",\"values\":\"long\"}");
        Map<CharSequence, Long> map = new java.util.HashMap<>();
        map.put(null, 1L);
        GenericRecord record = setup.record();
        record.put("f", map);

        assertThatThrownBy(() -> setup.convert(record))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("f.key")
                .hasMessageContaining("carries null");
    }

    @Test
    void outOfRangeTimeValueIsRowLevelFailure() {
        Setup setup = setupOf(logical("int", "time-millis"));
        GenericRecord record = setup.record();
        record.put("f", 90_000_000);

        assertThatThrownBy(() -> setup.convert(record))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("cannot be represented in the destination column");
    }

    @Test
    void collectionAndRecordReadersReportTheirOwnPath() {
        Setup array = setupOf("{\"type\":\"array\",\"items\":\"long\"}");
        GenericRecord arrayRecord = array.record();
        arrayRecord.put("f", "not a list");
        assertThatThrownBy(() -> array.convert(arrayRecord))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field f expects an array");

        Setup map = setupOf("{\"type\":\"map\",\"values\":\"long\"}");
        GenericRecord mapRecord = map.record();
        mapRecord.put("f", "not a map");
        assertThatThrownBy(() -> map.convert(mapRecord))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field f expects a map");

        Setup struct =
                setupOf(
                        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                                + "[{\"name\":\"n\",\"type\":\"long\"}]}");
        GenericRecord structRecord = struct.record();
        structRecord.put("f", "not a record");
        assertThatThrownBy(() -> struct.convert(structRecord))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field f expects a record");
    }
}
