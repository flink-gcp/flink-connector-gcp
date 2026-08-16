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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ProtoToAvroConverter}, driving it with descriptors derived by the client
 * library's {@code BQTableSchemaToProtoDescriptor} — the same wire form serializers produce.
 */
class ProtoToAvroConverterTest {

    /** One converter setup: schema-derived descriptor, Avro schema, and the converter itself. */
    private static final class Setup {
        private final Descriptors.Descriptor descriptor;
        private final ProtoToAvroConverter converter;

        Setup(TableSchema schema) throws Descriptors.DescriptorValidationException {
            this(
                    schema,
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema));
        }

        /** For the wire forms {@code BQTableSchemaToProtoDescriptor} does not derive. */
        Setup(TableSchema schema, Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
            this.converter =
                    new ProtoToAvroConverter(
                            schema, descriptor, TableSchemaToAvroConverter.convert(schema));
        }

        Descriptors.FieldDescriptor field(String name) {
            return descriptor.findFieldByName(name);
        }
    }

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    private static TableSchema schemaOf(TableFieldSchema... fields) {
        TableSchema.Builder schema = TableSchema.newBuilder();
        for (TableFieldSchema f : fields) {
            schema.addFields(f);
        }
        return schema.build();
    }

    /**
     * A {@code DATETIME} column whose descriptor field is a proto {@code string}. The two sides are
     * derived independently — the plan reads the column type from the table schema and the wire
     * form from the descriptor — so declaring the descriptor's schema {@code STRING} produces
     * exactly the pairing a hand-written serializer creates.
     */
    private static Setup stringDatetimeSetup() throws Descriptors.DescriptorValidationException {
        return new Setup(
                schemaOf(
                        field(
                                "dtt",
                                TableFieldSchema.Type.DATETIME,
                                TableFieldSchema.Mode.NULLABLE)),
                BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                        schemaOf(
                                field(
                                        "dtt",
                                        TableFieldSchema.Type.STRING,
                                        TableFieldSchema.Mode.NULLABLE))));
    }

    private static Object convertDatetime(Setup setup, String literal) throws IOException {
        return setup.converter
                .convert(
                        DynamicMessage.newBuilder(setup.descriptor)
                                .setField(setup.field("dtt"), literal)
                                .build())
                .get("dtt");
    }

    @Test
    void convertsScalarValues() throws Exception {
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "s",
                                        TableFieldSchema.Type.STRING,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "i",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "d",
                                        TableFieldSchema.Type.DOUBLE,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "b",
                                        TableFieldSchema.Type.BOOL,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "y",
                                        TableFieldSchema.Type.BYTES,
                                        TableFieldSchema.Mode.REQUIRED)));
        DynamicMessage row =
                DynamicMessage.newBuilder(setup.descriptor)
                        .setField(setup.field("s"), "hello")
                        .setField(setup.field("i"), 42L)
                        .setField(setup.field("d"), 1.5d)
                        .setField(setup.field("b"), true)
                        .setField(setup.field("y"), ByteString.copyFromUtf8("raw"))
                        .build();

        GenericRecord record = setup.converter.convert(row);

        assertThat(record.get("s")).isEqualTo("hello");
        assertThat(record.get("i")).isEqualTo(42L);
        assertThat(record.get("d")).isEqualTo(1.5d);
        assertThat(record.get("b")).isEqualTo(true);
        assertThat(record.get("y")).isEqualTo(ByteBuffer.wrap("raw".getBytes()));
    }

    @Test
    void convertsJsonColumnsAsStrings() throws Exception {
        // FILE_LOADS sees the row descriptor, in which a JSON column is already a proto string —
        // whether the serializer printed a message into it or passed a JSON string through
        // untouched. This is why JSON-mapped string fields need no handling of their own here.
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "j",
                                        TableFieldSchema.Type.JSON,
                                        TableFieldSchema.Mode.REQUIRED)));
        assertThat(setup.field("j").getJavaType())
                .isEqualTo(Descriptors.FieldDescriptor.JavaType.STRING);

        GenericRecord record =
                setup.converter.convert(
                        DynamicMessage.newBuilder(setup.descriptor)
                                .setField(setup.field("j"), "{\"a\":1}")
                                .build());

        assertThat(record.get("j")).isEqualTo("{\"a\":1}");
    }

    /**
     * The staging half of what makes a marked geography column reachable: the converter has carried
     * {@code GEOGRAPHY} since FILE_LOADS was written, but nothing derived such a column until the
     * serializers could mark one, so this path was never exercised.
     */
    @Test
    void convertsGeographyColumnsAsStrings() throws Exception {
        // NULLABLE, not REQUIRED: a marked column is never REQUIRED, so this is the only mode a
        // geography column reaching FILE_LOADS staging can have.
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "g",
                                        TableFieldSchema.Type.GEOGRAPHY,
                                        TableFieldSchema.Mode.NULLABLE)));
        assertThat(setup.field("g").getJavaType())
                .isEqualTo(Descriptors.FieldDescriptor.JavaType.STRING);

        GenericRecord record =
                setup.converter.convert(
                        DynamicMessage.newBuilder(setup.descriptor)
                                .setField(setup.field("g"), "POINT(1 2)")
                                .build());

        assertThat(record.get("g")).isEqualTo("POINT(1 2)");

        // An unset one stages as null, which is what the NULL geography in BigQueryFileLoadsITCase
        // exercises against the real load job.
        GenericRecord empty =
                setup.converter.convert(DynamicMessage.newBuilder(setup.descriptor).build());
        assertThat(empty.get("g")).isNull();
    }

    @Test
    void convertsTemporalValues() throws Exception {
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "ts",
                                        TableFieldSchema.Type.TIMESTAMP,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "dt",
                                        TableFieldSchema.Type.DATE,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "tm",
                                        TableFieldSchema.Type.TIME,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "dtt",
                                        TableFieldSchema.Type.DATETIME,
                                        TableFieldSchema.Mode.REQUIRED)));
        LocalTime time = LocalTime.of(6, 7, 8, 9_000);
        LocalDateTime datetime = LocalDateTime.of(2024, 3, 5, 6, 7, 8, 9_000);
        DynamicMessage row =
                DynamicMessage.newBuilder(setup.descriptor)
                        .setField(setup.field("ts"), 1_700_000_000_000_000L)
                        .setField(setup.field("dt"), 19_000)
                        .setField(
                                setup.field("tm"),
                                CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(time))
                        .setField(
                                setup.field("dtt"),
                                CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(
                                        datetime))
                        .build();

        GenericRecord record = setup.converter.convert(row);

        assertThat(record.get("ts")).isEqualTo(1_700_000_000_000_000L);
        assertThat(record.get("dt")).isEqualTo(19_000);
        assertThat(record.get("tm")).isEqualTo(time.toNanoOfDay() / 1_000);
        // local-timestamp-micros: micros from 1970-01-01T00:00:00, the civil instant read at UTC.
        assertThat(record.get("dtt"))
                .isEqualTo(datetime.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 9L);
    }

    /**
     * The other {@code DATETIME} wire form: the Storage Write API takes a datetime literal as well
     * as a packed civil-time long, so a hand-written serializer may send one, and it reaches the
     * same {@code local-timestamp-micros} value the packed form does.
     *
     * <p>The spellings are BigQuery's documented grammar, {@code YYYY-[M]M-[D]D[(
     * |T)[H]H:[M]M:[S]S[.F]]} — accepting less would make one serializer work under {@code
     * STORAGE_API_*} and fail under {@code FILE_LOADS} — plus the three unambiguous readings the
     * formatter also takes: omitted seconds, a lowercase separator, and a short year.
     */
    @Test
    void parsesTheDatetimeStringWireForm() throws Exception {
        Setup setup = stringDatetimeSetup();
        long midnight =
                LocalDateTime.of(2026, 8, 4, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        long noonish =
                LocalDateTime.of(2026, 8, 4, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;

        assertThat(convertDatetime(setup, "2026-08-04T12:34:56.123456"))
                .isEqualTo(noonish + 123_456L);
        assertThat(convertDatetime(setup, "2026-08-04 12:34:56")).isEqualTo(noonish);
        assertThat(convertDatetime(setup, "2026-8-4 12:34:56")).isEqualTo(noonish);
        assertThat(convertDatetime(setup, "2026-08-04T12:34")).isEqualTo(noonish - 56_000_000L);
        assertThat(convertDatetime(setup, "2026-08-04t12:34:56")).isEqualTo(noonish);
        assertThat(convertDatetime(setup, "2026-08-04")).isEqualTo(midnight);
        // Sub-microsecond digits are truncated, as they are on every other path into BigQuery.
        assertThat(convertDatetime(setup, "2026-08-04T12:34:56.123456789"))
                .isEqualTo(noonish + 123_456L);
    }

    /**
     * The micros are signed, and the two directions have to agree about which way: {@code
     * LocalDateTime.getNano()} is non-negative even for a negative epoch second, so an
     * implementation adding a signed sub-second part would be wrong only here.
     */
    @Test
    void convertsDatetimesBeforeTheEpoch() throws Exception {
        Setup setup = stringDatetimeSetup();

        assertThat(convertDatetime(setup, "1969-12-31T23:59:59.500000")).isEqualTo(-500_000L);
        assertThat(convertDatetime(setup, "1969-12-31T23:59:59.999999")).isEqualTo(-1L);
        assertThat(convertDatetime(setup, "1970-01-01")).isEqualTo(0L);
    }

    /**
     * A literal the service would reject must not be quietly rewritten into one it accepts. The
     * default {@code SMART} resolver does exactly that — {@code 2026-02-30} resolves to the 28th
     * and {@code 24:00:00} rolls into the next day — so the staged file would carry a date nobody
     * wrote, under a green job.
     */
    @Test
    void datetimeStringOutsideTheCalendarIsRowLevelFailure() throws Exception {
        Setup setup = stringDatetimeSetup();

        assertThatThrownBy(() -> convertDatetime(setup, "2026-02-30"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("dtt");
        assertThatThrownBy(() -> convertDatetime(setup, "2027-02-29"))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> convertDatetime(setup, "2026-08-04 24:00:00"))
                .isInstanceOf(IOException.class);
    }

    /**
     * A load job is all-or-nothing, so a year no {@code DATETIME} column can hold has to fail its
     * own row here rather than the whole job at load time.
     */
    @Test
    void datetimeOutsideBigQuerysYearRangeIsRowLevelFailure() throws Exception {
        Setup setup = stringDatetimeSetup();

        assertThat(convertDatetime(setup, "0001-01-01")).isNotNull();
        assertThat(convertDatetime(setup, "9999-12-31 23:59:59.999999")).isNotNull();
        assertThatThrownBy(() -> convertDatetime(setup, "12026-08-04"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("DATETIME range");
    }

    @Test
    void unparseableDatetimeStringIsRowLevelFailure() throws Exception {
        Setup setup = stringDatetimeSetup();

        assertThatThrownBy(() -> convertDatetime(setup, "04/08/2026"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("dtt");
        // An explicitly-set empty string is a bad value, not a NULL: only an *unset* field is null.
        assertThatThrownBy(() -> convertDatetime(setup, "")).isInstanceOf(IOException.class);
    }

    @Test
    void reencodesDecimalsAsBigEndianAvroDecimals() throws Exception {
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "n",
                                        TableFieldSchema.Type.NUMERIC,
                                        TableFieldSchema.Mode.REQUIRED),
                                field(
                                        "bn",
                                        TableFieldSchema.Type.BIGNUMERIC,
                                        TableFieldSchema.Mode.REQUIRED)));
        BigDecimal numeric = new BigDecimal("123.456");
        BigDecimal bignumeric = new BigDecimal("-9876.5");
        DynamicMessage row =
                DynamicMessage.newBuilder(setup.descriptor)
                        .setField(
                                setup.field("n"),
                                BigDecimalByteStringEncoder.encodeToNumericByteString(numeric))
                        .setField(
                                setup.field("bn"),
                                BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                                        bignumeric))
                        .build();

        GenericRecord record = setup.converter.convert(row);

        assertThat(record.get("n"))
                .isEqualTo(ByteBuffer.wrap(numeric.setScale(9).unscaledValue().toByteArray()));
        assertThat(record.get("bn"))
                .isEqualTo(ByteBuffer.wrap(bignumeric.setScale(38).unscaledValue().toByteArray()));
    }

    @Test
    void decimalExceedingColumnScaleIsRowLevelFailure() throws Exception {
        TableSchema schema =
                schemaOf(
                        TableFieldSchema.newBuilder()
                                .setName("n")
                                .setType(TableFieldSchema.Type.NUMERIC)
                                .setMode(TableFieldSchema.Mode.REQUIRED)
                                .setPrecision(10)
                                .setScale(2)
                                .build());
        Setup setup = new Setup(schema);
        DynamicMessage row =
                DynamicMessage.newBuilder(setup.descriptor)
                        .setField(
                                setup.field("n"),
                                BigDecimalByteStringEncoder.encodeToNumericByteString(
                                        new BigDecimal("1.234")))
                        .build();

        assertThatThrownBy(() -> setup.converter.convert(row))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("n");
    }

    /**
     * The union path, which is what every ordinary job now stages: with {@code NULLABLE} the
     * default column mode on both serializers, a scalar's Avro schema is {@code ["null", T]} rather
     * than a bare type, so the converter has to unwrap it before reading a logical type or
     * descending into a struct. The rest of this class drives {@code REQUIRED} columns, i.e. the
     * bare-type path.
     *
     * <p>A NUMERIC is the case that would fail loudest — {@code unwrap} feeding the decimal's
     * logical type to a cast — and a present-but-default string is the case that would fail
     * quietly, being indistinguishable from an absent one if the presence gate ever regressed.
     */
    @Test
    void convertsNullableColumnsThroughTheUnionPath() throws Exception {
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "n",
                                        TableFieldSchema.Type.NUMERIC,
                                        TableFieldSchema.Mode.NULLABLE),
                                field(
                                        "s",
                                        TableFieldSchema.Type.STRING,
                                        TableFieldSchema.Mode.NULLABLE),
                                TableFieldSchema.newBuilder()
                                        .setName("st")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.NULLABLE)
                                        .addFields(
                                                field(
                                                        "inner",
                                                        TableFieldSchema.Type.STRING,
                                                        TableFieldSchema.Mode.NULLABLE))
                                        .build()));
        Descriptors.Descriptor structType = setup.field("st").getMessageType();
        DynamicMessage row =
                DynamicMessage.newBuilder(setup.descriptor)
                        .setField(
                                setup.field("n"),
                                BigDecimalByteStringEncoder.encodeToNumericByteString(
                                        new BigDecimal("1.500000000")))
                        // Set, but to the type default: an explicit "" must not stage as Avro null.
                        .setField(setup.field("s"), "")
                        .setField(
                                setup.field("st"),
                                DynamicMessage.newBuilder(structType)
                                        .setField(structType.findFieldByName("inner"), "deep")
                                        .build())
                        .build();

        GenericRecord record = setup.converter.convert(row);

        assertThat(record.get("n")).isInstanceOf(ByteBuffer.class);
        assertThat(record.get("s")).isEqualTo("");
        assertThat(((GenericRecord) record.get("st")).get("inner")).isEqualTo("deep");
    }

    @Test
    void absentNullableFieldBecomesNull() throws Exception {
        Setup setup =
                new Setup(
                        schemaOf(
                                field(
                                        "s",
                                        TableFieldSchema.Type.STRING,
                                        TableFieldSchema.Mode.NULLABLE)));
        DynamicMessage row = DynamicMessage.newBuilder(setup.descriptor).build();

        assertThat(setup.converter.convert(row).get("s")).isNull();
    }

    @Test
    void convertsRepeatedAndNestedStructs() throws Exception {
        TableSchema schema =
                schemaOf(
                        TableFieldSchema.newBuilder()
                                .setName("items")
                                .setType(TableFieldSchema.Type.STRUCT)
                                .setMode(TableFieldSchema.Mode.REPEATED)
                                .addFields(
                                        field(
                                                "name",
                                                TableFieldSchema.Type.STRING,
                                                TableFieldSchema.Mode.REQUIRED))
                                .addFields(
                                        field(
                                                "count",
                                                TableFieldSchema.Type.INT64,
                                                TableFieldSchema.Mode.NULLABLE))
                                .build(),
                        field(
                                "tags",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.REPEATED));
        Setup setup = new Setup(schema);
        Descriptors.FieldDescriptor items = setup.field("items");
        Descriptors.Descriptor itemDescriptor = items.getMessageType();
        DynamicMessage item =
                DynamicMessage.newBuilder(itemDescriptor)
                        .setField(itemDescriptor.findFieldByName("name"), "apple")
                        .setField(itemDescriptor.findFieldByName("count"), 3L)
                        .build();
        DynamicMessage row =
                DynamicMessage.newBuilder(setup.descriptor)
                        .addRepeatedField(items, item)
                        .addRepeatedField(setup.field("tags"), "fruit")
                        .addRepeatedField(setup.field("tags"), "fresh")
                        .build();

        GenericRecord record = setup.converter.convert(row);

        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) record.get("tags");
        assertThat(tags).containsExactly("fruit", "fresh");
        List<?> convertedItems = (List<?>) record.get("items");
        assertThat(convertedItems).hasSize(1);
        GenericRecord convertedItem = (GenericRecord) convertedItems.get(0);
        assertThat(convertedItem.get("name")).isEqualTo("apple");
        assertThat(convertedItem.get("count")).isEqualTo(3L);
    }

    @Test
    void descriptorMissingSchemaFieldIsConfigurationError() {
        TableSchema schema =
                schemaOf(
                        field(
                                "present",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.REQUIRED));
        TableSchema wider =
                schemaOf(
                        field(
                                "present",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.REQUIRED),
                        field(
                                "missing",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.REQUIRED));

        assertThatThrownBy(
                        () -> {
                            Descriptors.Descriptor descriptor =
                                    BQTableSchemaToProtoDescriptor
                                            .convertBQTableSchemaToProtoDescriptor(schema);
                            new ProtoToAvroConverter(
                                    wider, descriptor, TableSchemaToAvroConverter.convert(wider));
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing");
    }
}
