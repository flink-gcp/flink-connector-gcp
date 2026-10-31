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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.tables.PrimaryKeyColumn;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRowKeys}. */
class CdcRowKeysTest {

    private static Descriptors.Descriptor descriptor(FieldDescriptorProto... fields) {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Row");
        for (FieldDescriptorProto field : fields) {
            message.addField(field);
        }
        try {
            return Descriptors.FileDescriptor.buildFrom(
                            FileDescriptorProto.newBuilder()
                                    .setName("row.proto")
                                    .setSyntax("proto2")
                                    .addMessageType(message)
                                    .addMessageType(DescriptorProto.newBuilder().setName("Nested"))
                                    .build(),
                            new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName("Row");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static FieldDescriptorProto field(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    private static PrimaryKeyColumn column(String name, StandardSQLTypeName type) {
        return new PrimaryKeyColumn(name, type, null);
    }

    private static ByteString row(Descriptors.Descriptor descriptor, Object... namesAndValues) {
        DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
        for (int i = 0; i < namesAndValues.length; i += 2) {
            message.setField(
                    descriptor.findFieldByName((String) namesAndValues[i]), namesAndValues[i + 1]);
        }
        return message.build().toByteString();
    }

    private static final Descriptors.Descriptor ROW =
            descriptor(
                    field("id", 1, FieldDescriptorProto.Type.TYPE_INT64),
                    field("region", 2, FieldDescriptorProto.Type.TYPE_STRING),
                    field("payload", 3, FieldDescriptorProto.Type.TYPE_STRING));

    private static final CdcRowKeys ID_REGION =
            CdcRowKeys.of(
                    Arrays.asList(
                            column("id", StandardSQLTypeName.INT64),
                            column("region", StandardSQLTypeName.STRING)),
                    ROW);

    @Test
    void rowsOfOneCompositeKeyHaveOneKeyWhateverTheirOtherColumns() {
        ByteString first = ID_REGION.keyOf(row(ROW, "id", 7L, "region", "eu", "payload", "x"));
        ByteString second = ID_REGION.keyOf(row(ROW, "payload", "y", "region", "eu", "id", 7L));

        assertThat(first).isNotNull().isEqualTo(second);
        assertThat(ID_REGION.unreadableReason()).isNull();
    }

    @Test
    void rowsThatDifferInAnyKeyColumnHaveDifferentKeys() {
        ByteString key = ID_REGION.keyOf(row(ROW, "id", 7L, "region", "eu"));

        assertThat(ID_REGION.keyOf(row(ROW, "id", 8L, "region", "eu"))).isNotEqualTo(key);
        assertThat(ID_REGION.keyOf(row(ROW, "id", 7L, "region", "us"))).isNotEqualTo(key);
        // An absent column is a value of its own, not a wildcard.
        assertThat(ID_REGION.keyOf(row(ROW, "id", 7L))).isNotEqualTo(key);
    }

    @Test
    void aValueDoesNotRunIntoTheNextColumn() {
        CdcRowKeys twoStrings =
                CdcRowKeys.of(
                        Arrays.asList(
                                column("region", StandardSQLTypeName.STRING),
                                column("payload", StandardSQLTypeName.STRING)),
                        ROW);

        assertThat(twoStrings.keyOf(row(ROW, "region", "ab", "payload", "c")))
                .isNotEqualTo(twoStrings.keyOf(row(ROW, "region", "a", "payload", "bc")));
    }

    @Test
    void aDefaultValueAndAnAbsentValueAreOneKey() {
        // A field without presence cannot write its default, so the two are merged; merging only
        // makes more rows match.
        assertThat(ID_REGION.keyOf(row(ROW, "id", 0L, "region", "")))
                .isEqualTo(ID_REGION.keyOf(row(ROW)));
    }

    @Test
    void anAbsentSingleColumnKeyIsAValueNotAnUnreadableKey() {
        // A null key would match every row; an absent key column is NULL to BigQuery, one value.
        CdcRowKeys id =
                CdcRowKeys.of(
                        Collections.singletonList(column("id", StandardSQLTypeName.INT64)), ROW);

        assertThat(id.keyOf(row(ROW, "payload", "x")))
                .isNotNull()
                .isEqualTo(id.keyOf(row(ROW, "id", 0L)))
                .isNotEqualTo(id.keyOf(row(ROW, "id", 1L)));
    }

    @Test
    void theLastOccurrenceOfAFieldIsTheValue() {
        ByteString twice =
                row(ROW, "id", 1L, "region", "eu").concat(row(ROW, "id", 2L, "region", "eu"));

        assertThat(ID_REGION.keyOf(twice))
                .isEqualTo(ID_REGION.keyOf(row(ROW, "id", 2L, "region", "eu")));
    }

    @Test
    void columnNamesMatchFieldNamesCaseInsensitively() {
        CdcRowKeys upper =
                CdcRowKeys.of(
                        Collections.singletonList(column("ID", StandardSQLTypeName.INT64)), ROW);

        assertThat(upper.keyOf(row(ROW, "id", 1L))).isNotEqualTo(upper.keyOf(row(ROW, "id", 2L)));
    }

    @Test
    void aKeyColumnTheDescriptorLacksIsAbsentInEveryRow() {
        CdcRowKeys withMissing =
                CdcRowKeys.of(
                        Arrays.asList(
                                column("id", StandardSQLTypeName.INT64),
                                column("tenant", StandardSQLTypeName.STRING)),
                        ROW);

        assertThat(withMissing.unreadableReason()).isNull();
        assertThat(withMissing.keyOf(row(ROW, "id", 1L, "payload", "x")))
                .isEqualTo(withMissing.keyOf(row(ROW, "id", 1L, "payload", "y")))
                .isNotEqualTo(withMissing.keyOf(row(ROW, "id", 2L)));
    }

    @Test
    void aBoolIsReadByTruthNotByItsVarint() throws IOException {
        Descriptors.Descriptor flags =
                descriptor(field("flag", 1, FieldDescriptorProto.Type.TYPE_BOOL));
        CdcRowKeys keys =
                CdcRowKeys.of(
                        Collections.singletonList(column("flag", StandardSQLTypeName.BOOL)), flags);

        assertThat(keys.keyOf(rawVarint(1, 2))).isEqualTo(keys.keyOf(row(flags, "flag", true)));
    }

    @Test
    void anInt32IsReadByTheValueAParserKeeps() throws IOException {
        Descriptors.Descriptor ints =
                descriptor(field("n", 1, FieldDescriptorProto.Type.TYPE_INT32));
        CdcRowKeys keys =
                CdcRowKeys.of(
                        Collections.singletonList(column("n", StandardSQLTypeName.INT64)), ints);

        // A parser keeps the low 32 bits of the varint.
        assertThat(keys.keyOf(rawVarint(1, (1L << 32) | 5)))
                .isEqualTo(keys.keyOf(row(ints, "n", 5)));
        assertThat(keys.keyOf(row(ints, "n", -1))).isNotEqualTo(keys.keyOf(row(ints, "n", 1)));
    }

    @Test
    void aDecimalInBytesIsReadWithoutRedundantSignBytes() {
        Descriptors.Descriptor decimals =
                descriptor(field("amount", 1, FieldDescriptorProto.Type.TYPE_BYTES));
        CdcRowKeys keys =
                CdcRowKeys.of(
                        Collections.singletonList(column("amount", StandardSQLTypeName.NUMERIC)),
                        decimals);
        ByteString minimal =
                BigDecimalByteStringEncoder.encodeToNumericByteString(new BigDecimal("-1.5"));
        ByteString padded = minimal.concat(ByteString.copyFrom(new byte[] {(byte) 0xFF, -1}));

        assertThat(keys.keyOf(row(decimals, "amount", padded)))
                .isEqualTo(keys.keyOf(row(decimals, "amount", minimal)))
                .isNotEqualTo(
                        keys.keyOf(
                                row(
                                        decimals,
                                        "amount",
                                        BigDecimalByteStringEncoder.encodeToNumericByteString(
                                                new BigDecimal("1.5")))));
    }

    @Test
    void aDecimalFinerThanItsColumnsScaleIsUnreadable() {
        // NUMERIC(5, 2) rounds 1.001 and 1.004 to the same 1.00, which their bytes cannot show.
        Descriptors.Descriptor decimals =
                descriptor(field("amount", 1, FieldDescriptorProto.Type.TYPE_BYTES));
        CdcRowKeys scaled =
                CdcRowKeys.of(
                        Collections.singletonList(
                                new PrimaryKeyColumn(
                                        "amount", StandardSQLTypeName.NUMERIC, null, 2L)),
                        decimals);
        CdcRowKeys unscaled =
                CdcRowKeys.of(
                        Collections.singletonList(column("amount", StandardSQLTypeName.NUMERIC)),
                        decimals);

        assertThat(scaled.keyOf(row(decimals, "amount", numeric("1.001")))).isNull();
        assertThat(scaled.keyOf(row(decimals, "amount", numeric("-1.004")))).isNull();
        assertThat(scaled.keyOf(row(decimals, "amount", numeric("1.10"))))
                .isNotNull()
                .isEqualTo(scaled.keyOf(row(decimals, "amount", numeric("1.1"))))
                .isNotEqualTo(scaled.keyOf(row(decimals, "amount", numeric("-1.1"))));
        // Without a declared scale BigQuery keeps every digit the encoding carries.
        assertThat(unscaled.keyOf(row(decimals, "amount", numeric("1.001"))))
                .isNotNull()
                .isNotEqualTo(unscaled.keyOf(row(decimals, "amount", numeric("1.004"))));
    }

    @Test
    void aBigNumericKeyIsCheckedAgainstItsOwnEncodedScale() {
        Descriptors.Descriptor decimals =
                descriptor(field("amount", 1, FieldDescriptorProto.Type.TYPE_BYTES));
        CdcRowKeys scaled =
                CdcRowKeys.of(
                        Collections.singletonList(
                                new PrimaryKeyColumn(
                                        "amount", StandardSQLTypeName.BIGNUMERIC, null, 10L)),
                        decimals);

        assertThat(scaled.keyOf(row(decimals, "amount", bigNumeric("1.0000000001")))).isNotNull();
        assertThat(scaled.keyOf(row(decimals, "amount", bigNumeric("1.00000000001")))).isNull();
    }

    private static ByteString numeric(String value) {
        return BigDecimalByteStringEncoder.encodeToNumericByteString(new BigDecimal(value));
    }

    private static ByteString bigNumeric(String value) {
        return BigDecimalByteStringEncoder.encodeToBigNumericByteString(new BigDecimal(value));
    }

    @Test
    void minimalTwosComplementKeepsTheSignByteAValueNeeds() {
        // 0x80 alone would read as -128, so 128 keeps its zero byte; -1 padded is one 0xFF.
        assertThat(CdcRowKeys.minimalTwosComplement(bytes(0x80, 0x00, 0x00)))
                .isEqualTo(bytes(0x80, 0x00));
        assertThat(CdcRowKeys.minimalTwosComplement(bytes(0xFF, 0xFF, 0xFF)))
                .isEqualTo(bytes(0xFF));
        assertThat(CdcRowKeys.minimalTwosComplement(bytes(0x00, 0x00))).isEqualTo(ByteString.EMPTY);
        assertThat(CdcRowKeys.minimalTwosComplement(bytes(0x7F, 0xFF)))
                .isEqualTo(bytes(0x7F, 0xFF));
    }

    @Test
    void aBytesColumnIsComparedAsWritten() {
        // A BYTES column has no sign to normalize: 0x80 0x00 and 0x80 are different values.
        Descriptors.Descriptor blobs =
                descriptor(field("blob", 1, FieldDescriptorProto.Type.TYPE_BYTES));
        CdcRowKeys keys =
                CdcRowKeys.of(
                        Collections.singletonList(column("blob", StandardSQLTypeName.BYTES)),
                        blobs);

        assertThat(keys.keyOf(row(blobs, "blob", bytes(0x80, 0x00))))
                .isNotEqualTo(keys.keyOf(row(blobs, "blob", bytes(0x80))));
    }

    @Test
    void encodingsWithSeveralSpellingsOfOneValueAreUnreadable() {
        Descriptors.Descriptor mixed =
                descriptor(
                        field("text", 1, FieldDescriptorProto.Type.TYPE_STRING),
                        field("real", 2, FieldDescriptorProto.Type.TYPE_DOUBLE),
                        FieldDescriptorProto.newBuilder(
                                        field("many", 3, FieldDescriptorProto.Type.TYPE_INT64))
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                .build(),
                        FieldDescriptorProto.newBuilder(
                                        field("nested", 4, FieldDescriptorProto.Type.TYPE_MESSAGE))
                                .setTypeName("Nested")
                                .build(),
                        field("flag", 5, FieldDescriptorProto.Type.TYPE_BOOL));

        for (PrimaryKeyColumn column :
                Arrays.asList(
                        column("text", StandardSQLTypeName.TIMESTAMP),
                        column("text", StandardSQLTypeName.NUMERIC),
                        column("text", StandardSQLTypeName.DATE),
                        column("text", StandardSQLTypeName.BYTES),
                        new PrimaryKeyColumn("text", StandardSQLTypeName.STRING, "und:ci"),
                        column("real", StandardSQLTypeName.NUMERIC),
                        column("many", StandardSQLTypeName.INT64),
                        column("nested", StandardSQLTypeName.STRING),
                        column("flag", StandardSQLTypeName.INT64))) {
            CdcRowKeys keys = CdcRowKeys.of(Collections.singletonList(column), mixed);

            assertThat(keys.unreadableReason()).as(column.toString()).contains(column.toString());
            assertThat(keys.keyOf(row(mixed))).as(column.toString()).isNull();
        }
    }

    @Test
    void readableEncodingsAreReadable() {
        Descriptors.Descriptor mixed =
                descriptor(
                        field("text", 1, FieldDescriptorProto.Type.TYPE_STRING),
                        field("blob", 2, FieldDescriptorProto.Type.TYPE_BYTES),
                        field("number", 3, FieldDescriptorProto.Type.TYPE_SFIXED64),
                        field("flag", 4, FieldDescriptorProto.Type.TYPE_BOOL));
        List<PrimaryKeyColumn> columns =
                Arrays.asList(
                        column("text", StandardSQLTypeName.STRING),
                        column("blob", StandardSQLTypeName.STRING),
                        column("blob", StandardSQLTypeName.BYTES),
                        column("blob", StandardSQLTypeName.BIGNUMERIC),
                        column("number", StandardSQLTypeName.INT64),
                        column("number", StandardSQLTypeName.DATE),
                        column("number", StandardSQLTypeName.DATETIME),
                        column("number", StandardSQLTypeName.TIMESTAMP),
                        column("number", StandardSQLTypeName.NUMERIC),
                        column("flag", StandardSQLTypeName.BOOL));

        for (PrimaryKeyColumn column : columns) {
            assertThat(CdcRowKeys.of(Collections.singletonList(column), mixed).unreadableReason())
                    .as(column.toString())
                    .isNull();
        }
    }

    @Test
    void aTableWithoutAPrimaryKeyIsUnreadable() {
        CdcRowKeys keys = CdcRowKeys.of(Collections.emptyList(), ROW);

        assertThat(keys.unreadableReason()).contains("no primary key");
        assertThat(keys.keyOf(row(ROW, "id", 1L))).isNull();
    }

    @Test
    void aRowThatDoesNotParseHasNoKey() throws IOException {
        // Field 1 is an INT64 key, so a length-delimited value under it is mistyped, and a
        // truncated varint does not parse at all.
        ByteString.Output out = ByteString.newOutput();
        CodedOutputStream stream = CodedOutputStream.newInstance(out);
        stream.writeString(1, "seven");
        stream.flush();

        assertThat(ID_REGION.keyOf(out.toByteString())).isNull();
        assertThat(ID_REGION.keyOf(bytes(0x08, 0x80))).isNull();
    }

    @Test
    void aKeyFieldInTheWrongWireTypeMakesTheKeyUnreadable() {
        // A parser keeps a mistyped value as an unknown field, so the service would not see it
        // as the column's value. Read as bytes, the varint 0 here would pass for an empty string
        // and the row would look like one with no region.
        ByteString mistyped = bytes(0x10, 0x00).concat(row(ROW, "id", 7L));

        assertThat(ID_REGION.keyOf(mistyped)).isNull();
    }

    private static ByteString rawVarint(int fieldNumber, long value) throws IOException {
        ByteString.Output out = ByteString.newOutput();
        CodedOutputStream stream = CodedOutputStream.newInstance(out);
        stream.writeUInt64(fieldNumber, value);
        stream.flush();
        return out.toByteString();
    }

    private static ByteString bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return ByteString.copyFrom(bytes);
    }
}
