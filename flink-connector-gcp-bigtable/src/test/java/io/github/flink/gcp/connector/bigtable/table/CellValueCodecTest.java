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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the cell encoding to exact bytes, one vector per type.
 *
 * <p>The encoding is the HBase ecosystem's and exists to be byte-compatible with it, so a test that
 * only round-tripped through this connector's own code would pass while the interop it was chosen
 * for was broken. Each expectation below is therefore a literal, written as hex, and was computed
 * from {@code org.apache.hadoop.hbase.util.Bytes} 2.6.6 as {@code HBaseSerde} 4.0.0-1.19 applies
 * it.
 */
class CellValueCodecTest {

    private static final byte[] NULL_STRING = "NULL".getBytes(StandardCharsets.UTF_8);

    /** One constructible type per {@link LogicalTypeRoot} the two switches could disagree on. */
    private static final Map<LogicalTypeRoot, LogicalType> SAMPLES = samples();

    private static Map<LogicalTypeRoot, LogicalType> samples() {
        Map<LogicalTypeRoot, LogicalType> map = new EnumMap<>(LogicalTypeRoot.class);
        for (DataType type :
                Arrays.asList(
                        DataTypes.CHAR(1),
                        DataTypes.VARCHAR(8),
                        DataTypes.STRING(),
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(1),
                        DataTypes.VARBINARY(8),
                        DataTypes.BYTES(),
                        DataTypes.DECIMAL(5, 2),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DATE(),
                        DataTypes.TIME(3),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_WITH_TIME_ZONE(3),
                        DataTypes.INTERVAL(DataTypes.MONTH()),
                        DataTypes.INTERVAL(DataTypes.SECOND(3)),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MULTISET(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT())),
                        DataTypes.NULL())) {
            map.put(type.getLogicalType().getTypeRoot(), type.getLogicalType());
        }
        return map;
    }

    private static String encodeHex(DataType type, Object value) {
        return hex(
                CellValueCodec.encoder(type.getLogicalType()).encode(GenericRowData.of(value), 0));
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    @Nested
    class GoldenVectors {

        @Test
        void aStringIsItsUtf8Bytes() {
            assertThat(encodeHex(DataTypes.STRING(), StringData.fromString("abc")))
                    .isEqualTo("616263");
            // Multi-byte, so a charset slip cannot pass: U+3042 HIRAGANA LETTER A.
            assertThat(encodeHex(DataTypes.STRING(), StringData.fromString("あ")))
                    .isEqualTo("e38182");
        }

        @Test
        void aBooleanIsOneByteAndTrueIsAllOnes() {
            // Not 0x01: HBase writes (byte) -1 for true, and a reader that tests for 0x01 would
            // disagree with every table the HBase connector has written.
            assertThat(encodeHex(DataTypes.BOOLEAN(), true)).isEqualTo("ff");
            assertThat(encodeHex(DataTypes.BOOLEAN(), false)).isEqualTo("00");
        }

        @Test
        void binaryIsPassedThrough() {
            assertThat(encodeHex(DataTypes.BYTES(), new byte[] {0x01, (byte) 0xfe}))
                    .isEqualTo("01fe");
        }

        @Test
        void aDecimalIsAFourByteScaleThenTheUnscaledValue() {
            assertThat(
                            encodeHex(
                                    DataTypes.DECIMAL(5, 2),
                                    DecimalData.fromBigDecimal(new BigDecimal("123.45"), 5, 2)))
                    .isEqualTo("00000002" + "3039");
            // A negative unscaled value keeps BigInteger's two's-complement shortest form.
            assertThat(
                            encodeHex(
                                    DataTypes.DECIMAL(5, 2),
                                    DecimalData.fromBigDecimal(new BigDecimal("-1.00"), 5, 2)))
                    .isEqualTo("00000002" + "9c");
        }

        @Test
        void aTinyintIsOneByteRatherThanTwo() {
            // The trap this pins: a byte widens to short, so routing it through the numeric
            // encoder would produce two bytes and silently break interop for this one type.
            assertThat(encodeHex(DataTypes.TINYINT(), (byte) 42)).isEqualTo("2a");
            assertThat(encodeHex(DataTypes.TINYINT(), (byte) -1)).isEqualTo("ff");
        }

        @Test
        void integralsAreBigEndianTwosComplement() {
            assertThat(encodeHex(DataTypes.SMALLINT(), (short) 258)).isEqualTo("0102");
            assertThat(encodeHex(DataTypes.SMALLINT(), (short) -2)).isEqualTo("fffe");
            assertThat(encodeHex(DataTypes.INT(), 66051)).isEqualTo("00010203");
            assertThat(encodeHex(DataTypes.INT(), -2)).isEqualTo("fffffffe");
            assertThat(encodeHex(DataTypes.BIGINT(), 1L)).isEqualTo("0000000000000001");
            assertThat(encodeHex(DataTypes.BIGINT(), -2L)).isEqualTo("fffffffffffffffe");
        }

        @Test
        void floatingPointIsItsIeee754Bits() {
            assertThat(encodeHex(DataTypes.FLOAT(), 1.0f)).isEqualTo("3f800000");
            assertThat(encodeHex(DataTypes.DOUBLE(), 1.0d)).isEqualTo("3ff0000000000000");
        }

        @Test
        void aDateIsAFourByteDayCount() {
            // Four bytes of days, not eight of epoch millis: HBaseSerde encodes a RowData DATE
            // through Bytes.toBytes(int), and the older Row/java.sql path of that connector — which
            // does use millis — is not what a Flink SQL job writes.
            assertThat(encodeHex(DataTypes.DATE(), 19723)).isEqualTo("00004d0b");
        }

        @Test
        void aTimeIsAFourByteMillisecondOfDay() {
            assertThat(encodeHex(DataTypes.TIME(3), 3_600_000)).isEqualTo("0036ee80");
        }

        @Test
        void aTimestampIsAnEightByteEpochMilli() {
            assertThat(
                            encodeHex(
                                    DataTypes.TIMESTAMP(3),
                                    TimestampData.fromEpochMillis(1_700_000_000_000L)))
                    .isEqualTo("0000018bcfe56800");
            assertThat(
                            encodeHex(
                                    DataTypes.TIMESTAMP_LTZ(3),
                                    TimestampData.fromEpochMillis(1_700_000_000_000L)))
                    .isEqualTo("0000018bcfe56800");
        }

        @Test
        void intervalsFollowTheirUnderlyingIntegral() {
            assertThat(encodeHex(DataTypes.INTERVAL(DataTypes.MONTH()), 14)).isEqualTo("0000000e");
            assertThat(encodeHex(DataTypes.INTERVAL(DataTypes.SECOND(3)), 1_500L))
                    .isEqualTo("00000000000005dc");
        }
    }

    @Nested
    class Nulls {

        @Test
        void aNullStringIsTheNullStringLiteral() {
            // A string cannot use the empty cell every other type uses for a null, because an
            // empty string is a value in its own right.
            LogicalType type = DataTypes.STRING().getLogicalType();
            byte[] encoded =
                    CellValueCodec.nullableEncoder(type, NULL_STRING)
                            .encode(GenericRowData.of((Object) null), 0);

            assertThat(encoded).isEqualTo(NULL_STRING);
        }

        @Test
        void aNullOfAnyOtherTypeIsAnEmptyCell() {
            LogicalType type = DataTypes.BIGINT().getLogicalType();
            byte[] encoded =
                    CellValueCodec.nullableEncoder(type, NULL_STRING)
                            .encode(GenericRowData.of((Object) null), 0);

            assertThat(encoded).isEmpty();
        }

        @Test
        void aPresentValueIsUnaffectedByTheNullWrapper() {
            LogicalType type = DataTypes.STRING().getLogicalType();
            RowData row = GenericRowData.of(StringData.fromString("x"));

            assertThat(CellValueCodec.nullableEncoder(type, NULL_STRING).encode(row, 0))
                    .isEqualTo(new byte[] {'x'});
        }

        @Test
        void aNotNullColumnGetsThePlainEncoder() {
            // The branch is observable, and only this way: a NOT NULL column pays for no null
            // check, so a null reaching one — which the declared type says cannot happen — hits the
            // encoder rather than quietly becoming the null literal. Asserting that it still
            // encodes a present value would pass with the fast path deleted.
            LogicalType type = DataTypes.STRING().notNull().getLogicalType();
            CellValueCodec.FieldEncoder encoder = CellValueCodec.nullableEncoder(type, NULL_STRING);

            assertThat(encoder.encode(GenericRowData.of(StringData.fromString("x")), 0))
                    .isEqualTo(new byte[] {'x'});
            assertThatThrownBy(() -> encoder.encode(GenericRowData.of((Object) null), 0))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Rejections {

        @Test
        void aTypeWithNoCellEncodingIsRejectedByName() {
            assertThatThrownBy(
                            () ->
                                    CellValueCodec.checkSupported(
                                            "cf.tags",
                                            DataTypes.ARRAY(DataTypes.STRING()).getLogicalType()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("cf.tags")
                    .hasMessageContaining("no Bigtable cell encoding");
        }

        @Test
        void aMapIsRejectedToo() {
            assertThatThrownBy(
                            () ->
                                    CellValueCodec.checkSupported(
                                            "cf.m",
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())
                                                    .getLogicalType()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("no Bigtable cell encoding");
        }

        @Test
        void aTimePrecisionFinerThanMillisecondsIsRejected() {
            assertThatThrownBy(
                            () ->
                                    CellValueCodec.checkSupported(
                                            "cf.t", DataTypes.TIME(6).getLogicalType()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("the cell stores milliseconds");
        }

        @Test
        void aTimestampPrecisionFinerThanMillisecondsIsRejected() {
            assertThatThrownBy(
                            () ->
                                    CellValueCodec.checkSupported(
                                            "cf.ts", DataTypes.TIMESTAMP(6).getLogicalType()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("the cell stores milliseconds");
            assertThatThrownBy(
                            () ->
                                    CellValueCodec.checkSupported(
                                            "cf.ts", DataTypes.TIMESTAMP_LTZ(9).getLogicalType()))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("the cell stores milliseconds");
        }

        @Test
        void theTwoSwitchesAgreeOnEveryTypeRoot() {
            // checkSupported and encoder are parallel switches over LogicalTypeRoot with nothing
            // holding them in step: a root added to one alone is either unreachable or reaches the
            // IllegalStateException backstop at plan time, in a message written for neither a user
            // nor a maintainer. This walks every root and requires the two to answer the same way.
            for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
                LogicalType type = SAMPLES.get(root);
                if (type == null) {
                    // A root with no constructible sample here — a structured or user-defined
                    // type. Neither switch names one, so there is nothing for them to disagree on.
                    continue;
                }
                boolean checked = true;
                try {
                    CellValueCodec.checkSupported("c", type);
                } catch (ValidationException e) {
                    checked = false;
                }
                boolean encodable = true;
                try {
                    CellValueCodec.encoder(type);
                } catch (IllegalStateException e) {
                    encodable = false;
                }
                assertThat(encodable).as("root %s", root).isEqualTo(checked);
            }
        }

        @Test
        void aMillisecondPrecisionIsAccepted() {
            // The control arm for the two rejections above: without it, a check that rejected
            // every precision would read exactly the same.
            CellValueCodec.checkSupported("cf.t", DataTypes.TIME(3).getLogicalType());
            CellValueCodec.checkSupported("cf.ts", DataTypes.TIMESTAMP(3).getLogicalType());
            CellValueCodec.checkSupported("cf.ts", DataTypes.TIMESTAMP_LTZ(0).getLogicalType());
        }
    }
}
