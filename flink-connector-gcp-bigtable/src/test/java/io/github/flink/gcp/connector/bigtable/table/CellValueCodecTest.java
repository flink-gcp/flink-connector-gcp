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
import org.apache.flink.util.InstantiationUtil;

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
 * Pins the cell encoding and decoding to exact bytes, one vector per type.
 *
 * <p>The encoding is the HBase ecosystem's and exists to be byte-compatible with it, so a test that
 * only round-tripped through this connector's own code would pass while the interop it was chosen
 * for was broken. Each expectation below is therefore a literal, written as hex, and was computed
 * from {@code org.apache.hadoop.hbase.util.Bytes} 2.6.6 as {@code HBaseSerde} 4.0.0-1.19 applies
 * it; the decode half reads the same literals back rather than whatever the encode half produced.
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

    private static Object decodeHex(DataType type, String hexBytes) {
        return CellValueCodec.decoder(type.getLogicalType()).decode(fromHex(hexBytes));
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    private static byte[] fromHex(String hexBytes) {
        byte[] bytes = new byte[hexBytes.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hexBytes.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
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

    /**
     * The same vectors read back. Each expectation decodes the literal bytes the encoding half
     * pins, so the two halves are held to one interop rather than merely to each other.
     */
    @Nested
    class DecodeVectors {

        @Test
        void aStringDecodesFromItsUtf8Bytes() {
            assertThat(decodeHex(DataTypes.STRING(), "616263"))
                    .isEqualTo(StringData.fromString("abc"));
            assertThat(decodeHex(DataTypes.STRING(), "e38182"))
                    .isEqualTo(StringData.fromString("あ"));
        }

        @Test
        void anyNonzeroByteDecodesAsTrue() {
            // Bytes.toBoolean's rule is a zero test, not an equality test against the 0xFF this
            // encoder writes — a cell written as 0x01 by another client still reads as true.
            assertThat(decodeHex(DataTypes.BOOLEAN(), "ff")).isEqualTo(true);
            assertThat(decodeHex(DataTypes.BOOLEAN(), "01")).isEqualTo(true);
            assertThat(decodeHex(DataTypes.BOOLEAN(), "00")).isEqualTo(false);
        }

        @Test
        void binaryIsPassedThroughUnchanged() {
            assertThat(decodeHex(DataTypes.BYTES(), "01fe"))
                    .isEqualTo(new byte[] {0x01, (byte) 0xfe});
        }

        @Test
        void aDecimalDecodesItsStoredScaleThenRescales() {
            assertThat(decodeHex(DataTypes.DECIMAL(5, 2), "00000002" + "3039"))
                    .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("123.45"), 5, 2));
            assertThat(decodeHex(DataTypes.DECIMAL(5, 2), "00000002" + "9c"))
                    .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("-1.00"), 5, 2));
            // A cell written at another scale still reads: the stored scale is the cell's, and
            // the value is rescaled to the declared column type afterwards.
            assertThat(decodeHex(DataTypes.DECIMAL(5, 2), "00000001" + "04d2"))
                    .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("123.4"), 5, 2));
        }

        @Test
        void aDecimalTooWideForTheDeclaredTypeIsRejected() {
            // DecimalData.fromBigDecimal answers an overflow with null, which Flink's HBase
            // connector passes through as a SQL NULL — aliasing a cell holding 12345678.90 onto
            // the empty-cell null convention, or handing a NOT NULL field a null the planner was
            // told cannot exist. Here it is a decode failure instead (#1038); the docs page
            // states the divergence. The message names the stored value's dimensions and the
            // declared type, never the value itself.
            assertThatThrownBy(() -> decodeHex(DataTypes.DECIMAL(5, 2), "00000002" + "499602d2"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("precision 10 and scale 2")
                    .hasMessageContaining("DECIMAL(5, 2)")
                    .message()
                    .doesNotContain("12345678");
            // The bound is exact: a value filling the declared precision still decodes.
            assertThat(decodeHex(DataTypes.DECIMAL(5, 2), "00000002" + "01869f"))
                    .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("999.99"), 5, 2));
            // And it is judged after rounding: 999.995 stored at scale 3 rounds to 1000.00,
            // which no longer fits — the stored digits alone look representable, which is why
            // the message reports the rounded precision too.
            assertThatThrownBy(() -> decodeHex(DataTypes.DECIMAL(5, 2), "00000003" + "0f423b"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("precision 6 and scale 3")
                    .hasMessageContaining("needs precision 6");
        }

        @Test
        void aTinyintIsItsOneByte() {
            assertThat(decodeHex(DataTypes.TINYINT(), "2a")).isEqualTo((byte) 42);
            assertThat(decodeHex(DataTypes.TINYINT(), "ff")).isEqualTo((byte) -1);
        }

        @Test
        void integralsDecodeBigEndianTwosComplement() {
            assertThat(decodeHex(DataTypes.SMALLINT(), "0102")).isEqualTo((short) 258);
            assertThat(decodeHex(DataTypes.SMALLINT(), "fffe")).isEqualTo((short) -2);
            assertThat(decodeHex(DataTypes.INT(), "00010203")).isEqualTo(66051);
            assertThat(decodeHex(DataTypes.INT(), "fffffffe")).isEqualTo(-2);
            assertThat(decodeHex(DataTypes.BIGINT(), "0000000000000001")).isEqualTo(1L);
            assertThat(decodeHex(DataTypes.BIGINT(), "fffffffffffffffe")).isEqualTo(-2L);
        }

        @Test
        void aFixedWidthDecoderIgnoresTrailingBytesLikeHBaseBytes() {
            // Bytes.toShort/toInt/toLong(byte[]) read exactly their declared width from offset
            // zero and ignore a longer array's tail. Preserve that interoperability detail while
            // still rejecting a value too short to hold the declared layout.
            assertThat(decodeHex(DataTypes.SMALLINT(), "01027f")).isEqualTo((short) 258);
            assertThat(decodeHex(DataTypes.INT(), "000102037f")).isEqualTo(66051);
            assertThat(decodeHex(DataTypes.BIGINT(), "00000000000000017f")).isEqualTo(1L);
        }

        @Test
        void floatingPointDecodesItsIeee754Bits() {
            assertThat(decodeHex(DataTypes.FLOAT(), "3f800000")).isEqualTo(1.0f);
            assertThat(decodeHex(DataTypes.DOUBLE(), "3ff0000000000000")).isEqualTo(1.0d);
        }

        @Test
        void temporalsDecodeTheirIntegralLayouts() {
            assertThat(decodeHex(DataTypes.DATE(), "00004d0b")).isEqualTo(19723);
            assertThat(decodeHex(DataTypes.TIME(3), "0036ee80")).isEqualTo(3_600_000);
            assertThat(decodeHex(DataTypes.TIMESTAMP(3), "0000018bcfe56800"))
                    .isEqualTo(TimestampData.fromEpochMillis(1_700_000_000_000L));
            assertThat(decodeHex(DataTypes.TIMESTAMP_LTZ(3), "0000018bcfe56800"))
                    .isEqualTo(TimestampData.fromEpochMillis(1_700_000_000_000L));
        }

        @Test
        void intervalsDecodeTheirUnderlyingIntegral() {
            assertThat(decodeHex(DataTypes.INTERVAL(DataTypes.MONTH()), "0000000e")).isEqualTo(14);
            assertThat(decodeHex(DataTypes.INTERVAL(DataTypes.SECOND(3)), "00000000000005dc"))
                    .isEqualTo(1_500L);
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

        @Test
        void theNullStringLiteralDecodesAsNull() {
            LogicalType type = DataTypes.STRING().getLogicalType();

            assertThat(CellValueCodec.nullableDecoder(type, NULL_STRING).decode(NULL_STRING))
                    .isNull();
        }

        @Test
        void anEmptyCellDecodesAsNullForEveryTypeButAString() {
            LogicalType type = DataTypes.BIGINT().getLogicalType();

            assertThat(CellValueCodec.nullableDecoder(type, NULL_STRING).decode(new byte[0]))
                    .isNull();
            // BYTES too, although an empty byte array is — like the empty string — a value in its
            // own right: only a character string gets the literal marker, so for BYTES a null and
            // a zero-length value are the same bytes, the collision the docs page names.
            assertThat(
                            CellValueCodec.nullableDecoder(
                                            DataTypes.BYTES().getLogicalType(), NULL_STRING)
                                    .decode(new byte[0]))
                    .isNull();
        }

        @Test
        void anEmptyCellDecodesAsAnEmptyString() {
            // The reason a string's null is the literal rather than the empty cell: an empty
            // string is a value in its own right, and this is where that pays off on the way out.
            LogicalType type = DataTypes.STRING().getLogicalType();

            assertThat(CellValueCodec.nullableDecoder(type, NULL_STRING).decode(new byte[0]))
                    .isEqualTo(StringData.fromString(""));
        }

        @Test
        void aPresentValueIsUnaffectedByTheNullUnwrapper() {
            LogicalType type = DataTypes.STRING().getLogicalType();

            assertThat(CellValueCodec.nullableDecoder(type, NULL_STRING).decode(new byte[] {'x'}))
                    .isEqualTo(StringData.fromString("x"));
        }

        @Test
        void aNotNullColumnGetsThePlainDecoder() {
            // The decode mirror of the branch above: a NOT NULL string column never wrote the
            // null literal, so bytes that happen to equal it are that text, not a null.
            LogicalType type = DataTypes.STRING().notNull().getLogicalType();

            assertThat(CellValueCodec.nullableDecoder(type, NULL_STRING).decode(NULL_STRING))
                    .isEqualTo(StringData.fromString("NULL"));
        }

        @Test
        void underNotNullOnlyStringsAndBytesSurviveAnEmptyCell() {
            // The SQL page tells a reader which declared types can hold an empty cell under a
            // NOT NULL qualifier and which fail the read. Nothing else pins that split: the plain
            // decoder a NOT NULL column gets has no null to offer, so the answer is whether the
            // decoder reads a byte at all. Walked over SAMPLES rather than a hand-written list, so
            // a root added to checkSupported later has to declare which side it falls on instead
            // of going unasserted.
            for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
                LogicalType sample = SAMPLES.get(root);
                if (sample == null) {
                    continue;
                }
                try {
                    CellValueCodec.checkSupported("c", sample);
                } catch (ValidationException e) {
                    // Rejected where the table is declared, so it never reaches a read.
                    continue;
                }
                CellValueCodec.FieldDecoder decoder =
                        CellValueCodec.nullableDecoder(sample.copy(false), NULL_STRING);

                switch (root) {
                    case CHAR:
                    case VARCHAR:
                        assertThat(decoder.decode(new byte[0]))
                                .describedAs("%s", root)
                                .isEqualTo(StringData.fromString(""));
                        break;
                    case BINARY:
                    case VARBINARY:
                        assertThat(decoder.decode(new byte[0]))
                                .describedAs("%s", root)
                                .isEqualTo(new byte[0]);
                        break;
                    default:
                        // Every remaining decoder indexes into the array, which is what
                        // RowToRowDataConverter turns into the message naming the cell and its
                        // row.
                        assertThatThrownBy(() -> decoder.decode(new byte[0]), "%s", root)
                                .isInstanceOf(IndexOutOfBoundsException.class);
                }
            }
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
        void theThreeSwitchesAgreeOnEveryTypeRoot() {
            // checkSupported, encoder and decoder are parallel switches over LogicalTypeRoot with
            // nothing holding them in step: a root added to one alone is either unreachable or
            // reaches the IllegalStateException backstop at plan time, in a message written for
            // neither a user nor a maintainer. This walks every root and requires the three to
            // answer the same way.
            // The roots with no constructible sample above — structured or user-defined types no
            // switch names. Pinned as an allowlist rather than skipped silently, so a new
            // LogicalTypeRoot arriving in a Flink upgrade fails the walk until a sample — and so
            // an agreement check — exists for it. A subset, not an equality: the supported range
            // spans Flink majors and the newer roots cannot all be named from this shared source:
            // DESCRIPTOR and VARIANT are absent on 1.20, and BITMAP is absent on 1.20 and 2.2.
            java.util.Set<LogicalTypeRoot> unsampled =
                    java.util.EnumSet.complementOf(java.util.EnumSet.copyOf(SAMPLES.keySet()));
            assertThat(unsampled)
                    .extracting(Enum::name)
                    .isSubsetOf(
                            "DISTINCT_TYPE",
                            "STRUCTURED_TYPE",
                            "RAW",
                            "SYMBOL",
                            "UNRESOLVED",
                            "DESCRIPTOR",
                            "VARIANT",
                            "BITMAP");
            for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
                LogicalType type = SAMPLES.get(root);
                if (type == null) {
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
                boolean decodable = true;
                try {
                    CellValueCodec.decoder(type);
                } catch (IllegalStateException e) {
                    decodable = false;
                }
                assertThat(encodable).as("encoder for root %s", root).isEqualTo(checked);
                assertThat(decodable).as("decoder for root %s", root).isEqualTo(checked);
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

    /**
     * A codec is held by the schemas that are serialized into the job graph, so it is restored on a
     * task manager — possibly by a build other than the one that wrote it.
     *
     * <p>A lambda is restored by its {@code SerializedLambda} synthetic-method name, and the
     * lambdas sharing an enclosing declaration and a descriptor share one name hash, leaving a
     * trailing index as the only thing telling them apart. Both switches here are ordered the way
     * {@link LogicalTypeRoot} declares its constants, so supporting one more root inserts a case in
     * the middle and renumbers the rest: measured, a BIGINT encoder restored against such a build
     * <em>was</em> the INTEGER encoder and wrote four bytes instead of eight, with no error
     * anywhere. What crosses is therefore the declared type.
     */
    @Nested
    class SerializedForm {

        @Test
        void noCodecCarriesALambdaIntoTheJobGraph() throws Exception {
            for (Map.Entry<LogicalTypeRoot, LogicalType> sample : SAMPLES.entrySet()) {
                LogicalType type = sample.getValue();
                if (!isSupported(type)) {
                    continue;
                }
                for (Object codec :
                        Arrays.asList(
                                CellValueCodec.encoder(type),
                                CellValueCodec.decoder(type),
                                CellValueCodec.nullableEncoder(type.copy(true), NULL_STRING),
                                CellValueCodec.nullableDecoder(type.copy(true), NULL_STRING))) {
                    assertThat(
                                    new String(
                                            InstantiationUtil.serializeObject(codec),
                                            StandardCharsets.ISO_8859_1))
                            .as("serialized form for root %s", sample.getKey())
                            .doesNotContain("SerializedLambda");
                }
            }
        }

        @Test
        void aRestoredCodecStillReadsAndWritesItsOwnTypesLayout() throws Exception {
            // BIGINT and INT are the pair the failure mode confuses — both no-capture lambdas of
            // one method, so only the index tells them apart, and their widths differ.
            assertRoundTrip(DataTypes.BIGINT(), 4_294_967_297L, "0000000100000001");
            assertRoundTrip(DataTypes.INT(), 7, "00000007");
            // Two that capture their precision, so restoring them rebuilds more than a choice.
            assertRoundTrip(
                    DataTypes.DECIMAL(5, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("1.25"), 5, 2),
                    "000000027d");
            assertRoundTrip(
                    DataTypes.TIMESTAMP(3),
                    TimestampData.fromEpochMillis(1_000L),
                    "00000000000003e8");
        }

        @Test
        void aRestoredNullableCodecStillRecognisesANull() throws Exception {
            for (Map.Entry<LogicalTypeRoot, LogicalType> sample : SAMPLES.entrySet()) {
                LogicalType nullable = sample.getValue().copy(true);
                if (!isSupported(nullable)) {
                    continue;
                }
                CellValueCodec.FieldEncoder encoder =
                        clone(CellValueCodec.nullableEncoder(nullable, NULL_STRING));
                CellValueCodec.FieldDecoder decoder =
                        clone(CellValueCodec.nullableDecoder(nullable, NULL_STRING));

                byte[] encoded = encoder.encode(GenericRowData.of((Object) null), 0);

                assertThat(decoder.decode(encoded))
                        .as("null round trip for root %s", sample.getKey())
                        .isNull();
            }
        }

        private void assertRoundTrip(DataType type, Object value, String expectedHex)
                throws Exception {
            LogicalType logicalType = type.getLogicalType();
            CellValueCodec.FieldEncoder encoder = clone(CellValueCodec.encoder(logicalType));
            CellValueCodec.FieldDecoder decoder = clone(CellValueCodec.decoder(logicalType));

            byte[] encoded = encoder.encode(GenericRowData.of(value), 0);

            assertThat(hex(encoded)).as("restored encoder for %s", type).isEqualTo(expectedHex);
            assertThat(decoder.decode(fromHex(expectedHex)))
                    .as("restored decoder for %s", type)
                    .isEqualTo(value);
        }

        private <T> T clone(T codec) throws Exception {
            byte[] serialized = InstantiationUtil.serializeObject(codec);
            return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        }

        private boolean isSupported(LogicalType type) {
            try {
                CellValueCodec.checkSupported("cf.c", type);
                return true;
            } catch (ValidationException e) {
                return false;
            }
        }
    }
}
