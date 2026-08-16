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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Turns a {@code RowData} field into the bytes of a Bigtable cell, and back.
 *
 * <p><b>The encoding is the HBase ecosystem's, and that is normative here.</b> A Bigtable cell is
 * an uninterpreted byte string, so some convention has to be picked; this connector inherits users
 * from Bigtable-via-HBase, where the convention is {@code org.apache.hadoop.hbase.util.Bytes} as
 * Flink's own HBase connector applies it in {@code HBaseSerde}. Reproducing it — rather than
 * depending on {@code hbase-common}, which drags in Hadoop — is what lets a table written by either
 * connector be read by the other. The layouts below were read from {@code hbase-common} 2.6.6 and
 * {@code flink-connector-hbase-base} 4.0.0-1.19 on 2026-08-10; {@code CellValueCodecTest} pins each
 * one to an exact byte array, so a refactor cannot quietly break the interop the choice was made
 * for.
 *
 * <p>Two places where the {@code RowData} path and the older {@code Row}/{@code java.sql} path of
 * that connector disagree, and this follows the {@code RowData} one because that is what a Flink
 * SQL job writes today: {@code DATE} is a four-byte day count, not an eight-byte epoch-millis
 * value, and {@code TIME} is a four-byte millisecond-of-day.
 *
 * <p>A null is an empty cell for every type but a character string, where an empty cell is a
 * legitimate value — that column writes the {@code null-string-literal} instead.
 */
@Internal
public final class CellValueCodec {

    /** The precision bounds {@code TIME} and {@code TIMESTAMP} cells are stored within. */
    private static final int MIN_TIME_PRECISION = 0;

    private static final int MAX_TIME_PRECISION = 3;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private CellValueCodec() {}

    /** Reads one field of a row as the bytes of a cell. */
    @FunctionalInterface
    @Internal
    public interface FieldEncoder extends Serializable {

        /**
         * Encodes the field at {@code pos}.
         *
         * @param row the row to read
         * @param pos the field's position in it
         * @return the cell bytes
         */
        byte[] encode(RowData row, int pos);
    }

    /**
     * Reads the bytes of a cell as one field of a row.
     *
     * <p>The mirror of {@link FieldEncoder}, returning Flink's internal data structures ({@code
     * StringData}, {@code DecimalData}, {@code TimestampData}, boxed primitives) so the result can
     * be placed into a {@code GenericRowData} field directly.
     */
    @FunctionalInterface
    @Internal
    public interface FieldDecoder extends Serializable {

        /**
         * Decodes the bytes of a cell.
         *
         * @param value the cell bytes
         * @return the field value, in Flink's internal data format
         */
        @Nullable
        Object decode(byte[] value);
    }

    /**
     * Rejects a column whose type has no cell encoding, naming the column.
     *
     * <p>Called while the DDL is parsed rather than left to {@link #encoder(LogicalType)}, so that
     * an unusable column is reported when the sink is built rather than when a row reaches it. That
     * is when the table is first written to, not when it is created: Flink does not consult a
     * connector while registering a table.
     *
     * @param column the column's name, for the message
     * @param type the declared type
     * @throws ValidationException if the type cannot be stored in a cell
     */
    public static void checkSupported(String column, LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
                return;
            case TIME_WITHOUT_TIME_ZONE:
                checkPrecision(column, type, ((TimeType) type).getPrecision());
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                checkPrecision(column, type, ((TimestampType) type).getPrecision());
                return;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                checkPrecision(column, type, ((LocalZonedTimestampType) type).getPrecision());
                return;
            default:
                throw new ValidationException(
                        String.format(
                                "Column '%s' has type %s, which has no Bigtable cell encoding.",
                                column, type));
        }
    }

    private static void checkPrecision(String column, LogicalType type, int precision) {
        if (precision < MIN_TIME_PRECISION || precision > MAX_TIME_PRECISION) {
            // The cell holds milliseconds, so a finer precision would be silently truncated on the
            // way out and could never be read back. The bound is the HBase connector's too.
            throw new ValidationException(
                    String.format(
                            "Column '%s' has type %s, whose precision is outside the range [%d,"
                                    + " %d] a Bigtable cell can hold: the cell stores"
                                    + " milliseconds.",
                            column, type, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
    }

    /**
     * Returns an encoder that writes a null as an empty cell, or as {@code nullStringBytes} for a
     * character string.
     *
     * @param type the declared type
     * @param nullStringBytes the {@code null-string-literal}, UTF-8 encoded
     * @return the encoder
     */
    public static FieldEncoder nullableEncoder(LogicalType type, byte[] nullStringBytes) {
        FieldEncoder encoder = encoder(type);
        if (!type.isNullable()) {
            return encoder;
        }
        if (type.is(LogicalTypeFamily.CHARACTER_STRING)) {
            byte[] nullBytes = nullStringBytes.clone();
            return (row, pos) -> row.isNullAt(pos) ? nullBytes : encoder.encode(row, pos);
        }
        return (row, pos) -> row.isNullAt(pos) ? EMPTY_BYTES : encoder.encode(row, pos);
    }

    /**
     * Returns an encoder for a field that is known to be present.
     *
     * @param type the declared type
     * @return the encoder
     */
    public static FieldEncoder encoder(LogicalType type) {
        // Ordered as LogicalTypeRoot declares them, which is how HBaseSerde orders its own switch.
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (row, pos) -> row.getString(pos).toBytes();
            case BOOLEAN:
                return (row, pos) -> toBytes(row.getBoolean(pos));
            case BINARY:
            case VARBINARY:
                return RowData::getBinary;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return (row, pos) -> toBytes(row.getDecimal(pos, precision, scale).toBigDecimal());
            case TINYINT:
                // Written out rather than routed through an overload: a byte widens to short, so
                // toBytes(row.getByte(pos)) would silently produce the two-byte encoding. HBase
                // has no toBytes(byte) for the same reason.
                return (row, pos) -> new byte[] {row.getByte(pos)};
            case SMALLINT:
                return (row, pos) -> toBytes(row.getShort(pos));
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case TIME_WITHOUT_TIME_ZONE:
                return (row, pos) -> toBytes(row.getInt(pos));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (row, pos) -> toBytes(row.getLong(pos));
            case FLOAT:
                return (row, pos) -> toBytes(Float.floatToRawIntBits(row.getFloat(pos)));
            case DOUBLE:
                return (row, pos) -> toBytes(Double.doubleToRawLongBits(row.getDouble(pos)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (row, pos) ->
                        toBytes(row.getTimestamp(pos, timestampPrecision).getMillisecond());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int instantPrecision = ((LocalZonedTimestampType) type).getPrecision();
                return (row, pos) ->
                        toBytes(row.getTimestamp(pos, instantPrecision).getMillisecond());
            default:
                // Unreachable through the DDL, which is checked by checkSupported above. Kept as
                // the invariant's backstop rather than as a second user-facing message.
                throw new IllegalStateException("No cell encoding for type " + type);
        }
    }

    /**
     * Returns a decoder that reads a null the way {@link #nullableEncoder(LogicalType, byte[])}
     * wrote it: an empty cell for every type but a character string, where it is the {@code
     * null-string-literal} — an empty string cell is a value in its own right and decodes as one.
     *
     * @param type the declared type
     * @param nullStringBytes the {@code null-string-literal}, UTF-8 encoded
     * @return the decoder
     */
    public static FieldDecoder nullableDecoder(LogicalType type, byte[] nullStringBytes) {
        FieldDecoder decoder = decoder(type);
        if (!type.isNullable()) {
            return decoder;
        }
        if (type.is(LogicalTypeFamily.CHARACTER_STRING)) {
            byte[] nullBytes = nullStringBytes.clone();
            return value -> Arrays.equals(value, nullBytes) ? null : decoder.decode(value);
        }
        return value -> value.length == 0 ? null : decoder.decode(value);
    }

    /**
     * Returns a decoder for a cell that is known to hold a value.
     *
     * @param type the declared type
     * @return the decoder
     */
    public static FieldDecoder decoder(LogicalType type) {
        // Ordered as the encoder switch above, whose layouts these reverse.
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData::fromBytes;
            case BOOLEAN:
                // Any nonzero byte reads as true, which is Bytes.toBoolean's rule — not an
                // equality test against the 0xFF the encoder writes.
                return value -> value[0] != 0;
            case BINARY:
            case VARBINARY:
                return value -> value;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return value -> DecimalData.fromBigDecimal(toBigDecimal(value), precision, scale);
            case TINYINT:
                return value -> value[0];
            case SMALLINT:
                return CellValueCodec::toShort;
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case TIME_WITHOUT_TIME_ZONE:
                return CellValueCodec::toInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return CellValueCodec::toLong;
            case FLOAT:
                return value -> Float.intBitsToFloat(toInt(value));
            case DOUBLE:
                return value -> Double.longBitsToDouble(toLong(value));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return value -> TimestampData.fromEpochMillis(toLong(value));
            default:
                // Unreachable through the DDL, which is checked by checkSupported above. Kept as
                // the invariant's backstop rather than as a second user-facing message.
                throw new IllegalStateException("No cell decoding for type " + type);
        }
    }

    // ------------------------------------------------------------------------
    //  The HBase byte layouts, reproduced
    // ------------------------------------------------------------------------

    /** {@code Bytes.toBytes(boolean)}: one byte, {@code 0xFF} for true. */
    private static byte[] toBytes(boolean value) {
        return new byte[] {value ? (byte) -1 : (byte) 0};
    }

    /** {@code Bytes.toBytes(short)}: two bytes, big-endian two's complement. */
    private static byte[] toBytes(short value) {
        return new byte[] {(byte) (value >> 8), (byte) value};
    }

    /** {@code Bytes.toBytes(int)}: four bytes, big-endian two's complement. */
    private static byte[] toBytes(int value) {
        return new byte[] {
            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
        };
    }

    /** {@code Bytes.toBytes(long)}: eight bytes, big-endian two's complement. */
    private static byte[] toBytes(long value) {
        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) value;
            value >>>= 8;
        }
        return bytes;
    }

    /**
     * {@code Bytes.toBytes(BigDecimal)}: the scale as a four-byte big-endian int, then the unscaled
     * value as {@code BigInteger.toByteArray()} — big-endian two's complement, shortest form.
     */
    private static byte[] toBytes(BigDecimal value) {
        byte[] unscaled = value.unscaledValue().toByteArray();
        byte[] bytes = new byte[4 + unscaled.length];
        int scale = value.scale();
        bytes[0] = (byte) (scale >> 24);
        bytes[1] = (byte) (scale >> 16);
        bytes[2] = (byte) (scale >> 8);
        bytes[3] = (byte) scale;
        System.arraycopy(unscaled, 0, bytes, 4, unscaled.length);
        return bytes;
    }

    /** {@code Bytes.toShort(byte[])}: two bytes, big-endian two's complement. */
    private static short toShort(byte[] bytes) {
        return (short) ((bytes[0] << 8) | (bytes[1] & 0xff));
    }

    /** {@code Bytes.toInt(byte[])}: four bytes, big-endian two's complement. */
    private static int toInt(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (bytes[i] & 0xff);
        }
        return value;
    }

    /** {@code Bytes.toLong(byte[])}: eight bytes, big-endian two's complement. */
    private static long toLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[i] & 0xff);
        }
        return value;
    }

    /**
     * {@code Bytes.toBigDecimal(byte[])}: the stored scale is the cell's, not the column's — the
     * decimal decoder rescales to the declared type afterwards.
     */
    private static BigDecimal toBigDecimal(byte[] bytes) {
        int scale = toInt(bytes);
        BigInteger unscaled = new BigInteger(Arrays.copyOfRange(bytes, 4, bytes.length));
        return new BigDecimal(unscaled, scale);
    }
}
