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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.WireFormat;
import io.github.flink.gcp.connector.bigquery.sink.tables.PrimaryKeyColumn;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the primary-key value of a serialized CDC row from its bytes, so the default-stream writer
 * can send a batch before a row whose key the batch already holds (ADR-0192).
 *
 * <p>Two rows have the same key here only if BigQuery would see the same key, and two rows BigQuery
 * would see with the same key always have the same key here: a key this class cannot read that way
 * is reported as {@code null}, which the writer treats as matching every row. So every imprecision
 * costs requests, never order. A key value is compared as the wire bytes of its field, normalized
 * only where one value has several encodings that this class can recognize, which holds for the
 * encodings in the table below. Any other encoding of a key column makes every row of the table
 * unreadable:
 *
 * <ul>
 *   <li>{@code STRING} without a collation, from a {@code string} or {@code bytes} field;
 *   <li>{@code BYTES}, from a {@code bytes} field;
 *   <li>{@code BOOL}, from a {@code bool} field;
 *   <li>{@code INT64}, {@code DATE}, {@code DATETIME} and {@code TIMESTAMP}, from an integer field,
 *       which carries the value, the day number, the packed civil date-time, or the epoch
 *       microseconds;
 *   <li>{@code NUMERIC} and {@code BIGNUMERIC}, from a {@code bytes} field in the client library's
 *       little-endian two's-complement encoding, with redundant sign bytes removed, or from an
 *       integer field. Under a parameterized type, a value with more fractional digits than the
 *       column's scale is rounded by BigQuery and may meet a different value, so its row's key is
 *       unreadable.
 * </ul>
 *
 * <p>A string spelling of a number, date or time, a floating-point field, and a collated string
 * have several spellings of one value, or several values BigQuery compares as equal, that the bytes
 * cannot tell apart.
 *
 * <p>A key column the descriptor has no field for is read as absent in every row. A field holding
 * its type's default value is read as absent too, which merges {@code NULL} with the default of a
 * field without presence; both only make more rows match.
 */
@Internal
final class CdcRowKeys {

    /** The key column each field number reads, by index into {@code fields}. */
    private final Map<Integer, Integer> columnByFieldNumber;

    /** The field of each key column, or {@code null} when the descriptor has none. */
    private final List<Descriptors.FieldDescriptor> fields;

    /** Whether each key column is a decimal written as two's-complement bytes. */
    private final boolean[] decimalBytes;

    /**
     * For a decimal key column written as bytes whose type declares a scale, the power of ten the
     * encoded unscaled value must be a multiple of; {@code null} otherwise. A value that is not has
     * more fractional digits than the column keeps, so BigQuery rounds it and two different values
     * can meet in one key.
     */
    private final BigInteger[] decimalQuanta;

    /** Why the key cannot be read from any row, or {@code null} when it can. */
    @Nullable private final String unreadableReason;

    private CdcRowKeys(
            Map<Integer, Integer> columnByFieldNumber,
            List<Descriptors.FieldDescriptor> fields,
            boolean[] decimalBytes,
            BigInteger[] decimalQuanta,
            @Nullable String unreadableReason) {
        this.columnByFieldNumber = columnByFieldNumber;
        this.fields = fields;
        this.decimalBytes = decimalBytes;
        this.decimalQuanta = decimalQuanta;
        this.unreadableReason = unreadableReason;
    }

    /**
     * Creates the reader for a table's primary key over the descriptor its rows are written with.
     *
     * @param key the table's primary-key columns, in key order
     * @param descriptor the descriptor the rows are serialized with
     * @return the reader
     */
    static CdcRowKeys of(List<PrimaryKeyColumn> key, Descriptors.Descriptor descriptor) {
        Preconditions.checkNotNull(key, "key must not be null");
        Preconditions.checkNotNull(descriptor, "descriptor must not be null");
        Map<Integer, Integer> columnByFieldNumber = new HashMap<>();
        List<Descriptors.FieldDescriptor> fields = new ArrayList<>(key.size());
        boolean[] decimalBytes = new boolean[key.size()];
        BigInteger[] decimalQuanta = new BigInteger[key.size()];
        String unreadableReason = key.isEmpty() ? "the table declares no primary key" : null;
        for (PrimaryKeyColumn column : key) {
            Descriptors.FieldDescriptor field = fieldNamed(descriptor, column.getName());
            fields.add(field);
            if (field == null) {
                continue;
            }
            decimalBytes[fields.size() - 1] =
                    (column.getType() == StandardSQLTypeName.NUMERIC
                                    || column.getType() == StandardSQLTypeName.BIGNUMERIC)
                            && field.getType() == Descriptors.FieldDescriptor.Type.BYTES;
            if (decimalBytes[fields.size() - 1] && column.getScale() != null) {
                int encodedScale =
                        column.getType() == StandardSQLTypeName.NUMERIC
                                ? NUMERIC_ENCODED_SCALE
                                : BIGNUMERIC_ENCODED_SCALE;
                long dropped = encodedScale - Math.max(0L, column.getScale());
                if (dropped > 0) {
                    decimalQuanta[fields.size() - 1] = BigInteger.TEN.pow((int) dropped);
                }
            }
            if (unreadableReason == null && !readable(column, field)) {
                unreadableReason =
                        "primary-key column "
                                + column
                                + " is written from proto field "
                                + field.getName()
                                + " of type "
                                + (field.isRepeated() ? "repeated " : "")
                                + field.getType()
                                + ", whose bytes do not name one key value";
            }
            columnByFieldNumber.put(field.getNumber(), fields.size() - 1);
        }
        return new CdcRowKeys(
                columnByFieldNumber, fields, decimalBytes, decimalQuanta, unreadableReason);
    }

    /**
     * Returns why no row's key can be read, or {@code null} when it can.
     *
     * @return the reason, or {@code null}
     */
    @Nullable
    String unreadableReason() {
        return unreadableReason;
    }

    /**
     * Returns the row's key, or {@code null} when it cannot be read, which the caller treats as
     * matching every row.
     *
     * @param row the serialized row
     * @return the key, or {@code null}
     */
    @Nullable
    ByteString keyOf(ByteString row) {
        if (unreadableReason != null) {
            return null;
        }
        ByteString[] values = new ByteString[fields.size()];
        try {
            CodedInputStream input = row.newCodedInput();
            while (true) {
                int tag = input.readTag();
                if (tag == 0) {
                    break;
                }
                Integer column = columnByFieldNumber.get(WireFormat.getTagFieldNumber(tag));
                if (column == null) {
                    if (!input.skipField(tag)) {
                        break;
                    }
                    continue;
                }
                Descriptors.FieldDescriptor field = fields.get(column);
                if (WireFormat.getTagWireType(tag) != field.getLiteType().getWireType()) {
                    // A parser would read a packed or mistyped value here; nothing this class
                    // compares has one, so the row's key is not readable.
                    return null;
                }
                // The last occurrence of a non-repeated field is the one a parser keeps.
                values[column] =
                        readValue(input, field, decimalBytes[column], decimalQuanta[column]);
            }
        } catch (IOException | RuntimeException e) {
            return null;
        }
        if (values.length == 1) {
            // One column needs no framing; this is the per-row path of the common key shape.
            return values[0] == null ? ByteString.EMPTY : values[0];
        }
        // Length-prefixed values keep the columns apart, and an empty one is an absent value.
        int size = 0;
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                values[i] = ByteString.EMPTY;
            }
            size += CodedOutputStream.computeBytesSizeNoTag(values[i]);
        }
        byte[] key = new byte[size];
        CodedOutputStream out = CodedOutputStream.newInstance(key);
        try {
            for (ByteString value : values) {
                out.writeBytesNoTag(value);
            }
            out.checkNoSpaceLeft();
        } catch (IOException e) {
            // A stream over an array of exactly the computed size cannot overflow.
            throw new IllegalStateException(e);
        }
        return UnsafeByteOperations.unsafeWrap(key);
    }

    /**
     * Reads one value in a form that is equal for equal values; empty stands for the type's default
     * value.
     */
    private static ByteString readValue(
            CodedInputStream input,
            Descriptors.FieldDescriptor field,
            boolean decimalBytes,
            @Nullable BigInteger decimalQuantum)
            throws IOException {
        long number;
        switch (field.getType()) {
            case STRING:
                return input.readBytes();
            case BYTES:
                ByteString bytes = input.readBytes();
                if (!decimalBytes) {
                    return bytes;
                }
                ByteString minimal = minimalTwosComplement(bytes);
                if (decimalQuantum != null
                        && unscaledValue(minimal).mod(decimalQuantum).signum() != 0) {
                    // BigQuery rounds this value to the column's scale, so it may share a key
                    // with a different value; the key is not readable from the bytes.
                    throw new IllegalArgumentException("A decimal key finer than its column");
                }
                return minimal;
            case BOOL:
                number = input.readBool() ? 1 : 0;
                break;
            case INT32:
                number = input.readInt32();
                break;
            case UINT32:
                number = input.readUInt32() & 0xFFFFFFFFL;
                break;
            case SINT32:
                number = input.readSInt32();
                break;
            case FIXED32:
                number = input.readFixed32() & 0xFFFFFFFFL;
                break;
            case SFIXED32:
                number = input.readSFixed32();
                break;
            case INT64:
                number = input.readInt64();
                break;
            case UINT64:
                number = input.readUInt64();
                break;
            case SINT64:
                number = input.readSInt64();
                break;
            case FIXED64:
                number = input.readFixed64();
                break;
            case SFIXED64:
                number = input.readSFixed64();
                break;
            default:
                throw new IllegalStateException("Unreadable key field type " + field.getType());
        }
        return number == 0 ? ByteString.EMPTY : longBytes(number);
    }

    private static ByteString longBytes(long value) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (value >>> (8 * i));
        }
        return ByteString.copyFrom(bytes);
    }

    /** The scale the client library encodes a {@code NUMERIC} value at, in bytes. */
    private static final int NUMERIC_ENCODED_SCALE = 9;

    /** The scale the client library encodes a {@code BIGNUMERIC} value at, in bytes. */
    private static final int BIGNUMERIC_ENCODED_SCALE = 38;

    /** Reads a little-endian two's-complement integer; empty is zero. */
    private static BigInteger unscaledValue(ByteString littleEndian) {
        if (littleEndian.isEmpty()) {
            return BigInteger.ZERO;
        }
        byte[] bigEndian = new byte[littleEndian.size()];
        for (int i = 0; i < bigEndian.length; i++) {
            bigEndian[i] = littleEndian.byteAt(bigEndian.length - 1 - i);
        }
        return new BigInteger(bigEndian);
    }

    /**
     * Removes the redundant sign-extension bytes from the most significant end of a little-endian
     * two's-complement integer, so that one value has one encoding. Zero becomes empty.
     */
    static ByteString minimalTwosComplement(ByteString littleEndian) {
        int length = littleEndian.size();
        while (length > 1) {
            byte top = littleEndian.byteAt(length - 1);
            byte next = littleEndian.byteAt(length - 2);
            boolean redundant = (top == 0 && next >= 0) || (top == (byte) 0xFF && next < 0);
            if (!redundant) {
                break;
            }
            length--;
        }
        if (length == 1 && littleEndian.byteAt(0) == 0) {
            return ByteString.EMPTY;
        }
        return littleEndian.substring(0, length);
    }

    /**
     * Whether the bytes a field of this type is written as name one value of the column, and one
     * value has one encoding, after the normalization {@link #readValue} applies.
     */
    private static boolean readable(PrimaryKeyColumn column, Descriptors.FieldDescriptor field) {
        if (field.isRepeated()) {
            return false;
        }
        Descriptors.FieldDescriptor.Type type = field.getType();
        StandardSQLTypeName sqlType = column.getType();
        switch (sqlType) {
            case STRING:
                return column.getCollation() == null
                        && (type == Descriptors.FieldDescriptor.Type.STRING
                                || type == Descriptors.FieldDescriptor.Type.BYTES);
            case BYTES:
                return type == Descriptors.FieldDescriptor.Type.BYTES;
            case BOOL:
                return type == Descriptors.FieldDescriptor.Type.BOOL;
            case INT64:
            case DATE:
            case DATETIME:
            case TIMESTAMP:
                return isInteger(type);
            case NUMERIC:
            case BIGNUMERIC:
                return type == Descriptors.FieldDescriptor.Type.BYTES || isInteger(type);
            default:
                return false;
        }
    }

    private static boolean isInteger(Descriptors.FieldDescriptor.Type type) {
        switch (type) {
            case INT32:
            case UINT32:
            case SINT32:
            case FIXED32:
            case SFIXED32:
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                return true;
            default:
                return false;
        }
    }

    /**
     * Finds a field by column name, case-insensitively as BigQuery matches them. A column whose
     * name is not a valid proto field name is written under a placeholder field that carries the
     * name in its {@code column_name} annotation, which is what is compared then.
     */
    @Nullable
    private static Descriptors.FieldDescriptor fieldNamed(
            Descriptors.Descriptor descriptor, String name) {
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            if (BigQuerySchemaUtil.getFieldName(field).equalsIgnoreCase(name)) {
                return field;
            }
        }
        return null;
    }
}
