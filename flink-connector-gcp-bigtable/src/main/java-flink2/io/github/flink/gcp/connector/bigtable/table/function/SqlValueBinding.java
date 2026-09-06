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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.conditional.AggregateValue;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

/** An eagerly typed operand, read from a row or retained as a literal. */
@Internal
final class SqlValueBinding implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;
    private final int position;
    private final CellValueCodec.FieldEncoder encoder;
    private final ByteString bytes;
    private final Long integer;
    private final LogicalTypeRoot type;

    private SqlValueBinding(
            String key,
            int position,
            CellValueCodec.FieldEncoder encoder,
            ByteString bytes,
            Long integer,
            LogicalTypeRoot type) {
        this.key = key;
        this.position = position;
        this.encoder = encoder;
        this.bytes = bytes;
        this.integer = integer;
        this.type = type;
    }

    static SqlValueBinding argument(String key, String index, List<DataType> arguments) {
        int argument = SqlSettingMap.index(key, index);
        if (argument >= arguments.size() - 2) {
            throw new ValidationException(
                    "Option '" + key + "' references an absent value argument.");
        }
        DataType dataType = arguments.get(argument + 2);
        CellValueCodec.checkSupported(key, dataType.getLogicalType());
        return new SqlValueBinding(
                key,
                argument + 1,
                CellValueCodec.encoder(dataType.getLogicalType()),
                null,
                null,
                dataType.getLogicalType().getTypeRoot());
    }

    static SqlValueBinding value(SqlSettingMap settings, List<DataType> arguments) {
        SqlValueBinding selected = null;
        for (String kind : List.of("argument", "utf8", "base64", "int64")) {
            String property = "value-" + kind;
            String value = settings.optional(property);
            if (value == null) {
                continue;
            }
            if (selected != null) {
                throw settings.error(property, "conflicts with another value binding");
            }
            String key = settings.key(property);
            if (kind.equals("argument")) {
                selected = argument(key, value, arguments);
            } else if (kind.equals("int64")) {
                selected = number(key, SqlSettingMap.integer(key, value));
            } else {
                selected =
                        new SqlValueBinding(
                                key,
                                -1,
                                null,
                                kind.equals("utf8")
                                        ? ByteString.copyFromUtf8(value)
                                        : base64(key, value),
                                null,
                                LogicalTypeRoot.VARBINARY);
            }
        }
        if (selected == null) {
            throw settings.error("value-argument", "requires exactly one value binding");
        }
        return selected;
    }

    static SqlValueBinding time(
            SqlSettingMap settings, String stem, List<DataType> arguments, boolean required) {
        String argument = settings.optional(stem + "-argument");
        String literal = settings.optional(stem + "-micros");
        if (argument != null && literal != null) {
            throw settings.error(stem + "-argument", "conflicts with " + stem + "-micros");
        }
        SqlValueBinding binding;
        if (argument != null) {
            binding = argument(settings.key(stem + "-argument"), argument, arguments);
            binding.requireInteger();
        } else if (literal != null) {
            binding =
                    number(
                            settings.key(stem + "-micros"),
                            SqlSettingMap.integer(settings.key(stem + "-micros"), literal));
        } else if (required) {
            throw settings.error(stem + "-micros", "requires a literal or argument binding");
        } else {
            return null;
        }
        return binding;
    }

    private static SqlValueBinding number(String key, long value) {
        return new SqlValueBinding(key, -1, null, null, value, LogicalTypeRoot.BIGINT);
    }

    void requireInteger() {
        if (type != LogicalTypeRoot.BIGINT) {
            throw new ValidationException("Option '" + key + "' requires BIGINT.");
        }
    }

    void requireAppend() {
        if (position >= 0
                && type != LogicalTypeRoot.CHAR
                && type != LogicalTypeRoot.VARCHAR
                && type != LogicalTypeRoot.BINARY
                && type != LogicalTypeRoot.VARBINARY) {
            throw new ValidationException(
                    "Option '" + key + "' requires a character or binary value.");
        }
        if (integer != null) {
            throw new ValidationException(
                    "Option '" + key + "' requires a character or binary value.");
        }
        if (bytes != null && bytes.isEmpty()) {
            throw new ValidationException("Option '" + key + "' append value must not be empty.");
        }
    }

    void requireAggregate() {
        if (position >= 0
                && type != LogicalTypeRoot.BIGINT
                && type != LogicalTypeRoot.BINARY
                && type != LogicalTypeRoot.VARBINARY) {
            throw new ValidationException(
                    "Option '" + key + "' requires BIGINT or binary aggregate input.");
        }
    }

    void requireMinimum(long minimum) {
        requireInteger();
        if (integer != null && integer < minimum) {
            throw new ValidationException("Option '" + key + "' must be at least " + minimum + ".");
        }
    }

    ByteString bytes(RowData input) {
        requirePresent(input);
        if (position >= 0) {
            return ByteString.copyFrom(encoder.encode(input, position));
        }
        return bytes != null
                ? bytes
                : ByteString.copyFrom(ByteBuffer.allocate(8).putLong(integer).array());
    }

    ByteString appendBytes(RowData input) {
        ByteString append = bytes(input);
        if (append.isEmpty()) {
            throw new ValidationException("Option '" + key + "' append value must not be empty.");
        }
        return append;
    }

    long number(RowData input) {
        requirePresent(input);
        return position >= 0 ? input.getLong(position) : integer;
    }

    long numberAtLeast(RowData input, long minimum) {
        long value = number(input);
        if (value < minimum) {
            throw new ValidationException("Option '" + key + "' must be at least " + minimum + ".");
        }
        return value;
    }

    static void checkRange(SqlValueBinding start, SqlValueBinding end) {
        if (start != null
                && end != null
                && start.integer != null
                && end.integer != null
                && start.integer >= end.integer) {
            throw new ValidationException(
                    "Option '" + end.key + "' must exceed " + start.key + ".");
        }
    }

    AggregateValue aggregate(RowData input) {
        return type == LogicalTypeRoot.BIGINT
                ? AggregateValue.int64(number(input))
                : AggregateValue.bytes(bytes(input));
    }

    private void requirePresent(RowData input) {
        if (position >= 0 && input.isNullAt(position)) {
            throw new ValidationException("Option '" + key + "' references a NULL argument.");
        }
    }

    static ByteString qualifier(SqlSettingMap settings) {
        String utf8 = settings.optional("qualifier");
        String encoded = settings.optional("qualifier-base64");
        if ((utf8 == null) == (encoded == null)) {
            throw settings.error(
                    encoded == null ? "qualifier" : "qualifier-base64",
                    "requires exactly one qualifier representation (qualifier or qualifier-base64)");
        }
        return utf8 != null
                ? ByteString.copyFromUtf8(utf8)
                : base64(settings.key("qualifier-base64"), encoded);
    }

    static ByteString base64(String key, String value) {
        try {
            byte[] decoded = Base64.getDecoder().decode(value);
            if (!Base64.getEncoder().encodeToString(decoded).equals(value)) {
                throw new IllegalArgumentException();
            }
            return ByteString.copyFrom(decoded);
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Option '" + key + "' must be canonical padded Base64.");
        }
    }
}
