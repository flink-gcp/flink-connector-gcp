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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.conditional.AggregateValue;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

/** A resolved physical column or typed literal, retaining no codec lambda in the job graph. */
@Internal
final class ConditionalValueBinding implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;
    private final int position;
    @Nullable private final CellValueCodec.FieldEncoder encoder;
    @Nullable private final ByteString literalBytes;
    @Nullable private final Long literalInteger;
    private final LogicalTypeRoot type;

    private ConditionalValueBinding(
            String key,
            int position,
            @Nullable CellValueCodec.FieldEncoder encoder,
            @Nullable ByteString literalBytes,
            @Nullable Long literalInteger,
            LogicalTypeRoot type) {
        this.key = key;
        this.position = position;
        this.encoder = encoder;
        this.literalBytes = literalBytes;
        this.literalInteger = literalInteger;
        this.type = type;
    }

    static ConditionalValueBinding column(String key, String name, RowType rowType) {
        int position = rowType.getFieldNames().indexOf(name);
        if (position < 0) {
            throw new ValidationException(
                    "Option '" + key + "' references absent physical column '" + name + "'.");
        }
        LogicalType type = rowType.getTypeAt(position);
        try {
            CellValueCodec.checkSupported(name, type);
        } catch (ValidationException e) {
            throw new ValidationException(
                    "Option '" + key + "' has an unsupported column type: " + e.getMessage(), e);
        }
        return new ConditionalValueBinding(
                key, position, CellValueCodec.encoder(type), null, null, type.getTypeRoot());
    }

    static ConditionalValueBinding value(ConditionalSettings settings, RowType rowType) {
        ConditionalValueBinding selected = null;
        for (String kind : List.of("column", "utf8", "base64", "int64")) {
            String attribute = "value-" + kind;
            String text = settings.optional(attribute);
            if (text == null) {
                continue;
            }
            if (selected != null) {
                throw settings.error(attribute, "conflicts with another value binding");
            }
            String key = settings.key(attribute);
            if (kind.equals("column")) {
                selected = column(key, text, rowType);
            } else if (kind.equals("int64")) {
                selected = integer(key, ConditionalSettings.integer(key, text));
            } else {
                selected =
                        new ConditionalValueBinding(
                                key,
                                -1,
                                null,
                                kind.equals("utf8")
                                        ? ByteString.copyFromUtf8(text)
                                        : base64(key, text),
                                null,
                                LogicalTypeRoot.VARBINARY);
            }
        }
        if (selected == null) {
            throw settings.error("value-column", "requires exactly one value binding");
        }
        return selected;
    }

    @Nullable
    static ConditionalValueBinding time(
            ConditionalSettings settings,
            String stem,
            RowType rowType,
            boolean required,
            long minimum) {
        String column = settings.optional(stem + "-column");
        String literal = settings.optional(stem + "-micros");
        if (column != null && literal != null) {
            throw settings.error(stem + "-column", "conflicts with " + stem + "-micros");
        }
        ConditionalValueBinding binding;
        if (column != null) {
            binding = column(settings.key(stem + "-column"), column, rowType);
        } else if (literal != null) {
            String key = settings.key(stem + "-micros");
            binding = integer(key, ConditionalSettings.integer(key, literal));
        } else if (required) {
            throw settings.error(
                    stem + "-micros", "requires an explicit literal or column binding");
        } else {
            return null;
        }
        if (binding.type != LogicalTypeRoot.BIGINT) {
            throw settings.error(stem + "-column", "requires BIGINT");
        }
        if (binding.literalInteger != null && binding.literalInteger < minimum) {
            throw new ValidationException(
                    "Option '" + binding.key + "' must be at least " + minimum + ".");
        }
        return binding;
    }

    private static ConditionalValueBinding integer(String key, long value) {
        return new ConditionalValueBinding(key, -1, null, null, value, LogicalTypeRoot.BIGINT);
    }

    void requireAggregate() {
        if (type != LogicalTypeRoot.BIGINT && type != LogicalTypeRoot.VARBINARY) {
            throw new ValidationException(
                    "Option '" + key + "' requires BIGINT or BYTES aggregate input.");
        }
    }

    ByteString bytes(RowData input) {
        requirePresent(input);
        if (position >= 0) {
            try {
                return ByteString.copyFrom(encoder.encode(input, position));
            } catch (RuntimeException e) {
                throw new ValidationException(
                        "Option '" + key + "' could not encode its input column.", e);
            }
        }
        return literalBytes != null
                ? literalBytes
                : ByteString.copyFrom(ByteBuffer.allocate(8).putLong(literalInteger).array());
    }

    long number(RowData input, long minimum) {
        requirePresent(input);
        long number = position >= 0 ? input.getLong(position) : literalInteger;
        if (number < minimum) {
            throw new ValidationException("Option '" + key + "' must be at least " + minimum + ".");
        }
        return number;
    }

    AggregateValue aggregate(RowData input) {
        return type == LogicalTypeRoot.BIGINT
                ? AggregateValue.int64(number(input, Long.MIN_VALUE))
                : AggregateValue.bytes(bytes(input));
    }

    private void requirePresent(RowData input) {
        if (position >= 0 && input.isNullAt(position)) {
            throw new ValidationException("Option '" + key + "' references a NULL input column.");
        }
    }

    static void checkLiteralRange(
            @Nullable ConditionalValueBinding start, @Nullable ConditionalValueBinding end) {
        if (start != null
                && end != null
                && start.literalInteger != null
                && end.literalInteger != null
                && start.literalInteger >= end.literalInteger) {
            throw new ValidationException(
                    "Option '" + end.key + "' must exceed '" + start.key + "'.");
        }
    }

    static ByteString qualifier(ConditionalSettings settings) {
        String utf8 = settings.optional("qualifier");
        String base64 = settings.optional("qualifier-base64");
        if ((utf8 == null) == (base64 == null)) {
            throw settings.error(
                    base64 == null ? "qualifier" : "qualifier-base64",
                    "requires exactly one qualifier representation (qualifier or qualifier-base64)");
        }
        return utf8 != null
                ? ByteString.copyFromUtf8(utf8)
                : base64(settings.key("qualifier-base64"), base64);
    }

    private static ByteString base64(String key, String text) {
        try {
            byte[] bytes = Base64.getDecoder().decode(text);
            if (!Base64.getEncoder().encodeToString(bytes).equals(text)) {
                throw new IllegalArgumentException("Noncanonical Base64");
            }
            return ByteString.copyFrom(bytes);
        } catch (IllegalArgumentException e) {
            throw new ValidationException(
                    "Option '" + key + "' must be canonical padded Base64.", e);
        }
    }
}
