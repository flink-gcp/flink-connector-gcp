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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import org.apache.flink.annotation.Internal;

import com.google.bigtable.admin.v2.Type;
import com.google.protobuf.TextFormat;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;

import java.util.Map;

/** Converts creation types and compares the writable portion of live family types. */
@Internal
final class ColumnFamilyTypes {
    private ColumnFamilyTypes() {}

    static com.google.cloud.bigtable.admin.v2.models.Type toClient(ColumnFamilyType type) {
        switch (type) {
            case RAW:
                return com.google.cloud.bigtable.admin.v2.models.Type.raw();
            case INT64_SUM:
                return com.google.cloud.bigtable.admin.v2.models.Type.int64Sum();
            case INT64_MIN:
                return com.google.cloud.bigtable.admin.v2.models.Type.int64Min();
            case INT64_MAX:
                return com.google.cloud.bigtable.admin.v2.models.Type.int64Max();
            case INT64_HLL:
                return com.google.cloud.bigtable.admin.v2.models.Type.int64Hll();
            default:
                throw new IllegalArgumentException("Unsupported column family type: " + type);
        }
    }

    static void check(
            TableDestination destination,
            Map<String, ColumnFamilyType> expected,
            Map<String, Type> actual,
            boolean allowMissing) {
        expected.forEach(
                (family, type) -> {
                    Type live = actual.get(family);
                    if (live == null && allowMissing) {
                        return;
                    }
                    if (live == null
                            || !normalize(toClient(type).toProto()).equals(normalize(live))) {
                        throw new Mismatch(
                                "Bigtable table "
                                        + destination
                                        + " column family '"
                                        + family
                                        + "' has value type "
                                        + (live == null ? "<missing>" : describe(live))
                                        + "; expected "
                                        + type
                                        + ". Existing families are never converted.");
                    }
                });
    }

    @SuppressWarnings("deprecation")
    private static Type normalize(Type type) {
        if (!type.hasAggregateType()) {
            return type;
        }
        Type.Aggregate.Builder aggregate = type.getAggregateType().toBuilder().clearStateType();
        Type input = aggregate.getInputType();
        if (input.hasInt64Type()) {
            Type.Int64.Encoding.Builder encoding = input.getInt64Type().getEncoding().toBuilder();
            if (encoding.getEncodingCase() == Type.Int64.Encoding.EncodingCase.ENCODING_NOT_SET) {
                encoding.mergeFrom(
                        toClient(ColumnFamilyType.INT64_SUM)
                                .toProto()
                                .getAggregateType()
                                .getInputType()
                                .getInt64Type()
                                .getEncoding());
            }
            if (encoding.hasBigEndianBytes()) {
                // The pinned admin schema marks this field deprecated and ignored if set.
                // Preserve unknown fields: they may describe an input contract we cannot write.
                encoding.setBigEndianBytes(
                        encoding.getBigEndianBytes().toBuilder().clearBytesType());
            }
            aggregate.setInputType(
                    input.toBuilder()
                            .setInt64Type(input.getInt64Type().toBuilder().setEncoding(encoding)));
        }
        return type.toBuilder().setAggregateType(aggregate).build();
    }

    private static String describe(Type type) {
        for (ColumnFamilyType candidate : ColumnFamilyType.values()) {
            if (normalize(toClient(candidate).toProto()).equals(normalize(type))) {
                return candidate.toString();
            }
        }
        return TextFormat.shortDebugString(type);
    }

    /** A configuration failure that cannot be repaired by retrying an admin operation. */
    static final class Mismatch extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Mismatch(String message) {
            super(message);
        }
    }
}
