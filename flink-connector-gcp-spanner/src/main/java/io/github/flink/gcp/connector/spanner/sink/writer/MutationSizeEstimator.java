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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;

import java.util.List;

/**
 * Estimates how large a mutation is, for the writer's {@code maxBatchBytes} threshold.
 *
 * <p><b>An estimate, and it has to be.</b> The client library exposes no public route from a {@code
 * Mutation} to the bytes it becomes on the wire — every conversion on that path is package-private
 * (checked against google-cloud-spanner 6.119.0) — so this adds up the values through the public
 * accessors instead, the way Apache Beam's {@code MutationSizeEstimator} has for years. It ignores
 * protobuf framing, column names and the request envelope, so it reads low; the default threshold
 * sits 100 times under the 100 MiB a batch write request is documented to allow, and ten times
 * under the 10 MiB it can also be read as allowing (#441). Either way, that gap is the room the
 * estimate is allowed to be wrong in.
 *
 * <p>A value this estimator cannot size is counted at {@link #UNKNOWN_VALUE_BYTES} rather than
 * rejected — a type the client library adds later, and a {@code Value.untyped(...)}, which carries
 * no {@code Type} at all. Neither must stop a running job, and an under-count of one value is
 * absorbed by that same headroom.
 */
@Internal
final class MutationSizeEstimator {

    /** What a value of a type this estimator does not know is assumed to cost. */
    static final int UNKNOWN_VALUE_BYTES = 64;

    /** What one key part of a delete costs, whatever its type. */
    private static final int KEY_PART_BYTES = 16;

    /** What a delete of a key range costs beyond its two keys. */
    private static final int KEY_RANGE_OVERHEAD_BYTES = 16;

    private MutationSizeEstimator() {}

    /**
     * Estimates the mutation's size in bytes.
     *
     * @param mutation the mutation
     * @return the estimate, never negative
     */
    static long sizeOf(Mutation mutation) {
        if (mutation.getOperation() == Mutation.Op.DELETE) {
            return sizeOfKeySet(mutation);
        }
        long size = 0;
        for (Value value : mutation.getValues()) {
            size += sizeOf(value);
        }
        return size;
    }

    private static long sizeOfKeySet(Mutation mutation) {
        long size = 0;
        for (Key key : mutation.getKeySet().getKeys()) {
            size += (long) key.size() * KEY_PART_BYTES;
        }
        for (KeyRange range : mutation.getKeySet().getRanges()) {
            size +=
                    KEY_RANGE_OVERHEAD_BYTES
                            + (long) (range.getStart().size() + range.getEnd().size())
                                    * KEY_PART_BYTES;
        }
        return size;
    }

    private static long sizeOf(Value value) {
        if (value.isNull()) {
            return 0;
        }
        // Not every value has a type. Value.untyped(...) is public API — it lets the backend infer
        // the type from the statement — and it carries a null Type, so reading the code without
        // this guard would throw rather than fall through to the unknown-type arm below.
        if (value.getType() == null) {
            return UNKNOWN_VALUE_BYTES;
        }
        switch (value.getType().getCode()) {
            case BOOL:
                return 1;
            case INT64:
            case PG_OID:
            case FLOAT64:
            case ENUM:
                return 8;
            case FLOAT32:
                return 4;
            case DATE:
                return 12;
            case TIMESTAMP:
            case INTERVAL:
                return 16;
            case UUID:
                return 16;
            case NUMERIC:
                return value.getNumeric().toString().length();
            case PG_NUMERIC:
            case STRING:
                return value.getString().length();
            case JSON:
                return value.getJson().length();
            case PG_JSONB:
                return value.getPgJsonb().length();
            case BYTES:
            case PROTO:
                return value.getBytes().length();
            case ARRAY:
                return sizeOfArray(value);
            default:
                // STRUCT is not a column type in a mutation, and UNRECOGNIZED is what the client
                // library reports for a type newer than itself.
                return UNKNOWN_VALUE_BYTES;
        }
    }

    private static long sizeOfArray(Value value) {
        if (value.getType().getArrayElementType() == null) {
            return UNKNOWN_VALUE_BYTES;
        }
        switch (value.getType().getArrayElementType().getCode()) {
            case BOOL:
                return value.getBoolArray().size();
            case INT64:
            case PG_OID:
                return 8L * value.getInt64Array().size();
            case FLOAT64:
                return 8L * value.getFloat64Array().size();
            case FLOAT32:
                return 4L * value.getFloat32Array().size();
            case DATE:
                return 12L * value.getDateArray().size();
            case TIMESTAMP:
                return 16L * value.getTimestampArray().size();
            case INTERVAL:
                return 16L * value.getIntervalArray().size();
            case UUID:
                return 16L * value.getUuidArray().size();
            case NUMERIC:
                return value.getNumericArray().stream()
                        .mapToLong(element -> element == null ? 0 : element.toString().length())
                        .sum();
            case PG_NUMERIC:
            case STRING:
                return sizeOfStrings(value.getStringArray());
            case JSON:
                return sizeOfStrings(value.getJsonArray());
            case PG_JSONB:
                return sizeOfStrings(value.getPgJsonbArray());
            case BYTES:
            case PROTO:
                return value.getBytesArray().stream()
                        .mapToLong(element -> element == null ? 0 : element.length())
                        .sum();
            default:
                return UNKNOWN_VALUE_BYTES;
        }
    }

    private static long sizeOfStrings(List<String> elements) {
        return elements.stream().mapToLong(element -> element == null ? 0 : element.length()).sum();
    }
}
