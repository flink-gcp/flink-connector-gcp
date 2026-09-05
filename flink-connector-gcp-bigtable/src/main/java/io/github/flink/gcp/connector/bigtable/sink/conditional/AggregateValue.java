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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.Serializable;

/** Immutable aggregate input or state: raw bytes, typed bytes or a native int64. */
@PublicEvolving
public final class AggregateValue implements Serializable {
    private static final long serialVersionUID = 1L;

    enum Kind {
        RAW,
        BYTES,
        INT64
    }

    final Kind kind;
    final ByteString bytes;
    final long integer;

    private AggregateValue(Kind kind, ByteString bytes, long integer) {
        this.kind = kind;
        this.bytes = bytes;
        this.integer = integer;
    }

    /**
     * Creates a raw aggregate input or state. The family schema determines its encoding.
     *
     * @param bytes the non-null bytes
     * @return the value
     */
    public static AggregateValue raw(ByteString bytes) {
        return new AggregateValue(
                Kind.RAW, Preconditions.checkNotNull(bytes, "bytes must not be null"), 0);
    }

    /**
     * Creates typed bytes, transported as {@code bytes_value}. Use this for a serialized
     * accumulator whose family state type requires bytes, such as an Int64 Sum cell read from
     * Bigtable. This is distinct from {@link #raw(ByteString)}; the service checks compatibility
     * with the family's input or state type.
     *
     * @param bytes the non-null encoded input or state
     * @return the value
     */
    public static AggregateValue bytes(ByteString bytes) {
        return new AggregateValue(
                Kind.BYTES, Preconditions.checkNotNull(bytes, "bytes must not be null"), 0);
    }

    /**
     * Creates a native signed int64 aggregate input or state.
     *
     * @param value the integer
     * @return the value
     */
    public static AggregateValue int64(long value) {
        return new AggregateValue(Kind.INT64, null, value);
    }
}
