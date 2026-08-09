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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.util.Objects;

/**
 * One row key the service offered as a section boundary, with the approximate number of bytes
 * stored before it.
 *
 * <p>This is the connector's own form of the client library's {@code KeyOffset}. The vendor type
 * carries exactly the same two values, but its only factory is annotated {@code @InternalApi},
 * which would put an internal call into every test that scripts a sampler. Converting once, at the
 * seam that talks to the SDK, keeps that call in one production class and gives the offset
 * arithmetic a home.
 *
 * <p>The empty key is admissible here and means what the service says it means — "end of table",
 * rather than a boundary. Dropping it is the planner's job, so that the interpretation of a sample
 * list lives in one place.
 */
@Internal
public final class RowKeySample {

    private final ByteString key;
    private final long offsetBytes;

    private RowKeySample(ByteString key, long offsetBytes) {
        this.key = Preconditions.checkNotNull(key, "key must not be null");
        this.offsetBytes = offsetBytes;
    }

    /**
     * Creates a sample.
     *
     * @param key the sampled row key
     * @param offsetBytes the approximate total size of the rows preceding it
     * @return the sample
     */
    public static RowKeySample of(ByteString key, long offsetBytes) {
        return new RowKeySample(key, offsetBytes);
    }

    /** Returns the sampled row key; empty means "end of table" rather than a boundary. */
    public ByteString getKey() {
        return key;
    }

    /** Returns the approximate total size, in bytes, of the rows that precede {@link #getKey()}. */
    public long getOffsetBytes() {
        return offsetBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowKeySample)) {
            return false;
        }
        RowKeySample other = (RowKeySample) o;
        return offsetBytes == other.offsetBytes && key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, offsetBytes);
    }

    @Override
    public String toString() {
        return "RowKeySample{key=" + key.size() + " bytes, offsetBytes=" + offsetBytes + '}';
    }
}
