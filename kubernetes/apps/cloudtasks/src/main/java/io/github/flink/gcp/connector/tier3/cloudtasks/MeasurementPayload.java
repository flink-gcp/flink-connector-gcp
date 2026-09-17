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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

/** Fixed-size synthetic HTTP body carrying a process-local observation origin. */
@Internal
final class MeasurementPayload {
    private static final int MAGIC = 0x43543132;
    private static final int HEADER_BYTES = 44;
    private static final UUID PROCESS = UUID.randomUUID();

    private MeasurementPayload() {}

    static ByteString create(int size, long sequence) {
        return create(size, sequence, PROCESS, System.nanoTime(), System.currentTimeMillis());
    }

    static ByteString create(int size, long sequence, UUID process, long nanos, long millis) {
        if (size != 1024 && size != 65536) {
            throw new IllegalArgumentException("HTTP body must be 1024 or 65536 bytes");
        }
        byte[] bytes = new byte[size];
        new Random(sequence).nextBytes(bytes);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        data.putInt(MAGIC).putLong(sequence);
        data.putLong(process.getMostSignificantBits()).putLong(process.getLeastSignificantBits());
        data.putLong(nanos).putLong(millis);
        return ByteString.copyFrom(data.array());
    }

    static Origin read(ByteString body) {
        if (body.size() != 1024 && body.size() != 65536) {
            throw new IllegalArgumentException("Unexpected measurement body size");
        }
        ByteBuffer data = body.asReadOnlyByteBuffer();
        if (data.remaining() < HEADER_BYTES || data.getInt() != MAGIC) {
            throw new IllegalArgumentException("Unexpected measurement body header");
        }
        return new Origin(
                data.getLong(),
                new UUID(data.getLong(), data.getLong()),
                data.getLong(),
                data.getLong());
    }

    static UUID process() {
        return PROCESS;
    }

    record Origin(long sequence, UUID process, long nanos, long millis) {
        long elapsedNanos(UUID currentProcess, long now) {
            // nanoTime is meaningful only within one JVM incarnation. Recovery observations
            // keep the wall-clock origin but never fabricate a cross-process latency sample.
            return process.equals(currentProcess) ? now - nanos : -1;
        }
    }
}
