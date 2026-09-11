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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.bigtable.v2.CheckAndMutateRowRequest;

import java.io.IOException;

/** Versioned checkpoint format; restored bytes must satisfy the full marker contract. */
@Internal
public final class BigtableCommittableSerializer
        implements SimpleVersionedSerializer<BigtableCommittable> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BigtableCommittable value) {
        return value.getRequest().toByteArray();
    }

    @Override
    public BigtableCommittable deserialize(int version, byte[] bytes) throws IOException {
        if (version != 1) {
            throw new IOException("Unsupported Bigtable committable version: " + version);
        }
        return new BigtableCommittable(CheckAndMutateRowRequest.parseFrom(bytes));
    }
}
