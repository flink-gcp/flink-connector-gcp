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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A projected index value read back as the client library's projection entity reads it. */
class ProjectedValuesTest {

    private static com.google.datastore.v1.Entity entity(String name, Value value) {
        return com.google.datastore.v1.Entity.newBuilder()
                .setKey(
                        Key.newBuilder()
                                .addPath(Key.PathElement.newBuilder().setKind("K").setName("a")))
                .putProperties(name, value)
                .build();
    }

    @Test
    void readsAProjectedTimestampFromItsMicroseconds() {
        // As the emulator returned a projected timestamp, measured 2026-10-03.
        Entity read =
                ProjectedValues.toEntity(
                        entity(
                                "ts",
                                Value.newBuilder()
                                        .setIntegerValue(1_700_000_000_123_456L)
                                        .setMeaning(ProjectedValues.INDEX_VALUE)
                                        .build()));

        assertThat(read.getTimestamp("ts"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000));
    }

    @Test
    void readsAProjectedTimestampBeforeTheEpoch() {
        Entity read =
                ProjectedValues.toEntity(
                        entity(
                                "ts",
                                Value.newBuilder()
                                        .setIntegerValue(-1L)
                                        .setMeaning(ProjectedValues.INDEX_VALUE)
                                        .build()));

        assertThat(read.getTimestamp("ts")).isEqualTo(Timestamp.ofTimeMicroseconds(-1L));
    }

    @Test
    void readsAProjectedBlobFromItsString() {
        Entity read =
                ProjectedValues.toEntity(
                        entity(
                                "b",
                                Value.newBuilder()
                                        .setStringValue("\u0001\u0002")
                                        .setMeaning(ProjectedValues.INDEX_VALUE)
                                        .build()));

        assertThat(read.getBlob("b")).isEqualTo(Blob.copyFrom(new byte[] {1, 2}));
    }

    @Test
    void leavesEveryOtherValueAsTheServiceReturnedIt() {
        Value plainLong = Value.newBuilder().setIntegerValue(7L).build();
        Value indexDouble =
                Value.newBuilder()
                        .setDoubleValue(1.5)
                        .setMeaning(ProjectedValues.INDEX_VALUE)
                        .build();
        com.google.datastore.v1.Entity both =
                entity("n", plainLong).toBuilder().putProperties("d", indexDouble).build();

        Entity read = ProjectedValues.toEntity(both);

        assertThat(read.getLong("n")).isEqualTo(7L);
        assertThat(read.getDouble("d")).isEqualTo(1.5);
    }
}
