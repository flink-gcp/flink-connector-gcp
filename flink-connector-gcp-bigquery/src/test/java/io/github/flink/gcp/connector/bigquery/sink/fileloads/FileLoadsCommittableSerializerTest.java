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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsCommittableSerializer}. */
class FileLoadsCommittableSerializerTest {

    private final FileLoadsCommittableSerializer serializer = new FileLoadsCommittableSerializer();

    @Test
    void roundTrips() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                        "0123456789abcdef0123456789abcdef",
                        TableDestination.of("my-project", "my_dataset", "my_table"),
                        "gs://bucket/prefix/jobid/my-project.my_dataset.my_table/0-0-abc-0.avro",
                        123_456L,
                        789L,
                        StagingFormat.AVRO);

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getCheckpointId()).isNull();
    }

    @Test
    void roundTripsTheCheckpointId() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                                "0123456789abcdef0123456789abcdef",
                                TableDestination.of("p", "d", "t"),
                                "gs://bucket/o.avro",
                                1L,
                                1L,
                                StagingFormat.AVRO)
                        .withCheckpointId(42L);

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getCheckpointId()).isEqualTo(42L);
    }

    @Test
    void roundTripsTheParquetFormat() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                        "0123456789abcdef0123456789abcdef",
                        TableDestination.of("p", "d", "t"),
                        "gs://bucket/o.parquet",
                        1L,
                        1L,
                        StagingFormat.PARQUET);

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getFormat()).isEqualTo(StagingFormat.PARQUET);
    }

    @Test
    void readsAVersionTwoCommittableAsAvro() throws IOException {
        // Version 2 is what main has produced since #69, so committables in that layout are in
        // committer state across the
        // upgrade that adds the format — and every one of them is Avro, because that was the only
        // format the writer produced. Rejecting them would fail a restart for no reason, which is
        // why version 2 is migrated where version 1 (which never survived a job) is not.
        byte[] versionTwo = versionTwoBytes("gs://bucket/o.avro", 7L, 3L, 42L);

        FileLoadsCommittable restored = serializer.deserialize(2, versionTwo);

        assertThat(restored.getFormat()).isEqualTo(StagingFormat.AVRO);
        assertThat(restored.getUri()).isEqualTo("gs://bucket/o.avro");
        assertThat(restored.getByteCount()).isEqualTo(7L);
        assertThat(restored.getRowCount()).isEqualTo(3L);
        assertThat(restored.getCheckpointId()).isEqualTo(42L);
    }

    /**
     * Writes the version-2 layout by hand. The old serializer is gone, so the bytes a running job
     * would hand back are reconstructed here rather than produced — which is also what makes this
     * fail if the version-3 reader starts consuming the format field before the branch.
     */
    private static byte[] versionTwoBytes(String uri, long bytes, long rows, Long checkpointId)
            throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream data = new java.io.DataOutputStream(out)) {
            data.writeUTF("0123456789abcdef0123456789abcdef");
            data.writeUTF("p");
            data.writeUTF("d");
            data.writeUTF("t");
            data.writeUTF(uri);
            data.writeLong(bytes);
            data.writeLong(rows);
            data.writeBoolean(checkpointId != null);
            if (checkpointId != null) {
                data.writeLong(checkpointId);
            }
        }
        return out.toByteArray();
    }

    @Test
    void rejectsUnknownVersion() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                        "0123456789abcdef0123456789abcdef",
                        TableDestination.of("p", "d", "t"),
                        "gs://bucket/o.avro",
                        1L,
                        1L,
                        StagingFormat.AVRO);
        byte[] bytes = serializer.serialize(committable);

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }
}
