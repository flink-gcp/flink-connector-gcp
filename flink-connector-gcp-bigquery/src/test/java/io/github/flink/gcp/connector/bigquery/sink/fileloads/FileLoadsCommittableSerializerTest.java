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
                        StagingFormat.AVRO,
                        "my_dataset");

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
                                StagingFormat.AVRO,
                                "d")
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
                        StagingFormat.PARQUET,
                        "d");

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getFormat()).isEqualTo(StagingFormat.PARQUET);
    }

    @Test
    void roundTripsTheJobProject() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                                "0123456789abcdef0123456789abcdef",
                                TableDestination.of("p", "d", "t"),
                                "gs://bucket/o.avro",
                                1L,
                                1L,
                                StagingFormat.AVRO,
                                "d")
                        .withCheckpointId(42L)
                        .withJobProject("jobs-project");

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getJobProject()).isEqualTo("jobs-project");
        assertThat(copy.getCheckpointId()).isEqualTo(42L);
        // The project is part of the committable's identity, not decoration.
        assertThat(copy).isNotEqualTo(committable.withJobProject(null));

        FileLoadsCommittable batch = committable.withCheckpointId(1L).withJobProject("other");
        FileLoadsCommittable batchWithoutCheckpoint =
                new FileLoadsCommittable(
                                batch.getFlinkJobId(),
                                batch.getDestination(),
                                batch.getUri(),
                                batch.getByteCount(),
                                batch.getRowCount(),
                                batch.getFormat(),
                                "d")
                        .withJobProject("other");
        assertThat(
                        serializer.deserialize(
                                serializer.getVersion(),
                                serializer.serialize(batchWithoutCheckpoint)))
                .isEqualTo(batchWithoutCheckpoint)
                .returns(null, FileLoadsCommittable::getCheckpointId)
                .returns("other", FileLoadsCommittable::getJobProject);
    }

    @Test
    void readsAVersionThreeCommittableWithoutAJobProject() throws IOException {
        // Version 3 is what every release before #2178 wrote, so its committables are in committer
        // state across the upgrade that adds the job project. No run then configured one, so the
        // restored committable carries none and commits where the restored job is configured.
        byte[] versionThree =
                versionThreeBytes("gs://bucket/o.parquet", StagingFormat.PARQUET, 42L);

        FileLoadsCommittable restored = serializer.deserialize(3, versionThree);

        assertThat(restored.getJobProject()).isNull();
        assertThat(restored.getFormat()).isEqualTo(StagingFormat.PARQUET);
        assertThat(restored.getCheckpointId()).isEqualTo(42L);
        assertThat(restored.isStagedByOlderVersion()).isTrue();
    }

    @Test
    void readsAVersionFourCommittableAsStagedByAnOlderVersion() throws IOException {
        // Every run before #2289 named its temporary tables and jobs from the Flink job id and
        // checkpoint id and recorded no temporary dataset. A restored commit that a successful
        // attempt already ran has had its staged files deleted, so it can only re-attach to that
        // attempt's jobs under the names they had.
        byte[] versionFour = versionFourBytes("gs://bucket/o.avro", 42L, "jobs-project");

        FileLoadsCommittable restored = serializer.deserialize(4, versionFour);

        assertThat(serializer.getVersion()).isEqualTo(5);
        assertThat(restored.isStagedByOlderVersion()).isTrue();
        assertThat(restored.getRecordedTempDataset()).isNull();
        assertThat(restored.getJobProject()).isEqualTo("jobs-project");
        assertThat(restored.getCheckpointId()).isEqualTo(42L);
    }

    @Test
    void keepsARestoredCommittableUnrecordedWhenWrittenAgain() throws IOException {
        // A restored committable that has not committed yet is written again by this version, as a
        // failed commit or a retained unaligned checkpoint's channel state does; it must still be
        // planned under the names its earlier attempt used.
        FileLoadsCommittable restored =
                serializer.deserialize(4, versionFourBytes("gs://bucket/o.avro", 42L, null));

        FileLoadsCommittable rewritten =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(restored));

        assertThat(rewritten).isEqualTo(restored);
        assertThat(rewritten.isStagedByOlderVersion()).isTrue();
        assertThat(rewritten.withCheckpointId(43L).isStagedByOlderVersion()).isTrue();
        assertThat(rewritten.withJobProject("jobs-project").isStagedByOlderVersion()).isTrue();
    }

    @Test
    void writesTheTemporaryDatasetOfNewCommittables() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                                "0123456789abcdef0123456789abcdef",
                                TableDestination.of("p", "d", "t"),
                                "gs://bucket/o.avro",
                                1L,
                                1L,
                                StagingFormat.AVRO,
                                "scratch")
                        .withCheckpointId(42L)
                        .withJobProject("jobs-project");

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getRecordedTempDataset()).isEqualTo("scratch");
        assertThat(copy.isStagedByOlderVersion()).isFalse();
        // The dataset is part of the committable's identity, not decoration.
        assertThat(copy)
                .isNotEqualTo(
                        new FileLoadsCommittable(
                                copy.getFlinkJobId(),
                                copy.getDestination(),
                                copy.getUri(),
                                copy.getByteCount(),
                                copy.getRowCount(),
                                copy.getFormat(),
                                copy.getCheckpointId(),
                                copy.getJobProject(),
                                "other"));
        assertThatThrownBy(
                        () ->
                                new FileLoadsCommittable(
                                        "0123456789abcdef0123456789abcdef",
                                        TableDestination.of("p", "d", "t"),
                                        "gs://bucket/o.avro",
                                        1L,
                                        1L,
                                        StagingFormat.AVRO,
                                        null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                new FileLoadsCommittable(
                                        "0123456789abcdef0123456789abcdef",
                                        TableDestination.of("p", "d", "t"),
                                        "gs://bucket/o.avro",
                                        1L,
                                        1L,
                                        StagingFormat.AVRO,
                                        42L,
                                        "jobs-project",
                                        null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void readsAndWritesTheVersionFiveLayout() throws IOException {
        // Pinned by hand, so the layout state this version writes cannot be edited in place by a
        // later one; a change takes a new version that still reads this one (ADR-0200).
        byte[] versionFive = versionFiveBytes("gs://bucket/o.avro", 42L, "jobs-project", "scratch");
        byte[] withoutDataset = versionFiveBytes("gs://bucket/o.avro", 42L, null, null);

        FileLoadsCommittable restored = serializer.deserialize(5, versionFive);
        FileLoadsCommittable unrecorded = serializer.deserialize(5, withoutDataset);

        assertThat(restored.getUri()).isEqualTo("gs://bucket/o.avro");
        assertThat(restored.getCheckpointId()).isEqualTo(42L);
        assertThat(restored.getJobProject()).isEqualTo("jobs-project");
        assertThat(restored.getRecordedTempDataset()).isEqualTo("scratch");
        assertThat(restored.isStagedByOlderVersion()).isFalse();
        assertThat(serializer.serialize(restored)).isEqualTo(versionFive);
        assertThat(unrecorded.getRecordedTempDataset()).isNull();
        assertThat(unrecorded.isStagedByOlderVersion()).isTrue();
        assertThat(serializer.serialize(unrecorded)).isEqualTo(withoutDataset);
    }

    /** Writes the version-5 layout by hand, for the reason {@link #versionTwoBytes} gives. */
    private static byte[] versionFiveBytes(
            String uri,
            Long checkpointId,
            @javax.annotation.Nullable String jobProject,
            @javax.annotation.Nullable String tempDataset)
            throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream data = new java.io.DataOutputStream(out)) {
            data.write(versionFourBytes(uri, checkpointId, jobProject));
            data.writeBoolean(tempDataset != null);
            if (tempDataset != null) {
                data.writeUTF(tempDataset);
            }
        }
        return out.toByteArray();
    }

    /** Writes the version-4 layout by hand, for the reason {@link #versionTwoBytes} gives. */
    private static byte[] versionFourBytes(
            String uri, Long checkpointId, @javax.annotation.Nullable String jobProject)
            throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream data = new java.io.DataOutputStream(out)) {
            data.write(versionThreeBytes(uri, StagingFormat.AVRO, checkpointId));
            data.writeBoolean(jobProject != null);
            if (jobProject != null) {
                data.writeUTF(jobProject);
            }
        }
        return out.toByteArray();
    }

    /** Writes the version-3 layout by hand, for the reason {@link #versionTwoBytes} gives. */
    private static byte[] versionThreeBytes(String uri, StagingFormat format, Long checkpointId)
            throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream data = new java.io.DataOutputStream(out)) {
            data.writeUTF("0123456789abcdef0123456789abcdef");
            data.writeUTF("p");
            data.writeUTF("d");
            data.writeUTF("t");
            data.writeUTF(uri);
            data.writeLong(1L);
            data.writeLong(1L);
            data.writeUTF(format.name());
            data.writeBoolean(checkpointId != null);
            if (checkpointId != null) {
                data.writeLong(checkpointId);
            }
        }
        return out.toByteArray();
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
        assertThat(restored.isStagedByOlderVersion()).isTrue();
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
                        StagingFormat.AVRO,
                        "d");
        byte[] bytes = serializer.serialize(committable);

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }
}
