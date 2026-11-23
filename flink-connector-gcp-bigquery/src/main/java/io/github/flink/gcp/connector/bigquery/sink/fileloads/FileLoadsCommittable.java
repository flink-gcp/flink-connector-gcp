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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One finalized staging file: the Flink job id of the run that staged it, its destination table,
 * its Cloud Storage URI, size counters used for load-job partitioning, the checkpoint that
 * triggered it in streaming execution, the project that run's BigQuery jobs run in when it
 * configured one, and the dataset that run put the destination's temporary tables in.
 *
 * <p>Load jobs reference exactly the URIs collected from committables — never a bucket prefix — so
 * objects orphaned by failed attempts can never leak into a load.
 *
 * <p>The originating Flink job id travels with the committable (rather than being read from the
 * runtime at commit time) so that BigQuery job ids and temporary table names stay deterministic
 * when committer state is restored under a <em>new</em> Flink job id (a savepoint or retained
 * checkpoint resubmitted with {@code flink run -s}): the re-commit reproduces the original ids and
 * re-attaches instead of double-loading. The job project travels for the same reason: a re-commit
 * looks for the previous attempt's jobs in the project that created them (#2178).
 *
 * <p>The staging format travels here rather than being read from configuration at commit time,
 * because a committable recovered from state must be loaded as the format its file was actually
 * written in — which the configuration no longer says once the option changes, or once an upgrade
 * introduces the option at all and the in-flight committables are Avro. One load job cannot mix
 * formats, so the orchestrator groups on it (see {@code LoadJobOrchestrator}).
 *
 * <p>The Parquet <em>codec</em> deliberately does not travel: a Parquet footer names its own, and
 * the load job is never told, so carrying it would be state nobody reads.
 *
 * <p>The checkpoint id is stamped by the pre-commit stage (the writer does not know it); it stays
 * {@code null} in batch execution and selects the streaming behavior of the load-job orchestrator
 * (visible {@code -c<id>} job-id segment and checkpoint-scoped overflow tables).
 *
 * <p>The dataset of the destination's temporary tables travels for the reason the job project does
 * (#2289): a re-commit names the tables the attempt that may already have run it filled, whatever
 * {@code tempDataset} the restored job configures, or a destination whose copy that attempt
 * completed is copied again. Its presence also tells the planner which names that attempt derived:
 * a committable restored from state an older version wrote carries none, and its commit is planned
 * in the configured dataset under the names that version gave its temporary tables and jobs.
 */
@Internal
public final class FileLoadsCommittable {

    private final String flinkJobId;
    private final TableDestination destination;
    private final String uri;
    private final long byteCount;
    private final long rowCount;
    private final StagingFormat format;
    @Nullable private final Long checkpointId;
    @Nullable private final String jobProject;
    @Nullable private final String recordedTempDataset;

    /**
     * Creates a committable without a checkpoint id, as the writer emits it.
     *
     * @param flinkJobId the Flink job id (hex) of the run that staged the file
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     * @param format the format the file was written in
     * @param recordedTempDataset the dataset of the destination's temporary tables as the run that
     *     staged the file resolved it (see {@link #resolveTempDataset(String, TableDestination)})
     */
    public FileLoadsCommittable(
            String flinkJobId,
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            StagingFormat format,
            String recordedTempDataset) {
        this(
                Preconditions.checkNotNull(recordedTempDataset),
                flinkJobId,
                destination,
                uri,
                byteCount,
                rowCount,
                format,
                null,
                null);
    }

    /**
     * Creates a committable.
     *
     * @param flinkJobId the Flink job id (hex) of the run that staged the file
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     * @param format the format the file was written in
     * @param checkpointId the checkpoint that triggered this file, or {@code null} in batch
     *     execution
     * @param jobProject the project the run that staged the file runs its BigQuery jobs in, or
     *     {@code null} when that run configured none
     * @param recordedTempDataset the dataset of the destination's temporary tables as the run that
     *     staged the file resolved it (see {@link #resolveTempDataset(String, TableDestination)})
     */
    public FileLoadsCommittable(
            String flinkJobId,
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            StagingFormat format,
            @Nullable Long checkpointId,
            @Nullable String jobProject,
            String recordedTempDataset) {
        this(
                Preconditions.checkNotNull(recordedTempDataset),
                flinkJobId,
                destination,
                uri,
                byteCount,
                rowCount,
                format,
                checkpointId,
                jobProject);
    }

    /**
     * Restores a committable that a version before #2289 staged, which recorded no temporary
     * dataset (see {@link #isStagedByOlderVersion()}). Package-private, so that in main code only
     * the committable serializer, in this package, creates one; everything this version stages
     * records its dataset.
     *
     * @param flinkJobId the Flink job id (hex) of the run that staged the file
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     * @param format the format the file was written in
     * @param checkpointId the checkpoint that triggered this file, or {@code null} in batch
     *     execution
     * @param jobProject the project the run that staged the file runs its BigQuery jobs in, or
     *     {@code null} when that run configured none
     * @return the committable
     */
    static FileLoadsCommittable stagedByOlderVersion(
            String flinkJobId,
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            StagingFormat format,
            @Nullable Long checkpointId,
            @Nullable String jobProject) {
        return new FileLoadsCommittable(
                null,
                flinkJobId,
                destination,
                uri,
                byteCount,
                rowCount,
                format,
                checkpointId,
                jobProject);
    }

    /**
     * The one constructor that accepts no temporary dataset, so only {@link
     * #stagedByOlderVersion(String, TableDestination, String, long, long, StagingFormat, Long,
     * String)} and the copies below can produce such a committable. The dataset leads so that the
     * signature differs from the public constructor's.
     */
    private FileLoadsCommittable(
            @Nullable String recordedTempDataset,
            String flinkJobId,
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            StagingFormat format,
            @Nullable Long checkpointId,
            @Nullable String jobProject) {
        this.flinkJobId = flinkJobId;
        this.destination = destination;
        this.uri = uri;
        this.byteCount = byteCount;
        this.rowCount = rowCount;
        this.format = format;
        this.checkpointId = checkpointId;
        this.jobProject = jobProject;
        this.recordedTempDataset = recordedTempDataset;
    }

    /** Returns a copy of this committable stamped with the given checkpoint id. */
    public FileLoadsCommittable withCheckpointId(long checkpointId) {
        return new FileLoadsCommittable(
                recordedTempDataset,
                flinkJobId,
                destination,
                uri,
                byteCount,
                rowCount,
                format,
                checkpointId,
                jobProject);
    }

    /**
     * Returns a copy of this committable stamped with the project its run's BigQuery jobs run in.
     *
     * @param jobProject the project, or {@code null} when the run configures none
     * @return the stamped committable
     */
    public FileLoadsCommittable withJobProject(@Nullable String jobProject) {
        return new FileLoadsCommittable(
                recordedTempDataset,
                flinkJobId,
                destination,
                uri,
                byteCount,
                rowCount,
                format,
                checkpointId,
                jobProject);
    }

    /** Returns the Flink job id (hex) of the run that staged the file. */
    public String getFlinkJobId() {
        return flinkJobId;
    }

    /** Returns the destination table. */
    public TableDestination getDestination() {
        return destination;
    }

    /** Returns the staging object URI. */
    public String getUri() {
        return uri;
    }

    /** Returns the object size in bytes. */
    public long getByteCount() {
        return byteCount;
    }

    /** Returns the number of rows in the object. */
    public long getRowCount() {
        return rowCount;
    }

    /** Returns the format the file was written in. */
    public StagingFormat getFormat() {
        return format;
    }

    /**
     * Returns the checkpoint that triggered this file, or {@code null} in batch execution (or
     * before the pre-commit stage stamped it).
     */
    @Nullable
    public Long getCheckpointId() {
        return checkpointId;
    }

    /**
     * Returns the project the run that staged the file runs its BigQuery jobs in, or {@code null}
     * when that run configured none.
     *
     * <p>A commit looks for a previous attempt's jobs in this project, so it travels with the file
     * rather than being read from configuration at commit time: a restored commit must look where
     * the attempt that may already have run it created its jobs, whatever the restored job
     * configures. A committable carrying none commits where the committing job's configuration
     * says.
     */
    @Nullable
    public String getJobProject() {
        return jobProject;
    }

    /**
     * Returns the dataset a run puts a destination's temporary tables in: its configured {@code
     * tempDataset}, or the destination's own dataset when it configures none. The writer records
     * it, and a commit of committables an older version wrote resolves it the same way.
     *
     * @param configuredTempDataset the run's configured {@code tempDataset}, or {@code null}
     * @param destination the destination table
     * @return the dataset
     */
    public static String resolveTempDataset(
            @Nullable String configuredTempDataset, TableDestination destination) {
        return configuredTempDataset != null ? configuredTempDataset : destination.getDataset();
    }

    /**
     * Returns the dataset of the destination's temporary tables as the run that staged the file
     * resolved it, or {@code null} for a file a version before #2289 staged.
     *
     * <p>A commit names its temporary tables in this dataset rather than in the one the committing
     * job configures, so a restored commit finds the tables and jobs the attempt that may already
     * have run it left; a committable carrying none uses the configured dataset.
     */
    @Nullable
    public String getRecordedTempDataset() {
        return recordedTempDataset;
    }

    /**
     * Returns whether a version before #2289 staged this file, which then recorded no temporary
     * dataset.
     *
     * <p>Its commit is planned under the temporary-table names and job ids that version derived
     * from the Flink job id and checkpoint id rather than from the staged files: a successful
     * commit deletes its files, and every restore commits the last checkpoint's committables again,
     * so a restored commit can only re-attach to the jobs its attempt ran under those names. The
     * record is persisted rather than derived from the serializer version, because a restored
     * committable that has not yet committed is written again in the current version.
     */
    public boolean isStagedByOlderVersion() {
        return recordedTempDataset == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileLoadsCommittable that = (FileLoadsCommittable) o;
        return byteCount == that.byteCount
                && rowCount == that.rowCount
                && flinkJobId.equals(that.flinkJobId)
                && destination.equals(that.destination)
                && uri.equals(that.uri)
                && format == that.format
                && Objects.equals(checkpointId, that.checkpointId)
                && Objects.equals(jobProject, that.jobProject)
                && Objects.equals(recordedTempDataset, that.recordedTempDataset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                flinkJobId,
                destination,
                uri,
                byteCount,
                rowCount,
                format,
                checkpointId,
                jobProject,
                recordedTempDataset);
    }

    @Override
    public String toString() {
        return "FileLoadsCommittable{flinkJobId="
                + flinkJobId
                + ", destination="
                + destination
                + ", uri="
                + uri
                + ", byteCount="
                + byteCount
                + ", rowCount="
                + rowCount
                + ", format="
                + format
                + ", checkpointId="
                + checkpointId
                + (jobProject != null ? ", jobProject=" + jobProject : "")
                + (recordedTempDataset != null
                        ? ", recordedTempDataset=" + recordedTempDataset
                        : "")
                + "}";
    }
}
