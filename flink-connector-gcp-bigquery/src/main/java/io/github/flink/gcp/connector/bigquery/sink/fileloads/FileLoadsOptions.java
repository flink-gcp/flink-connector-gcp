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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Options specific to {@link WriteMethod#FILE_LOADS}: where staging files go on Cloud Storage, how
 * loaded rows land in tables that already hold data, where oversized loads stage their temporary
 * tables, how checkpoint-triggered loads are paced in streaming execution, and the two schedules
 * the committer backs off on.
 *
 * <p>The two schedules pace different things. {@code loadJobPoll*} is how often a submitted load
 * job's completion is checked — the caller's own {@code jobs.get} rate and how promptly a finished
 * load is noticed — and has deliberately <b>no attempt cap</b>: batch loads may legitimately run
 * for hours, and bounding the polling would fail a load that was progressing normally. Overall
 * timeouts are the Flink job's to enforce. {@code schemaUpdate*} is the budget for losing an etag
 * race when parallel subtasks reconcile the same table's schema, so it scales with the job's
 * parallelism.
 *
 * <p>Set via {@link BigQuerySinkBuilder#fileLoadsOptions(FileLoadsOptions)}; required when building
 * a {@code FILE_LOADS} sink and rejected for every other write method.
 *
 * <p>The staging path should point at a bucket dedicated to load staging — separate from
 * checkpoint/savepoint storage — with a lifecycle rule that expires stale objects. Staged files are
 * deleted after a successful load, but cleanup is best-effort and files are deliberately kept when
 * a load fails (so a Flink restart can retry deterministically), so orphaned objects can accumulate
 * without a lifecycle rule.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class FileLoadsOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * {@code gs://bucket} or {@code gs://bucket/prefix} (bucket naming checked by GCS itself). The
     * scheme is case-sensitive: the GCS client and BigQuery load jobs only accept lowercase {@code
     * gs://}.
     */
    private static final Pattern STAGING_PATH_PATTERN = Pattern.compile("gs://[^/]+(/.+)?");

    /**
     * Default for {@link Builder#minCheckpointInterval(Duration)}. Two minutes keeps a sustained
     * streaming job at 720 load jobs per table per day, safely under BigQuery's 1,500 limit.
     */
    public static final Duration DEFAULT_MIN_CHECKPOINT_INTERVAL = Duration.ofMinutes(2);

    /** Default for {@link Builder#loadJobPollInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF = Duration.ofSeconds(1);

    /** Default for {@link Builder#loadJobPollMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF = Duration.ofSeconds(30);

    /** Default for {@link Builder#schemaUpdateInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_SCHEMA_UPDATE_INITIAL_BACKOFF = Duration.ofMillis(500);

    /** Default for {@link Builder#schemaUpdateMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_SCHEMA_UPDATE_MAX_BACKOFF = Duration.ofSeconds(10);

    /** Default for {@link Builder#schemaUpdateMaxAttempts(int)}. */
    public static final int DEFAULT_SCHEMA_UPDATE_MAX_ATTEMPTS = 10;

    private final String stagingPath;
    @Nullable private final String tempDataset;
    private final WriteDisposition writeDisposition;
    private final Duration minCheckpointInterval;
    private final Duration loadJobPollInitialBackoff;
    private final Duration loadJobPollMaxBackoff;
    private final Duration schemaUpdateInitialBackoff;
    private final Duration schemaUpdateMaxBackoff;
    private final int schemaUpdateMaxAttempts;

    private FileLoadsOptions(Builder builder) {
        this.loadJobPollInitialBackoff = builder.loadJobPollInitialBackoff;
        this.loadJobPollMaxBackoff = builder.loadJobPollMaxBackoff;
        this.schemaUpdateInitialBackoff = builder.schemaUpdateInitialBackoff;
        this.schemaUpdateMaxBackoff = builder.schemaUpdateMaxBackoff;
        this.schemaUpdateMaxAttempts = builder.schemaUpdateMaxAttempts;
        this.stagingPath = builder.stagingPath;
        this.tempDataset = builder.tempDataset;
        this.writeDisposition = builder.writeDisposition;
        this.minCheckpointInterval = builder.minCheckpointInterval;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the Cloud Storage path staged files are written under, without a trailing slash. */
    public String getStagingPath() {
        return stagingPath;
    }

    /**
     * Returns the dataset holding temporary tables for loads exceeding per-job limits, or {@code
     * null} to use each destination table's own dataset.
     */
    @Nullable
    public String getTempDataset() {
        return tempDataset;
    }

    /** Returns how loaded rows land in a destination table that already contains data. */
    public WriteDisposition getWriteDisposition() {
        return writeDisposition;
    }

    /**
     * Returns the smallest checkpoint interval accepted for streaming execution. Ignored in batch
     * execution.
     */
    public Duration getMinCheckpointInterval() {
        return minCheckpointInterval;
    }

    /** Returns the first backoff between load-job completion polls. */
    public Duration getLoadJobPollInitialBackoff() {
        return loadJobPollInitialBackoff;
    }

    /** Returns the backoff cap between load-job completion polls. */
    public Duration getLoadJobPollMaxBackoff() {
        return loadJobPollMaxBackoff;
    }

    /** Returns the first backoff of the schema-reconcile budget. */
    public Duration getSchemaUpdateInitialBackoff() {
        return schemaUpdateInitialBackoff;
    }

    /** Returns the backoff cap of the schema-reconcile budget. */
    public Duration getSchemaUpdateMaxBackoff() {
        return schemaUpdateMaxBackoff;
    }

    /** Returns the maximum number of attempts of the schema-reconcile budget. */
    public int getSchemaUpdateMaxAttempts() {
        return schemaUpdateMaxAttempts;
    }

    /**
     * Returns the load-job completion polling schedule. Effectively unbounded in attempts, so a
     * long-running batch load is awaited rather than abandoned.
     */
    @Internal
    public RetrySchedule toLoadJobPollSchedule() {
        return new RetrySchedule(
                loadJobPollInitialBackoff.toMillis(),
                loadJobPollMaxBackoff.toMillis(),
                Integer.MAX_VALUE,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    /** Returns the schema-reconcile budget the {@code schemaUpdate*} knobs describe. */
    @Internal
    public RetrySchedule toSchemaUpdateSchedule() {
        return new RetrySchedule(
                schemaUpdateInitialBackoff.toMillis(),
                schemaUpdateMaxBackoff.toMillis(),
                schemaUpdateMaxAttempts,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileLoadsOptions that = (FileLoadsOptions) o;
        return stagingPath.equals(that.stagingPath)
                && Objects.equals(tempDataset, that.tempDataset)
                && writeDisposition == that.writeDisposition
                && minCheckpointInterval.equals(that.minCheckpointInterval)
                && loadJobPollInitialBackoff.equals(that.loadJobPollInitialBackoff)
                && loadJobPollMaxBackoff.equals(that.loadJobPollMaxBackoff)
                && schemaUpdateInitialBackoff.equals(that.schemaUpdateInitialBackoff)
                && schemaUpdateMaxBackoff.equals(that.schemaUpdateMaxBackoff)
                && schemaUpdateMaxAttempts == that.schemaUpdateMaxAttempts;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                stagingPath,
                tempDataset,
                writeDisposition,
                minCheckpointInterval,
                loadJobPollInitialBackoff,
                loadJobPollMaxBackoff,
                schemaUpdateInitialBackoff,
                schemaUpdateMaxBackoff,
                schemaUpdateMaxAttempts);
    }

    @Override
    public String toString() {
        return "FileLoadsOptions{stagingPath="
                + stagingPath
                + ", tempDataset="
                + tempDataset
                + ", writeDisposition="
                + writeDisposition
                + ", minCheckpointInterval="
                + minCheckpointInterval
                + ", loadJobPollInitialBackoff="
                + loadJobPollInitialBackoff
                + ", loadJobPollMaxBackoff="
                + loadJobPollMaxBackoff
                + ", schemaUpdateInitialBackoff="
                + schemaUpdateInitialBackoff
                + ", schemaUpdateMaxBackoff="
                + schemaUpdateMaxBackoff
                + ", schemaUpdateMaxAttempts="
                + schemaUpdateMaxAttempts
                + "}";
    }

    /** Builder for {@link FileLoadsOptions}. */
    @PublicEvolving
    public static final class Builder {

        private String stagingPath;
        @Nullable private String tempDataset;
        private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
        private Duration minCheckpointInterval = DEFAULT_MIN_CHECKPOINT_INTERVAL;
        private Duration loadJobPollInitialBackoff = DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF;
        private Duration loadJobPollMaxBackoff = DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF;
        private Duration schemaUpdateInitialBackoff = DEFAULT_SCHEMA_UPDATE_INITIAL_BACKOFF;
        private Duration schemaUpdateMaxBackoff = DEFAULT_SCHEMA_UPDATE_MAX_BACKOFF;
        private int schemaUpdateMaxAttempts = DEFAULT_SCHEMA_UPDATE_MAX_ATTEMPTS;

        private Builder() {}

        /**
         * Sets the Cloud Storage path staged files are written under. Required; must be of the form
         * {@code gs://bucket} or {@code gs://bucket/prefix}. A trailing slash is stripped.
         *
         * @param stagingPath the staging path
         * @return this builder
         */
        public Builder stagingPath(String stagingPath) {
            Preconditions.checkNotNull(stagingPath, "stagingPath must not be null");
            String normalized =
                    stagingPath.endsWith("/")
                            ? stagingPath.substring(0, stagingPath.length() - 1)
                            : stagingPath;
            Preconditions.checkArgument(
                    STAGING_PATH_PATTERN.matcher(normalized).matches(),
                    "stagingPath must be of the form gs://bucket[/prefix]: '%s'",
                    stagingPath);
            this.stagingPath = normalized;
            return this;
        }

        /**
         * Sets the dataset holding temporary tables when a table's staged files exceed the
         * per-load-job limits and rows go through temporary tables plus a copy job. Optional;
         * defaults to each destination table's own dataset. A dedicated dataset with a default
         * table expiration is recommended so temporary tables orphaned by hard failures are
         * garbage-collected.
         *
         * @param tempDataset the dataset id, in the same project as the destination table
         * @return this builder
         */
        public Builder tempDataset(String tempDataset) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(tempDataset),
                    "tempDataset must not be blank");
            this.tempDataset = tempDataset;
            return this;
        }

        /**
         * Sets how loaded rows land in a destination table that already contains data. Defaults to
         * {@link WriteDisposition#WRITE_APPEND}.
         *
         * @param writeDisposition the write disposition
         * @return this builder
         */
        public Builder writeDisposition(WriteDisposition writeDisposition) {
            this.writeDisposition =
                    Preconditions.checkNotNull(
                            writeDisposition, "writeDisposition must not be null");
            return this;
        }

        /**
         * Sets the smallest checkpoint interval accepted for streaming execution; a configured
         * interval below it is rejected when the job graph is built. Defaults to {@link
         * FileLoadsOptions#DEFAULT_MIN_CHECKPOINT_INTERVAL}. Lowering it is an explicit opt-in for
         * jobs whose daily load-job count stays safe despite fast checkpoints (e.g. short-lived
         * streaming jobs): BigQuery allows 1,500 load jobs per table per day, and each checkpoint
         * issues at least one load job per destination table. Ignored in batch execution.
         *
         * @param minCheckpointInterval the smallest accepted checkpoint interval
         * @return this builder
         */
        public Builder minCheckpointInterval(Duration minCheckpointInterval) {
            Preconditions.checkNotNull(
                    minCheckpointInterval, "minCheckpointInterval must not be null");
            Preconditions.checkArgument(
                    !minCheckpointInterval.isNegative() && !minCheckpointInterval.isZero(),
                    "minCheckpointInterval must be positive: %s",
                    minCheckpointInterval);
            this.minCheckpointInterval = minCheckpointInterval;
            return this;
        }

        /**
         * Sets the first backoff between polls of a submitted load job's completion. Defaults to
         * {@link FileLoadsOptions#DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF}. Lowering it notices a
         * finished load sooner at the cost of more {@code jobs.get} calls; raising it does the
         * reverse. There is no attempt cap to configure — see the class javadoc.
         *
         * @param loadJobPollInitialBackoff the first poll backoff, positive
         * @return this builder
         */
        public Builder loadJobPollInitialBackoff(Duration loadJobPollInitialBackoff) {
            this.loadJobPollInitialBackoff =
                    checkPositive(loadJobPollInitialBackoff, "loadJobPollInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff between polls of a submitted load job's completion. Must be at least the
         * initial backoff. Defaults to {@link FileLoadsOptions#DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF}.
         *
         * @param loadJobPollMaxBackoff the poll backoff cap, positive
         * @return this builder
         */
        public Builder loadJobPollMaxBackoff(Duration loadJobPollMaxBackoff) {
            this.loadJobPollMaxBackoff =
                    checkPositive(loadJobPollMaxBackoff, "loadJobPollMaxBackoff");
            return this;
        }

        /**
         * Sets the first backoff after losing an etag race while reconciling a destination table's
         * schema. Defaults to {@link FileLoadsOptions#DEFAULT_SCHEMA_UPDATE_INITIAL_BACKOFF}.
         *
         * @param schemaUpdateInitialBackoff the first backoff, positive
         * @return this builder
         */
        public Builder schemaUpdateInitialBackoff(Duration schemaUpdateInitialBackoff) {
            this.schemaUpdateInitialBackoff =
                    checkPositive(schemaUpdateInitialBackoff, "schemaUpdateInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the schema-reconcile budget. Must be at least the initial backoff.
         * Defaults to {@link FileLoadsOptions#DEFAULT_SCHEMA_UPDATE_MAX_BACKOFF}.
         *
         * @param schemaUpdateMaxBackoff the backoff cap, positive
         * @return this builder
         */
        public Builder schemaUpdateMaxBackoff(Duration schemaUpdateMaxBackoff) {
            this.schemaUpdateMaxBackoff =
                    checkPositive(schemaUpdateMaxBackoff, "schemaUpdateMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts at reconciling a destination table's schema. Defaults to {@link
         * FileLoadsOptions#DEFAULT_SCHEMA_UPDATE_MAX_ATTEMPTS}. Each attempt is a fresh read, union
         * and etag-conditioned update, so only lost races consume attempts — raise it for jobs
         * whose parallelism makes those races frequent.
         *
         * @param schemaUpdateMaxAttempts the attempt cap, positive
         * @return this builder
         */
        public Builder schemaUpdateMaxAttempts(int schemaUpdateMaxAttempts) {
            Preconditions.checkArgument(
                    schemaUpdateMaxAttempts > 0,
                    "schemaUpdateMaxAttempts must be positive: %s",
                    schemaUpdateMaxAttempts);
            this.schemaUpdateMaxAttempts = schemaUpdateMaxAttempts;
            return this;
        }

        private static Duration checkPositive(Duration value, String name) {
            Preconditions.checkNotNull(value, name + " must not be null");
            Preconditions.checkArgument(
                    !value.isNegative() && !value.isZero(), name + " must be positive: %s", value);
            return value;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public FileLoadsOptions build() {
            Preconditions.checkState(
                    stagingPath != null, "stagingPath is required: set stagingPath(\"gs://...\").");
            Preconditions.checkState(
                    loadJobPollMaxBackoff.compareTo(loadJobPollInitialBackoff) >= 0,
                    "loadJobPollMaxBackoff must be >= loadJobPollInitialBackoff: %s < %s",
                    loadJobPollMaxBackoff,
                    loadJobPollInitialBackoff);
            Preconditions.checkState(
                    schemaUpdateMaxBackoff.compareTo(schemaUpdateInitialBackoff) >= 0,
                    "schemaUpdateMaxBackoff must be >= schemaUpdateInitialBackoff: %s < %s",
                    schemaUpdateMaxBackoff,
                    schemaUpdateInitialBackoff);
            return new FileLoadsOptions(this);
        }
    }
}
