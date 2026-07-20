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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

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
 * tables, and how checkpoint-triggered loads are paced in streaming execution.
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

    private final String stagingPath;
    @Nullable private final String tempDataset;
    private final WriteDisposition writeDisposition;
    private final Duration minCheckpointInterval;

    private FileLoadsOptions(Builder builder) {
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
                && minCheckpointInterval.equals(that.minCheckpointInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stagingPath, tempDataset, writeDisposition, minCheckpointInterval);
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
                + "}";
    }

    /** Builder for {@link FileLoadsOptions}. */
    @PublicEvolving
    public static final class Builder {

        private String stagingPath;
        @Nullable private String tempDataset;
        private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
        private Duration minCheckpointInterval = DEFAULT_MIN_CHECKPOINT_INTERVAL;

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
         * Builds the options.
         *
         * @return the options
         */
        public FileLoadsOptions build() {
            Preconditions.checkState(
                    stagingPath != null, "stagingPath is required: set stagingPath(\"gs://...\").");
            return new FileLoadsOptions(this);
        }
    }
}
