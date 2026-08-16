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
import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.options.OptionChecks;
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
 * <p>The two schedules pace different things. {@code loadJobPoll*} is how often a submitted load or
 * copy job's completion is checked — the caller's own {@code jobs.get} rate and how promptly a
 * finished job is noticed — and has deliberately <b>no attempt cap</b>: batch loads may
 * legitimately run for hours, and bounding the polling would fail a load that was progressing
 * normally. Overall timeouts are the Flink job's to enforce.
 *
 * <p>{@code schemaReconcile*} is the budget for losing an etag race while reconciling a destination
 * table's schema. Those races do <b>not</b> come from this job's parallelism — FILE_LOADS
 * reconciles from a single committer subtask — but from anything else updating the same table at
 * the same time: a second Flink job, a Storage Write API sink writing the same destination, or
 * external tooling.
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
@Public
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
     * streaming job at 720 destination-table modifications per day, safely under the 1,500 limit
     * for standard tables.
     */
    public static final Duration DEFAULT_MIN_CHECKPOINT_INTERVAL = Duration.ofMinutes(2);

    /** Default for {@link Builder#loadJobPollInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF = Duration.ofSeconds(1);

    /** Default for {@link Builder#loadJobPollMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF = Duration.ofSeconds(30);

    /** Default for {@link Builder#schemaReconcileInitialBackoff(Duration)}. */
    public static final Duration DEFAULT_SCHEMA_RECONCILE_INITIAL_BACKOFF = Duration.ofMillis(500);

    /** Default for {@link Builder#schemaReconcileMaxBackoff(Duration)}. */
    public static final Duration DEFAULT_SCHEMA_RECONCILE_MAX_BACKOFF = Duration.ofSeconds(10);

    /** Default for {@link Builder#schemaReconcileMaxAttempts(int)}. */
    public static final int DEFAULT_SCHEMA_RECONCILE_MAX_ATTEMPTS = 10;

    /**
     * Default for {@link Builder#maxStagingFileBytes(long)}: 16 MiB, chosen from measured load
     * throughput rather than from the URI arithmetic alone that chose its 1.5 GiB predecessor.
     *
     * <p>Measured against real BigQuery on 2026-08-08 — 769 MiB staged as Avro, seven loads per
     * point, configurations interleaved — load duration against staging file size is a basin with a
     * floor near 8 MiB and steep sides: 2 MiB took 15.0 s, 4 MiB 9.7 s, 8 MiB 8.3 s, 16 MiB 9.3 s,
     * 32 MiB 11.1 s and 128 MiB 16.9 s. So smaller is <em>not</em> monotonically better, and any
     * change to this value needs a floor as well as a ceiling.
     *
     * <p>16 MiB rather than the measured optimum because of what the value trades against: a load
     * job takes at most 10,000 source URIs, so this size sets how much of one destination goes
     * through a single load job before the temporary-table plus copy path is needed — ~156 GiB
     * here, ~78 GiB at 8 MiB, ~14.6 TiB at the 1.5 GiB this replaces. Twice the headroom costs 12%
     * of load time.
     *
     * <p>The threshold only fires where a subtask writes more than it to one destination between
     * commits. At high parallelism a checkpoint's data divided by the subtask count is already
     * inside the band, and this value never applies.
     */
    public static final long DEFAULT_MAX_STAGING_FILE_BYTES = 16L * 1024 * 1024;

    /** Default for {@link Builder#stagingFormat(StagingFormat)}. */
    public static final StagingFormat DEFAULT_STAGING_FORMAT = StagingFormat.AVRO;

    /** Default for {@link Builder#parquetCompression(ParquetCompression)}. */
    public static final ParquetCompression DEFAULT_PARQUET_COMPRESSION = ParquetCompression.ZSTD;

    private final String stagingPath;
    @Nullable private final String tempDataset;
    private final WriteDisposition writeDisposition;
    private final Duration minCheckpointInterval;
    private final long maxStagingFileBytes;
    private final StagingFormat stagingFormat;
    private final ParquetCompression parquetCompression;
    private final Duration loadJobPollInitialBackoff;
    private final Duration loadJobPollMaxBackoff;
    private final Duration schemaReconcileInitialBackoff;
    private final Duration schemaReconcileMaxBackoff;
    private final int schemaReconcileMaxAttempts;
    private final boolean perDestinationMetrics;

    private FileLoadsOptions(Builder builder) {
        this.loadJobPollInitialBackoff = builder.loadJobPollInitialBackoff;
        this.loadJobPollMaxBackoff = builder.loadJobPollMaxBackoff;
        this.schemaReconcileInitialBackoff = builder.schemaReconcileInitialBackoff;
        this.schemaReconcileMaxBackoff = builder.schemaReconcileMaxBackoff;
        this.schemaReconcileMaxAttempts = builder.schemaReconcileMaxAttempts;
        this.stagingPath = builder.stagingPath;
        this.tempDataset = builder.tempDataset;
        this.writeDisposition = builder.writeDisposition;
        this.minCheckpointInterval = builder.minCheckpointInterval;
        this.maxStagingFileBytes = builder.maxStagingFileBytes;
        this.stagingFormat = builder.stagingFormat;
        this.parquetCompression =
                builder.parquetCompression == null
                        ? DEFAULT_PARQUET_COMPRESSION
                        : builder.parquetCompression;
        this.perDestinationMetrics = builder.perDestinationMetrics;
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

    /** Returns the size at which an open staging file is finished and the next one opened. */
    public long getMaxStagingFileBytes() {
        return maxStagingFileBytes;
    }

    /**
     * Returns the format staging files are written in, before the per-destination {@code JSON}
     * override the writer applies.
     */
    public StagingFormat getStagingFormat() {
        return stagingFormat;
    }

    /** Returns how Parquet staging files are compressed; meaningless under Avro. */
    public ParquetCompression getParquetCompression() {
        return parquetCompression;
    }

    /** Returns the first backoff between load- or copy-job completion polls. */
    public Duration getLoadJobPollInitialBackoff() {
        return loadJobPollInitialBackoff;
    }

    /** Returns the backoff cap between load- or copy-job completion polls. */
    public Duration getLoadJobPollMaxBackoff() {
        return loadJobPollMaxBackoff;
    }

    /** Returns the first backoff of the schema-reconcile budget. */
    public Duration getSchemaUpdateInitialBackoff() {
        return schemaReconcileInitialBackoff;
    }

    /** Returns the backoff cap of the schema-reconcile budget. */
    public Duration getSchemaUpdateMaxBackoff() {
        return schemaReconcileMaxBackoff;
    }

    /** Returns the maximum number of attempts of the schema-reconcile budget. */
    public int getSchemaUpdateMaxAttempts() {
        return schemaReconcileMaxAttempts;
    }

    /** Returns whether per-destination send counters are registered. */
    public boolean isPerDestinationMetrics() {
        return perDestinationMetrics;
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

    /** Returns the schema-reconcile budget the {@code schemaReconcile*} knobs describe. */
    @Internal
    public RetrySchedule toSchemaReconcileSchedule() {
        return new RetrySchedule(
                schemaReconcileInitialBackoff.toMillis(),
                schemaReconcileMaxBackoff.toMillis(),
                schemaReconcileMaxAttempts,
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
                && maxStagingFileBytes == that.maxStagingFileBytes
                && stagingFormat == that.stagingFormat
                && parquetCompression == that.parquetCompression
                && loadJobPollInitialBackoff.equals(that.loadJobPollInitialBackoff)
                && loadJobPollMaxBackoff.equals(that.loadJobPollMaxBackoff)
                && schemaReconcileInitialBackoff.equals(that.schemaReconcileInitialBackoff)
                && schemaReconcileMaxBackoff.equals(that.schemaReconcileMaxBackoff)
                && schemaReconcileMaxAttempts == that.schemaReconcileMaxAttempts
                && perDestinationMetrics == that.perDestinationMetrics;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                stagingPath,
                tempDataset,
                writeDisposition,
                minCheckpointInterval,
                maxStagingFileBytes,
                stagingFormat,
                parquetCompression,
                loadJobPollInitialBackoff,
                loadJobPollMaxBackoff,
                schemaReconcileInitialBackoff,
                schemaReconcileMaxBackoff,
                schemaReconcileMaxAttempts,
                perDestinationMetrics);
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
                + ", maxStagingFileBytes="
                + maxStagingFileBytes
                + ", stagingFormat="
                + stagingFormat
                + ", parquetCompression="
                + parquetCompression
                + ", loadJobPollInitialBackoff="
                + loadJobPollInitialBackoff
                + ", loadJobPollMaxBackoff="
                + loadJobPollMaxBackoff
                + ", schemaReconcileInitialBackoff="
                + schemaReconcileInitialBackoff
                + ", schemaReconcileMaxBackoff="
                + schemaReconcileMaxBackoff
                + ", schemaReconcileMaxAttempts="
                + schemaReconcileMaxAttempts
                + ", perDestinationMetrics="
                + perDestinationMetrics
                + "}";
    }

    /** Builder for {@link FileLoadsOptions}. */
    @Public
    public static final class Builder {

        private String stagingPath;
        @Nullable private String tempDataset;
        private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
        private Duration minCheckpointInterval = DEFAULT_MIN_CHECKPOINT_INTERVAL;
        private long maxStagingFileBytes = DEFAULT_MAX_STAGING_FILE_BYTES;
        private StagingFormat stagingFormat = DEFAULT_STAGING_FORMAT;
        // Null until set, so build() can tell "explicitly chose the default" from "never
        // touched it" and reject the option under Avro rather than silently ignoring it.
        @Nullable private ParquetCompression parquetCompression;
        private Duration loadJobPollInitialBackoff = DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF;
        private Duration loadJobPollMaxBackoff = DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF;
        private Duration schemaReconcileInitialBackoff = DEFAULT_SCHEMA_RECONCILE_INITIAL_BACKOFF;
        private Duration schemaReconcileMaxBackoff = DEFAULT_SCHEMA_RECONCILE_MAX_BACKOFF;
        private int schemaReconcileMaxAttempts = DEFAULT_SCHEMA_RECONCILE_MAX_ATTEMPTS;
        private boolean perDestinationMetrics;

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
         * per-load-job limits or replacement rows span staging formats. Rows go through leaf
         * tables, optional intermediate copy levels, and one final copy or query. Optional;
         * defaults to each destination table's own dataset. The temporary and final datasets must
         * share a BigQuery location. A dedicated dataset with a default table expiration is
         * recommended so temporary tables orphaned by hard failures are garbage-collected.
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
         * {@link WriteDisposition#WRITE_APPEND}. Non-append dispositions are batch-only. {@link
         * WriteDisposition#WRITE_TRUNCATE_DATA} preserves the destination table's schema and
         * constraints, while {@link WriteDisposition#WRITE_TRUNCATE} replaces its schema.
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
         * jobs whose daily destination-table modification count stays safe despite fast checkpoints
         * (e.g. short-lived streaming jobs): each checkpoint issues a direct load or, on overflow,
         * one final copy per destination. Ignored in batch execution.
         *
         * @param minCheckpointInterval the smallest accepted checkpoint interval
         * @return this builder
         */
        public Builder minCheckpointInterval(Duration minCheckpointInterval) {
            OptionChecks.checkPositive(minCheckpointInterval, "minCheckpointInterval");
            this.minCheckpointInterval = minCheckpointInterval;
            return this;
        }

        /**
         * Sets the size at which an open staging file is finished and the next one opened. Defaults
         * to {@link FileLoadsOptions#DEFAULT_MAX_STAGING_FILE_BYTES}, whose javadoc carries the
         * measurement the value comes from.
         *
         * <p>Worth setting only where the default's trade-off does not fit the deployment, and the
         * two directions are not symmetric. <b>Raise it</b> for a job writing a very large volume
         * to a single destination, which the 10,000-URI cap would otherwise push onto the
         * temporary-table plus copy path — the cap is a file count, so the ceiling moves with this
         * value. <b>Lowering it</b> buys little: the measured floor is around 8 MiB and load time
         * climbs steeply below it.
         *
         * <p>At high parallelism this knob does nothing at all, since a checkpoint's data divided
         * by the subtask count already produces smaller files than any sensible threshold.
         *
         * @param maxStagingFileBytes the roll threshold in bytes, positive
         * @return this builder
         */
        public Builder maxStagingFileBytes(long maxStagingFileBytes) {
            Preconditions.checkArgument(
                    maxStagingFileBytes > 0,
                    "maxStagingFileBytes must be positive: %s",
                    maxStagingFileBytes);
            this.maxStagingFileBytes = maxStagingFileBytes;
            return this;
        }

        /**
         * Sets the format staging files are written in. Defaults to {@link StagingFormat#AVRO},
         * which is the recommended value; see {@link StagingFormat#PARQUET} for what choosing the
         * other one costs.
         *
         * <p>Selecting {@code PARQUET} requires {@code org.apache.parquet:parquet-avro} on the
         * runtime classpath, and — unless {@link ParquetCompression#NONE} is also selected — a
         * Hadoop runtime, neither of which this connector ships. Both are checked here, on the
         * client, so a missing dependency fails when the job graph is built rather than on a
         * TaskManager when the first staging file is opened. A client whose classpath differs from
         * the cluster's can still defeat that, which is why the docs name the artifacts.
         *
         * @param stagingFormat the staging format
         * @return this builder
         */
        public Builder stagingFormat(StagingFormat stagingFormat) {
            this.stagingFormat =
                    Preconditions.checkNotNull(stagingFormat, "stagingFormat must not be null");
            return this;
        }

        /**
         * Sets how {@link StagingFormat#PARQUET} staging files are compressed. Defaults to {@link
         * ParquetCompression#ZSTD}. Rejected when the staging format is {@link StagingFormat#AVRO},
         * whose codec is not configurable — an ignored option is worse than a rejected one.
         *
         * @param parquetCompression the Parquet codec
         * @return this builder
         */
        public Builder parquetCompression(ParquetCompression parquetCompression) {
            this.parquetCompression =
                    Preconditions.checkNotNull(
                            parquetCompression, "parquetCompression must not be null");
            return this;
        }

        /**
         * Sets the first backoff between polls of a submitted load, copy, or query job's
         * completion. Defaults to {@link FileLoadsOptions#DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF}.
         * Lowering it notices a finished load sooner at the cost of more {@code jobs.get} calls;
         * raising it does the reverse. There is no attempt cap to configure — see the class
         * javadoc.
         *
         * @param loadJobPollInitialBackoff the first poll backoff, at least 1 ms
         * @return this builder
         */
        public Builder loadJobPollInitialBackoff(Duration loadJobPollInitialBackoff) {
            this.loadJobPollInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            loadJobPollInitialBackoff, "loadJobPollInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff between polls of a submitted load, copy, or query job's completion. Must
         * be at least the initial backoff. Defaults to {@link
         * FileLoadsOptions#DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF}.
         *
         * @param loadJobPollMaxBackoff the poll backoff cap, at least 1 ms
         * @return this builder
         */
        public Builder loadJobPollMaxBackoff(Duration loadJobPollMaxBackoff) {
            this.loadJobPollMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            loadJobPollMaxBackoff, "loadJobPollMaxBackoff");
            return this;
        }

        /**
         * Sets the first backoff after losing an etag race while reconciling a destination table's
         * schema. Defaults to {@link FileLoadsOptions#DEFAULT_SCHEMA_RECONCILE_INITIAL_BACKOFF}.
         *
         * @param schemaReconcileInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder schemaReconcileInitialBackoff(Duration schemaReconcileInitialBackoff) {
            this.schemaReconcileInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            schemaReconcileInitialBackoff, "schemaReconcileInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the schema-reconcile budget. Must be at least the initial backoff.
         * Defaults to {@link FileLoadsOptions#DEFAULT_SCHEMA_RECONCILE_MAX_BACKOFF}.
         *
         * @param schemaReconcileMaxBackoff the backoff cap, at least 1 ms
         * @return this builder
         */
        public Builder schemaReconcileMaxBackoff(Duration schemaReconcileMaxBackoff) {
            this.schemaReconcileMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            schemaReconcileMaxBackoff, "schemaReconcileMaxBackoff");
            return this;
        }

        /**
         * Caps the attempts at reconciling a destination table's schema. Defaults to {@link
         * FileLoadsOptions#DEFAULT_SCHEMA_RECONCILE_MAX_ATTEMPTS}. Each attempt is a fresh read,
         * union and etag-conditioned update, so only lost races consume attempts — raise it when
         * something outside this job updates the same table concurrently.
         *
         * @param schemaReconcileMaxAttempts the attempt cap, positive
         * @return this builder
         */
        public Builder schemaReconcileMaxAttempts(int schemaReconcileMaxAttempts) {
            Preconditions.checkArgument(
                    schemaReconcileMaxAttempts > 0,
                    "schemaReconcileMaxAttempts must be positive: %s",
                    schemaReconcileMaxAttempts);
            this.schemaReconcileMaxAttempts = schemaReconcileMaxAttempts;
            return this;
        }

        /**
         * Registers per-table {@code recordsSend} and {@code sendErrors} counters beside the
         * writer's totals. Defaults to {@code false}.
         *
         * <p>Off by default because Flink cannot unregister a metric: with a per-record {@code
         * destinationResolver} the table set is unbounded — a table per day, a table per tenant —
         * so every table the job ever writes to keeps a row in the metric registry for the lifetime
         * of the task. Switch it on for a sink whose destinations are few and known.
         *
         * @param perDestinationMetrics whether to register per-table counters
         * @return this builder
         */
        public Builder perDestinationMetrics(boolean perDestinationMetrics) {
            this.perDestinationMetrics = perDestinationMetrics;
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
            Preconditions.checkState(
                    loadJobPollMaxBackoff.compareTo(loadJobPollInitialBackoff) >= 0,
                    "loadJobPollMaxBackoff must be >= loadJobPollInitialBackoff: %s < %s",
                    loadJobPollMaxBackoff,
                    loadJobPollInitialBackoff);
            Preconditions.checkState(
                    stagingFormat == StagingFormat.PARQUET || parquetCompression == null,
                    "parquetCompression is only accepted with stagingFormat(PARQUET): the Avro"
                            + " staging codec is not configurable.");
            if (stagingFormat == StagingFormat.PARQUET) {
                checkParquetOnClasspath(
                        parquetCompression == null
                                ? DEFAULT_PARQUET_COMPRESSION
                                : parquetCompression);
            }
            Preconditions.checkState(
                    schemaReconcileMaxBackoff.compareTo(schemaReconcileInitialBackoff) >= 0,
                    "schemaReconcileMaxBackoff must be >= schemaReconcileInitialBackoff: %s < %s",
                    schemaReconcileMaxBackoff,
                    schemaReconcileInitialBackoff);
            return new FileLoadsOptions(this);
        }

        /**
         * Fails at graph construction, with the artifact to add, rather than on a TaskManager with
         * a {@code NoClassDefFoundError} the first time a staging file is opened.
         *
         * <p>Two probes, not one: compressed Parquet also needs Hadoop, because every codec in
         * {@code parquet-hadoop} is resolved through Hadoop's {@code CompressionCodec} SPI — so a
         * classpath with parquet and no Hadoop works for {@link ParquetCompression#NONE} and fails
         * for anything else, and the message has to say which of the two is missing.
         */
        private static void checkParquetOnClasspath(ParquetCompression compression) {
            checkClass(
                    "org.apache.parquet.avro.AvroParquetWriter",
                    "stagingFormat(PARQUET) needs org.apache.parquet:parquet-avro on the runtime"
                            + " classpath; this connector does not ship it.");
            if (compression != ParquetCompression.NONE) {
                checkClass(
                        "org.apache.hadoop.conf.Configuration",
                        "stagingFormat(PARQUET) with parquetCompression("
                                + compression.name()
                                + ") needs a Hadoop runtime (org.apache.hadoop:hadoop-common and"
                                + " its dependencies) on the runtime classpath: every Parquet codec"
                                + " is resolved through Hadoop's CompressionCodec SPI. Use"
                                + " parquetCompression(NONE) to stage Parquet without Hadoop, at"
                                + " the cost of substantially larger staging files.");
            }
        }

        private static void checkClass(String className, String message) {
            try {
                Class.forName(className, false, FileLoadsOptions.class.getClassLoader());
            } catch (ClassNotFoundException | LinkageError e) {
                throw new IllegalStateException(message, e);
            }
        }
    }
}
