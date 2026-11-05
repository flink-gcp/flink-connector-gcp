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

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.options.ResourceNames;
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
 * table's schema. Those races do <b>not</b> come from this job's destination parallelism: the
 * committer runs one task per distinct table, so the same table is never reconciled by two of its
 * workers. They come from anything else updating the same table at the same time: a second Flink
 * job, a Storage Write API sink writing the same destination, or external tooling.
 *
 * <p>Set via {@link BigQuerySinkBuilder#fileLoadsOptions(FileLoadsOptions)}; required when building
 * a {@code FILE_LOADS} sink and rejected for every other write method.
 *
 * <p>The staging path should point at a bucket dedicated to load staging — separate from
 * checkpoint/savepoint storage — with a lifecycle rule that expires stale objects. Staged files are
 * deleted best-effort only after every destination load and temporary-table cleanup complete
 * normally. They are deliberately kept after a load failure or cleanup interruption so a Flink
 * restart can retry deterministically, so orphaned objects can accumulate without a lifecycle rule.
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

    /** Default for {@link Builder#maxOpenDestinations(int)}. */
    public static final int DEFAULT_MAX_OPEN_DESTINATIONS = 16;

    /** Default for {@link Builder#maxPendingFiles(int)}. */
    public static final int DEFAULT_MAX_PENDING_FILES = 10_000;

    /** Default for {@link Builder#maxConcurrentCheckpointFinalizations(int)}: serial close. */
    public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS = 1;

    private static final int MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS = 8;

    /**
     * Default for {@link Builder#stagingUploadChunkBytes(long)}: 4 MiB, the upload chunk every open
     * destination file holds, chosen to bound writer memory under many open destinations.
     */
    public static final long DEFAULT_STAGING_UPLOAD_CHUNK_BYTES = 4L * 1024 * 1024;

    /** The granularity Cloud Storage resumable uploads require of every chunk but the last. */
    private static final long STAGING_UPLOAD_CHUNK_GRANULARITY = 256L * 1024;

    /** The largest chunk the storage client's {@code int} chunk size can express. */
    private static final long MAX_STAGING_UPLOAD_CHUNK_BYTES =
            Integer.MAX_VALUE / STAGING_UPLOAD_CHUNK_GRANULARITY * STAGING_UPLOAD_CHUNK_GRANULARITY;

    /**
     * Default for {@link Builder#backgroundStagingUploadEnabled(boolean)}: off, so uploads run on
     * the task thread.
     */
    public static final boolean DEFAULT_BACKGROUND_STAGING_UPLOAD_ENABLED = false;

    /**
     * Default for {@link Builder#maxInFlightUploadOperations(int)}: 2, the smallest value that
     * measured as fast as 4, with the task thread waiting for a permit a few percent of the time at
     * most against 9-14% at 1 (ADR-0195).
     */
    public static final int DEFAULT_MAX_IN_FLIGHT_UPLOAD_OPERATIONS = 2;

    /** Default for {@link Builder#maxConcurrentDestinations(int)}. */
    public static final int DEFAULT_MAX_CONCURRENT_DESTINATIONS = 8;

    private static final int MAX_CONCURRENT_DESTINATIONS = 64;

    /** Default for {@link Builder#destinationIdleTimeout(Duration)}. */
    public static final Duration DEFAULT_DESTINATION_IDLE_TIMEOUT = Duration.ofMinutes(1);

    /** Default for {@link Builder#tempTableExpiration(Duration)}. */
    public static final Duration DEFAULT_TEMP_TABLE_EXPIRATION = Duration.ofDays(1);

    private static final Duration MIN_TEMP_TABLE_EXPIRATION = Duration.ofHours(1);

    /**
     * Default for {@link Builder#maxSerializedRowBytes(long)}.
     *
     * <p>The limit is applied to the serialized protobuf before it is converted to Avro. The
     * 15,000,000-byte default leaves headroom below BigQuery's 16,000,000-byte Avro block limit,
     * but protobuf and Avro encodings are not byte-for-byte equivalent; schemas whose Avro
     * representation expands substantially may need a lower value.
     */
    public static final long DEFAULT_MAX_SERIALIZED_ROW_BYTES = 15_000_000L;

    /** Default for {@link Builder#stagingFormat(StagingFormat)}. */
    public static final StagingFormat DEFAULT_STAGING_FORMAT = StagingFormat.AVRO;

    /** Default for {@link Builder#parquetCompression(ParquetCompression)}. */
    public static final ParquetCompression DEFAULT_PARQUET_COMPRESSION = ParquetCompression.ZSTD;

    /** Default for {@link Builder#avroCompression(AvroCompression)}. */
    public static final AvroCompression DEFAULT_AVRO_COMPRESSION = AvroCompression.ZSTD;

    /** Default for {@link Builder#zstdLevel(int)}: zstandard's own default level. */
    public static final int DEFAULT_ZSTD_LEVEL = 3;

    /** The lowest level {@link Builder#zstdLevel(int)} accepts: zstandard's fastest. */
    static final int MIN_ZSTD_LEVEL = -(1 << 17);

    /**
     * The highest level {@link Builder#zstdLevel(int)} accepts: 19, the highest zstandard offers
     * without its "ultra" opt-in. Both codecs build a fresh compression context for every Avro
     * block or Parquet page, and zstandard estimates that context at about 90 MiB of native memory
     * at 19 against 194 MiB at 20 and 778 MiB at 22, outside Flink's memory budget.
     */
    static final int MAX_ZSTD_LEVEL = 19;

    private final String stagingPath;
    @Nullable private final String tempDataset;
    // Null in an instance serialized before the option existed; the getter supplies the default.
    @Nullable private final Duration tempTableExpiration;
    private final WriteDisposition writeDisposition;
    private final Duration minCheckpointInterval;
    private final long maxStagingFileBytes;
    private final int maxOpenDestinations;
    private final int maxPendingFiles;
    private final int maxConcurrentCheckpointFinalizations;
    // Zero in an instance serialized before the option existed; the getter supplies the default.
    private final long stagingUploadChunkBytes;
    private final boolean backgroundStagingUploadEnabled;
    // Zero in an instance serialized before the option existed; the getter supplies the default.
    private final int maxInFlightUploadOperations;
    private final int maxConcurrentDestinations;
    @Nullable private final Duration destinationIdleTimeout;
    private final long maxSerializedRowBytes;
    private final StagingFormat stagingFormat;
    private final ParquetCompression parquetCompression;
    // Null in an instance serialized before the option existed; the getter supplies the default.
    @Nullable private final AvroCompression avroCompression;
    // Null in an instance serialized before the option existed; the getter supplies the default.
    @Nullable private final Integer zstdLevel;
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
        this.tempTableExpiration = builder.tempTableExpiration;
        this.writeDisposition = builder.writeDisposition;
        this.minCheckpointInterval = builder.minCheckpointInterval;
        this.maxStagingFileBytes = builder.maxStagingFileBytes;
        this.maxOpenDestinations = builder.maxOpenDestinations;
        this.maxPendingFiles = builder.maxPendingFiles;
        this.maxConcurrentCheckpointFinalizations = builder.maxConcurrentCheckpointFinalizations;
        this.stagingUploadChunkBytes = builder.stagingUploadChunkBytes;
        this.backgroundStagingUploadEnabled = builder.backgroundStagingUploadEnabled;
        // Zero when unset, like an instance serialized before the option existed: the getter
        // supplies the default for both.
        this.maxInFlightUploadOperations =
                builder.maxInFlightUploadOperations == null
                        ? 0
                        : builder.maxInFlightUploadOperations;
        this.maxConcurrentDestinations = builder.maxConcurrentDestinations;
        this.destinationIdleTimeout = builder.destinationIdleTimeout;
        this.maxSerializedRowBytes = builder.maxSerializedRowBytes;
        this.stagingFormat = builder.stagingFormat;
        this.parquetCompression =
                builder.parquetCompression == null
                        ? DEFAULT_PARQUET_COMPRESSION
                        : builder.parquetCompression;
        this.avroCompression =
                builder.avroCompression == null
                        ? DEFAULT_AVRO_COMPRESSION
                        : builder.avroCompression;
        this.zstdLevel = builder.zstdLevel == null ? DEFAULT_ZSTD_LEVEL : builder.zstdLevel;
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

    /**
     * Returns how long a temporary table laid out for a column- or range-partitioned or clustered
     * destination lives after a commit attempt creates it or extends it.
     */
    public Duration getTempTableExpiration() {
        return tempTableExpiration == null ? DEFAULT_TEMP_TABLE_EXPIRATION : tempTableExpiration;
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

    /** Returns the maximum number of destination files held open by each writer subtask. */
    public int getMaxOpenDestinations() {
        return maxOpenDestinations == 0 ? DEFAULT_MAX_OPEN_DESTINATIONS : maxOpenDestinations;
    }

    /** Returns the maximum staging files retained for the next commit by each writer subtask. */
    public int getMaxPendingFiles() {
        return maxPendingFiles == 0 ? DEFAULT_MAX_PENDING_FILES : maxPendingFiles;
    }

    /** Returns how many open files each writer may finalize concurrently at a checkpoint. */
    public int getMaxConcurrentCheckpointFinalizations() {
        return maxConcurrentCheckpointFinalizations == 0
                ? DEFAULT_MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS
                : maxConcurrentCheckpointFinalizations;
    }

    /** Returns the size of the upload chunk each open staging file buffers. */
    public long getStagingUploadChunkBytes() {
        return stagingUploadChunkBytes == 0
                ? DEFAULT_STAGING_UPLOAD_CHUNK_BYTES
                : stagingUploadChunkBytes;
    }

    /** Returns whether each writer uploads its staging files on a background staging uploader. */
    public boolean isBackgroundStagingUploadEnabled() {
        return backgroundStagingUploadEnabled;
    }

    /**
     * Returns how many upload operations each writer's background staging uploader may hold, queued
     * or running. Meaningful only while {@link #isBackgroundStagingUploadEnabled()}.
     */
    public int getMaxInFlightUploadOperations() {
        return maxInFlightUploadOperations == 0
                ? DEFAULT_MAX_IN_FLIGHT_UPLOAD_OPERATIONS
                : maxInFlightUploadOperations;
    }

    /** Returns the maximum destination actions this committer executes concurrently. */
    public int getMaxConcurrentDestinations() {
        return maxConcurrentDestinations == 0
                ? DEFAULT_MAX_CONCURRENT_DESTINATIONS
                : maxConcurrentDestinations;
    }

    /** Returns how long an inactive destination file remains open. */
    public Duration getDestinationIdleTimeout() {
        return destinationIdleTimeout == null
                ? DEFAULT_DESTINATION_IDLE_TIMEOUT
                : destinationIdleTimeout;
    }

    /** Returns the largest serialized protobuf row accepted by the staging writer. */
    public long getMaxSerializedRowBytes() {
        return maxSerializedRowBytes == 0
                ? DEFAULT_MAX_SERIALIZED_ROW_BYTES
                : maxSerializedRowBytes;
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

    /**
     * Returns how Avro staging files are compressed, including the Avro files a Parquet sink stages
     * for a destination with a {@code JSON} column, which are always zstandard.
     */
    public AvroCompression getAvroCompression() {
        return avroCompression == null ? DEFAULT_AVRO_COMPRESSION : avroCompression;
    }

    /** Returns the zstandard level of every staging file written with zstandard. */
    public int getZstdLevel() {
        return zstdLevel == null ? DEFAULT_ZSTD_LEVEL : zstdLevel;
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
    public Duration getSchemaReconcileInitialBackoff() {
        return schemaReconcileInitialBackoff;
    }

    /** Returns the backoff cap of the schema-reconcile budget. */
    public Duration getSchemaReconcileMaxBackoff() {
        return schemaReconcileMaxBackoff;
    }

    /** Returns the maximum number of attempts of the schema-reconcile budget. */
    public int getSchemaReconcileMaxAttempts() {
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
                && getTempTableExpiration().equals(that.getTempTableExpiration())
                && writeDisposition == that.writeDisposition
                && minCheckpointInterval.equals(that.minCheckpointInterval)
                && maxStagingFileBytes == that.maxStagingFileBytes
                && getMaxOpenDestinations() == that.getMaxOpenDestinations()
                && getMaxPendingFiles() == that.getMaxPendingFiles()
                && getMaxConcurrentCheckpointFinalizations()
                        == that.getMaxConcurrentCheckpointFinalizations()
                && getStagingUploadChunkBytes() == that.getStagingUploadChunkBytes()
                && backgroundStagingUploadEnabled == that.backgroundStagingUploadEnabled
                && getMaxInFlightUploadOperations() == that.getMaxInFlightUploadOperations()
                && getMaxConcurrentDestinations() == that.getMaxConcurrentDestinations()
                && getDestinationIdleTimeout().equals(that.getDestinationIdleTimeout())
                && getMaxSerializedRowBytes() == that.getMaxSerializedRowBytes()
                && stagingFormat == that.stagingFormat
                && parquetCompression == that.parquetCompression
                && getAvroCompression() == that.getAvroCompression()
                && getZstdLevel() == that.getZstdLevel()
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
                getTempTableExpiration(),
                writeDisposition,
                minCheckpointInterval,
                maxStagingFileBytes,
                getMaxOpenDestinations(),
                getMaxPendingFiles(),
                getMaxConcurrentCheckpointFinalizations(),
                getStagingUploadChunkBytes(),
                backgroundStagingUploadEnabled,
                getMaxInFlightUploadOperations(),
                getMaxConcurrentDestinations(),
                getDestinationIdleTimeout(),
                getMaxSerializedRowBytes(),
                stagingFormat,
                parquetCompression,
                getAvroCompression(),
                getZstdLevel(),
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
                + ", tempTableExpiration="
                + getTempTableExpiration()
                + ", writeDisposition="
                + writeDisposition
                + ", minCheckpointInterval="
                + minCheckpointInterval
                + ", maxStagingFileBytes="
                + maxStagingFileBytes
                + ", maxOpenDestinations="
                + getMaxOpenDestinations()
                + ", maxPendingFiles="
                + getMaxPendingFiles()
                + ", maxConcurrentCheckpointFinalizations="
                + getMaxConcurrentCheckpointFinalizations()
                + ", stagingUploadChunkBytes="
                + getStagingUploadChunkBytes()
                + ", backgroundStagingUploadEnabled="
                + backgroundStagingUploadEnabled
                + ", maxInFlightUploadOperations="
                + getMaxInFlightUploadOperations()
                + ", maxConcurrentDestinations="
                + getMaxConcurrentDestinations()
                + ", destinationIdleTimeout="
                + getDestinationIdleTimeout()
                + ", maxSerializedRowBytes="
                + getMaxSerializedRowBytes()
                + ", stagingFormat="
                + stagingFormat
                + ", parquetCompression="
                + parquetCompression
                + ", avroCompression="
                + getAvroCompression()
                + ", zstdLevel="
                + getZstdLevel()
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
        private Duration tempTableExpiration = DEFAULT_TEMP_TABLE_EXPIRATION;
        private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
        private Duration minCheckpointInterval = DEFAULT_MIN_CHECKPOINT_INTERVAL;
        private long maxStagingFileBytes = DEFAULT_MAX_STAGING_FILE_BYTES;
        private int maxOpenDestinations = DEFAULT_MAX_OPEN_DESTINATIONS;
        private int maxPendingFiles = DEFAULT_MAX_PENDING_FILES;
        private int maxConcurrentCheckpointFinalizations =
                DEFAULT_MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS;
        private long stagingUploadChunkBytes = DEFAULT_STAGING_UPLOAD_CHUNK_BYTES;
        private boolean backgroundStagingUploadEnabled = DEFAULT_BACKGROUND_STAGING_UPLOAD_ENABLED;
        // Null until set, so build() can reject a depth given while the uploader is off rather
        // than silently ignoring it.
        @Nullable private Integer maxInFlightUploadOperations;
        private int maxConcurrentDestinations = DEFAULT_MAX_CONCURRENT_DESTINATIONS;
        private Duration destinationIdleTimeout = DEFAULT_DESTINATION_IDLE_TIMEOUT;
        private long maxSerializedRowBytes = DEFAULT_MAX_SERIALIZED_ROW_BYTES;
        private StagingFormat stagingFormat = DEFAULT_STAGING_FORMAT;
        // Null until set, so build() can tell "explicitly chose the default" from "never
        // touched it" and reject the option under Avro rather than silently ignoring it.
        @Nullable private ParquetCompression parquetCompression;
        // Null until set, for the same reason: rejected under Parquet rather than ignored.
        @Nullable private AvroCompression avroCompression;
        // Null until set, so build() can reject a level for a codec that has none.
        @Nullable private Integer zstdLevel;
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
         * recommended so temporary tables orphaned by hard failures are garbage-collected; those
         * laid out for a column- or range-partitioned or clustered destination carry their own
         * expiration instead ({@link #tempTableExpiration(Duration)}).
         *
         * @param tempDataset the dataset id, in the same project as the destination table
         * @return this builder
         */
        public Builder tempDataset(String tempDataset) {
            this.tempDataset = ResourceNames.checkComponent(tempDataset, "tempDataset");
            return this;
        }

        /**
         * Sets how long a temporary table laid out for a column- or range-partitioned or clustered
         * destination lives after a commit attempt creates it or extends it. Such a table takes its
         * destination's partitioning and clustering so the final copy is accepted, and is created
         * before the job that fills it, because a partitioned table may not take its dataset's
         * default table expiration. Every attempt of the commit extends it again, and a table that
         * expired before a retry is created and filled anew from the staged files. Temporary tables
         * that are not laid out, and those of {@code WRITE_TRUNCATE_DATA}, are left to the
         * dataset's default table expiration. Defaults to {@link
         * FileLoadsOptions#DEFAULT_TEMP_TABLE_EXPIRATION}.
         *
         * @param tempTableExpiration the expiration, at least one hour
         * @return this builder
         */
        public Builder tempTableExpiration(Duration tempTableExpiration) {
            Preconditions.checkNotNull(tempTableExpiration, "tempTableExpiration must not be null");
            Preconditions.checkArgument(
                    tempTableExpiration.compareTo(MIN_TEMP_TABLE_EXPIRATION) >= 0,
                    "tempTableExpiration must be at least 1 hour: %s",
                    tempTableExpiration);
            this.tempTableExpiration =
                    OptionChecks.checkExpressibleInNanos(
                            tempTableExpiration, "tempTableExpiration");
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
         * Sets the maximum number of destination files held open by each writer subtask. When a new
         * destination would exceed the limit, the least recently used file is finished before the
         * new one is opened. Defaults to {@link FileLoadsOptions#DEFAULT_MAX_OPEN_DESTINATIONS}.
         *
         * @param maxOpenDestinations the per-subtask active-destination limit, positive
         * @return this builder
         */
        public Builder maxOpenDestinations(int maxOpenDestinations) {
            Preconditions.checkArgument(
                    maxOpenDestinations > 0,
                    "maxOpenDestinations must be positive: %s",
                    maxOpenDestinations);
            this.maxOpenDestinations = maxOpenDestinations;
            return this;
        }

        /**
         * Sets the maximum number of staging files each writer subtask may retain for the next
         * commit, including files that are still open. Reaching the limit fails the task before
         * opening another file, rather than allowing destination churn or size-based rolls to grow
         * heap without bound. The limit must be at least {@code maxOpenDestinations}. Defaults to
         * {@link FileLoadsOptions#DEFAULT_MAX_PENDING_FILES}.
         *
         * @param maxPendingFiles the per-subtask pending-file limit, positive and at least the
         *     configured maximum open destinations
         * @return this builder
         */
        public Builder maxPendingFiles(int maxPendingFiles) {
            Preconditions.checkArgument(
                    maxPendingFiles > 0, "maxPendingFiles must be positive: %s", maxPendingFiles);
            this.maxPendingFiles = maxPendingFiles;
            return this;
        }

        /**
         * Sets the maximum number of open staging files this writer subtask finalizes concurrently
         * when a checkpoint or end of input calls {@code prepareCommit()}. Defaults to {@link
         * FileLoadsOptions#DEFAULT_MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS}. Values above 8 are
         * rejected: eight was the smallest measured bound to improve both 10- and 50-destination 5
         * MiB workloads by at least 25%, while a 16-way large-file arm did not complete.
         *
         * <p>This option does not parallelize appends, size-based rolls, capacity evictions, or
         * idle closes. Each worker closes one independent Cloud Storage upload, adding a thread and
         * a concurrent finalization request but no queued row buffer. It is useful when several
         * destinations reach one writer and sink parallelism cannot be raised; one open file has
         * nothing to parallelize.
         *
         * @param maxConcurrentCheckpointFinalizations the per-writer bound, from 1 through 8
         * @return this builder
         */
        public Builder maxConcurrentCheckpointFinalizations(
                int maxConcurrentCheckpointFinalizations) {
            Preconditions.checkArgument(
                    maxConcurrentCheckpointFinalizations > 0,
                    "maxConcurrentCheckpointFinalizations must be positive: %s",
                    maxConcurrentCheckpointFinalizations);
            Preconditions.checkArgument(
                    maxConcurrentCheckpointFinalizations <= MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS,
                    "maxConcurrentCheckpointFinalizations must be at most %s: %s",
                    MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS,
                    maxConcurrentCheckpointFinalizations);
            this.maxConcurrentCheckpointFinalizations = maxConcurrentCheckpointFinalizations;
            return this;
        }

        /**
         * Sets the size of the Cloud Storage upload chunk each open staging file buffers before
         * sending it. Every open destination file holds one chunk, so a writer's upload buffers
         * grow with this value times {@code maxOpenDestinations}. Defaults to {@link
         * FileLoadsOptions#DEFAULT_STAGING_UPLOAD_CHUNK_BYTES}.
         *
         * <p>A larger chunk sends a file in fewer resumable-upload requests, which saves their
         * round trips but not the time spent transferring bytes. A chunk larger than every file the
         * writer stages defers the whole upload to the file's close; matching {@code
         * maxStagingFileBytes} is not enough for Avro, whose file finishes only once its bytes
         * reach that size, a block later, while Parquet usually closes within about half a row
         * group of it when its row groups are of similar size.
         *
         * @param stagingUploadChunkBytes the chunk size in bytes, a positive multiple of 256 KiB
         *     that the storage client's {@code int} chunk size can express
         * @return this builder
         */
        public Builder stagingUploadChunkBytes(long stagingUploadChunkBytes) {
            Preconditions.checkArgument(
                    stagingUploadChunkBytes > 0
                            && stagingUploadChunkBytes % STAGING_UPLOAD_CHUNK_GRANULARITY == 0,
                    "stagingUploadChunkBytes must be a positive multiple of %s: %s",
                    STAGING_UPLOAD_CHUNK_GRANULARITY,
                    stagingUploadChunkBytes);
            Preconditions.checkArgument(
                    stagingUploadChunkBytes <= MAX_STAGING_UPLOAD_CHUNK_BYTES,
                    "stagingUploadChunkBytes must be at most %s: %s",
                    MAX_STAGING_UPLOAD_CHUNK_BYTES,
                    stagingUploadChunkBytes);
            this.stagingUploadChunkBytes = stagingUploadChunkBytes;
            return this;
        }

        /**
         * Sets whether each writer subtask uploads its staging files on a background staging
         * uploader. Defaults to {@link FileLoadsOptions#DEFAULT_BACKGROUND_STAGING_UPLOAD_ENABLED},
         * which sends every chunk and finalizes every size-rolled, evicted, or idle file on the
         * task thread.
         *
         * <p>Enabled, each writer starts one upload thread, which runs upload operations in the
         * order the task thread submits them while the task thread keeps encoding into the next
         * chunk. An upload operation is one full upload chunk, or the close of a size-rolled,
         * evicted, or idle file together with its last chunk, if any. A checkpoint or end of input
         * waits for the uploader to drain before it finalizes the remaining open files, which
         * {@code maxConcurrentCheckpointFinalizations} still governs. The uploader moves the upload
         * wait, not encoding or compression, off the task thread.
         *
         * <p>It helps most where data crosses upload chunks or rolls files between checkpoints. A
         * writer with many destinations, whose files mostly upload at the checkpoint, gains from it
         * only together with {@code maxConcurrentCheckpointFinalizations}.
         *
         * @param backgroundStagingUploadEnabled whether to start the background staging uploader
         * @return this builder
         */
        public Builder backgroundStagingUploadEnabled(boolean backgroundStagingUploadEnabled) {
            this.backgroundStagingUploadEnabled = backgroundStagingUploadEnabled;
            return this;
        }

        /**
         * Sets how many upload operations each writer subtask's background staging uploader may
         * hold, queued or running. Accepted only with {@link
         * #backgroundStagingUploadEnabled(boolean)} on. Defaults to {@link
         * FileLoadsOptions#DEFAULT_MAX_IN_FLIGHT_UPLOAD_OPERATIONS}.
         *
         * <p>Once this many operations are queued or running, the task thread blocks until one
         * completes. One already lets encoding overlap one upload but measured the task thread
         * waiting for the permit; a larger value only lets more work queue, since the uploader
         * never uploads in parallel. The uploader adds at most this many chunks of upload payload
         * per writer, independent of the number of open destinations; the open files' own chunks
         * are separate. No upper bound is imposed, because the value adds no thread or client, only
         * that heap.
         *
         * @param maxInFlightUploadOperations the per-writer operation bound, positive
         * @return this builder
         */
        public Builder maxInFlightUploadOperations(int maxInFlightUploadOperations) {
            Preconditions.checkArgument(
                    maxInFlightUploadOperations > 0,
                    "maxInFlightUploadOperations must be positive: %s",
                    maxInFlightUploadOperations);
            this.maxInFlightUploadOperations = maxInFlightUploadOperations;
            return this;
        }

        /**
         * Sets the maximum number of destination actions this committer executes concurrently. Each
         * pooled worker owns one thread and one BigQuery REST client. Before the pool is
         * initialized, a single-destination phase instead runs on the caller; later singleton
         * phases reuse the pool. Defaults to {@link
         * FileLoadsOptions#DEFAULT_MAX_CONCURRENT_DESTINATIONS}; values above 64 are rejected to
         * keep client, thread, and in-flight state bounded.
         *
         * @param maxConcurrentDestinations the committer-wide destination bound, from 1 through 64
         * @return this builder
         */
        public Builder maxConcurrentDestinations(int maxConcurrentDestinations) {
            Preconditions.checkArgument(
                    maxConcurrentDestinations > 0,
                    "maxConcurrentDestinations must be positive: %s",
                    maxConcurrentDestinations);
            Preconditions.checkArgument(
                    maxConcurrentDestinations <= MAX_CONCURRENT_DESTINATIONS,
                    "maxConcurrentDestinations must be at most %s: %s",
                    MAX_CONCURRENT_DESTINATIONS,
                    maxConcurrentDestinations);
            this.maxConcurrentDestinations = maxConcurrentDestinations;
            return this;
        }

        /**
         * Sets how long an inactive destination file remains open before it is finished and its
         * conversion state is released. Defaults to {@link
         * FileLoadsOptions#DEFAULT_DESTINATION_IDLE_TIMEOUT}.
         *
         * @param destinationIdleTimeout the idle timeout, at least 1 ms
         * @return this builder
         */
        public Builder destinationIdleTimeout(Duration destinationIdleTimeout) {
            this.destinationIdleTimeout =
                    OptionChecks.checkAtLeastOneMilli(
                            destinationIdleTimeout, "destinationIdleTimeout");
            return this;
        }

        /**
         * Sets the largest serialized protobuf row accepted by the staging writer. Larger rows are
         * routed to the configured failure handler before conversion state or a staging file is
         * created. Defaults to {@link FileLoadsOptions#DEFAULT_MAX_SERIALIZED_ROW_BYTES}.
         *
         * @param maxSerializedRowBytes the serialized row limit in bytes, positive
         * @return this builder
         */
        public Builder maxSerializedRowBytes(long maxSerializedRowBytes) {
            Preconditions.checkArgument(
                    maxSerializedRowBytes > 0,
                    "maxSerializedRowBytes must be positive: %s",
                    maxSerializedRowBytes);
            this.maxSerializedRowBytes = maxSerializedRowBytes;
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
         * whose codec {@link #avroCompression(AvroCompression)} sets — an ignored option is worse
         * than a rejected one.
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
         * Sets how {@link StagingFormat#AVRO} staging files are compressed. Defaults to {@link
         * AvroCompression#ZSTD}. Rejected when the staging format is {@link StagingFormat#PARQUET},
         * whose codec {@link #parquetCompression(ParquetCompression)} sets.
         *
         * <p>{@link AvroCompression#NONE} spends the least CPU on the writer's task thread and
         * stages the most bytes; the datastream page's tuning section gives the measured trade-off.
         *
         * @param avroCompression the Avro codec
         * @return this builder
         */
        public Builder avroCompression(AvroCompression avroCompression) {
            this.avroCompression =
                    Preconditions.checkNotNull(avroCompression, "avroCompression must not be null");
            return this;
        }

        /**
         * Sets the zstandard level of every staging file written with zstandard: Avro files under
         * {@link AvroCompression#ZSTD}, and Parquet files under {@link ParquetCompression#ZSTD}.
         * Defaults to {@link FileLoadsOptions#DEFAULT_ZSTD_LEVEL}. Rejected when the selected codec
         * is {@code NONE}, which has no level.
         *
         * <p>A negative level is one of zstandard's fast levels: less CPU and more bytes. A level
         * above the default buys little size for much more CPU, and each Avro block or Parquet page
         * then allocates a larger native compression context: zstandard estimates about 90 MiB at
         * 19, the highest level accepted. {@code 0} is rejected because zstandard reads it as its
         * default level, which {@code 3} states plainly.
         *
         * @param zstdLevel the level, from -131,072 to 19, not {@code 0}
         * @return this builder
         */
        public Builder zstdLevel(int zstdLevel) {
            Preconditions.checkArgument(
                    zstdLevel >= MIN_ZSTD_LEVEL && zstdLevel <= MAX_ZSTD_LEVEL && zstdLevel != 0,
                    "zstdLevel must be from %s to %s and not 0: %s",
                    MIN_ZSTD_LEVEL,
                    MAX_ZSTD_LEVEL,
                    zstdLevel);
            this.zstdLevel = zstdLevel;
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
                    maxPendingFiles >= maxOpenDestinations,
                    "maxPendingFiles must be >= maxOpenDestinations: %s < %s",
                    maxPendingFiles,
                    maxOpenDestinations);
            Preconditions.checkState(
                    loadJobPollMaxBackoff.compareTo(loadJobPollInitialBackoff) >= 0,
                    "loadJobPollMaxBackoff must be >= loadJobPollInitialBackoff: %s < %s",
                    loadJobPollMaxBackoff,
                    loadJobPollInitialBackoff);
            Preconditions.checkState(
                    backgroundStagingUploadEnabled || maxInFlightUploadOperations == null,
                    "maxInFlightUploadOperations is only accepted with"
                            + " backgroundStagingUploadEnabled(true): without the uploader nothing"
                            + " queues.");
            Preconditions.checkState(
                    stagingFormat == StagingFormat.PARQUET || parquetCompression == null,
                    "parquetCompression is only accepted with stagingFormat(PARQUET): use"
                            + " avroCompression(...) for the Avro staging codec.");
            Preconditions.checkState(
                    stagingFormat == StagingFormat.AVRO || avroCompression == null,
                    "avroCompression is only accepted with stagingFormat(AVRO): use"
                            + " parquetCompression(...) for the Parquet staging codec.");
            boolean uncompressed =
                    stagingFormat == StagingFormat.AVRO
                            ? avroCompression == AvroCompression.NONE
                            : parquetCompression == ParquetCompression.NONE;
            Preconditions.checkState(
                    zstdLevel == null || !uncompressed,
                    "zstdLevel is only accepted with a zstandard codec: the %s staging codec is"
                            + " NONE.",
                    stagingFormat.name());
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
