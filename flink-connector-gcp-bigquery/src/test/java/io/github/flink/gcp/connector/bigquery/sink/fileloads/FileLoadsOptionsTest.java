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

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsOptions}. */
class FileLoadsOptionsTest {

    @Test
    void defaultsAndRequiredStagingPath() {
        FileLoadsOptions options =
                FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build();

        assertThat(options.getStagingPath()).isEqualTo("gs://bucket/prefix");
        assertThat(options.getTempDataset()).isNull();
        assertThat(options.getWriteDisposition()).isEqualTo(WriteDisposition.WRITE_APPEND);
        assertThat(options.getMinCheckpointInterval()).isEqualTo(Duration.ofMinutes(2));
        // Asserted as a literal rather than against the constant, which would hold whatever the
        // constant became: this default is a measured value and moving it is a decision
        // (docs/adr/0070), so it should cost an edit here.
        assertThat(options.getMaxStagingFileBytes()).isEqualTo(16L * 1024 * 1024);
        assertThat(options.getMaxOpenDestinations()).isEqualTo(16);
        assertThat(options.getMaxPendingFiles()).isEqualTo(10_000);
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(options.getMaxSerializedRowBytes()).isEqualTo(15_000_000L);
        assertThat(options.getStagingFormat()).isEqualTo(StagingFormat.AVRO);
        assertThat(options.getParquetCompression()).isEqualTo(ParquetCompression.ZSTD);
        assertThat(options.getLoadJobPollInitialBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF);
        assertThat(options.getLoadJobPollMaxBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF);
        assertThat(options.getSchemaReconcileInitialBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_SCHEMA_RECONCILE_INITIAL_BACKOFF);
        assertThat(options.getSchemaReconcileMaxBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_SCHEMA_RECONCILE_MAX_BACKOFF);
        assertThat(options.getSchemaReconcileMaxAttempts())
                .isEqualTo(FileLoadsOptions.DEFAULT_SCHEMA_RECONCILE_MAX_ATTEMPTS);
    }

    @Test
    void theLoadJobPollScheduleIsDerivedFromTheKnobsAndUnbounded() {
        RetrySchedule schedule =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .loadJobPollInitialBackoff(Duration.ofSeconds(1))
                        .loadJobPollMaxBackoff(Duration.ofSeconds(4))
                        .build()
                        .toLoadJobPollSchedule();

        // No attempt cap to configure: a batch load may legitimately run for hours.
        assertThat(schedule.maxAttempts()).isEqualTo(Integer.MAX_VALUE);
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        assertThat(schedule.backoffMs(1)).isBetween(750L, 1250L);
        assertThat(schedule.backoffMs(2)).isBetween(1500L, 2500L);
    }

    @Test
    void theSchemaReconcileScheduleIsDerivedFromTheKnobs() {
        RetrySchedule schedule =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .schemaReconcileInitialBackoff(Duration.ofSeconds(1))
                        .schemaReconcileMaxBackoff(Duration.ofSeconds(4))
                        .schemaReconcileMaxAttempts(3)
                        .build()
                        .toSchemaReconcileSchedule();

        assertThat(schedule.maxAttempts()).isEqualTo(3);
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        assertThat(schedule.backoffMs(1)).isBetween(750L, 1250L);
        assertThat(schedule.backoffMs(2)).isBetween(1500L, 2500L);
    }

    @Test
    void rejectsInvalidScheduleKnobs() {
        assertThatThrownBy(
                        () -> FileLoadsOptions.builder().loadJobPollInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("loadJobPollInitialBackoff");
        assertThatThrownBy(() -> FileLoadsOptions.builder().schemaReconcileMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaReconcileMaxAttempts");
        assertThatThrownBy(() -> FileLoadsOptions.builder().schemaReconcileMaxBackoff(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FileLoadsOptions.builder().loadJobPollMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("loadJobPollMaxBackoff");
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .schemaReconcileInitialBackoff(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaReconcileInitialBackoff");
        // Sub-millisecond durations truncate to a zero the schedule rejects at first commit, so
        // they are rejected here instead.
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .loadJobPollInitialBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 millisecond");
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket")
                                        .loadJobPollInitialBackoff(Duration.ofSeconds(5))
                                        .loadJobPollMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("loadJobPollMaxBackoff");
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket")
                                        .schemaReconcileInitialBackoff(Duration.ofSeconds(5))
                                        .schemaReconcileMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("schemaReconcileMaxBackoff");
    }

    /**
     * One knob at a time, so dropping any single field from {@code equals}/{@code hashCode} fails
     * here — and every new knob appears in {@code toString}, the operator-facing dump.
     */
    @Test
    void equalsAndToStringCoverEachScheduleKnob() {
        FileLoadsOptions defaults = FileLoadsOptions.builder().stagingPath("gs://bucket").build();

        assertThat(base().loadJobPollInitialBackoff(Duration.ofMillis(1)).build())
                .isNotEqualTo(defaults);
        assertThat(base().loadJobPollMaxBackoff(Duration.ofMinutes(1)).build())
                .isNotEqualTo(defaults);
        assertThat(base().schemaReconcileInitialBackoff(Duration.ofMillis(1)).build())
                .isNotEqualTo(defaults);
        assertThat(base().schemaReconcileMaxBackoff(Duration.ofMinutes(1)).build())
                .isNotEqualTo(defaults);
        assertThat(base().schemaReconcileMaxAttempts(3).build()).isNotEqualTo(defaults);
        assertThat(base().maxOpenDestinations(8).build()).isNotEqualTo(defaults);
        assertThat(base().maxPendingFiles(20_000).build()).isNotEqualTo(defaults);
        assertThat(base().destinationIdleTimeout(Duration.ofSeconds(30)).build())
                .isNotEqualTo(defaults);
        assertThat(base().maxSerializedRowBytes(12_000_000).build()).isNotEqualTo(defaults);

        assertThat(defaults.toString())
                // The enum renders its DDL spelling here, which is the whole visible cost of
                // WriteDisposition.toString(); nothing parses this string, but nothing pinned the
                // rendering either.
                .contains("writeDisposition=write-append")
                .contains("loadJobPollInitialBackoff=PT1S")
                .contains("loadJobPollMaxBackoff=PT30S")
                .contains("schemaReconcileInitialBackoff=PT0.5S")
                .contains("schemaReconcileMaxBackoff=PT10S")
                .contains("schemaReconcileMaxAttempts=10")
                .contains("maxStagingFileBytes=16777216")
                .contains("maxOpenDestinations=16")
                .contains("maxPendingFiles=10000")
                .contains("destinationIdleTimeout=PT1M")
                .contains("maxSerializedRowBytes=15000000");
    }

    private static FileLoadsOptions.Builder base() {
        return FileLoadsOptions.builder().stagingPath("gs://bucket");
    }

    @Test
    void minCheckpointIntervalOverrideIsKept() {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .minCheckpointInterval(Duration.ofSeconds(30))
                        .build();

        assertThat(options.getMinCheckpointInterval()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void maxStagingFileBytesOverrideIsKept() {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .maxStagingFileBytes(64L * 1024 * 1024)
                        .build();

        assertThat(options.getMaxStagingFileBytes()).isEqualTo(64L * 1024 * 1024);
    }

    @Test
    void rejectsNonPositiveMaxStagingFileBytes() {
        assertThatThrownBy(() -> FileLoadsOptions.builder().maxStagingFileBytes(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStagingFileBytes");
        assertThatThrownBy(() -> FileLoadsOptions.builder().maxStagingFileBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void destinationLifecycleOverridesAreKept() {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .maxOpenDestinations(8)
                        .maxPendingFiles(5000)
                        .destinationIdleTimeout(Duration.ofSeconds(30))
                        .maxSerializedRowBytes(12_000_000)
                        .build();

        assertThat(options.getMaxOpenDestinations()).isEqualTo(8);
        assertThat(options.getMaxPendingFiles()).isEqualTo(5000);
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(options.getMaxSerializedRowBytes()).isEqualTo(12_000_000);
    }

    @Test
    void absentFieldsFromAnOlderSerializedJobGraphUseTheNewDefaults() throws Exception {
        FileLoadsOptions.Builder oldBuilder = base();
        setBuilderField(oldBuilder, "maxOpenDestinations", 0);
        setBuilderField(oldBuilder, "maxPendingFiles", 0);
        setBuilderField(oldBuilder, "destinationIdleTimeout", null);
        setBuilderField(oldBuilder, "maxSerializedRowBytes", 0L);
        FileLoadsOptions restored = oldBuilder.build();
        FileLoadsOptions defaults = base().build();

        assertThat(restored.getMaxOpenDestinations()).isEqualTo(16);
        assertThat(restored.getMaxPendingFiles()).isEqualTo(10_000);
        assertThat(restored.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(restored.getMaxSerializedRowBytes()).isEqualTo(15_000_000L);
        assertThat(restored).isEqualTo(defaults).hasSameHashCodeAs(defaults);
        assertThat(restored.toString()).isEqualTo(defaults.toString());
    }

    private static void setBuilderField(FileLoadsOptions.Builder builder, String name, Object value)
            throws Exception {
        java.lang.reflect.Field field = FileLoadsOptions.Builder.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(builder, value);
    }

    @Test
    void rejectsInvalidDestinationLifecycleOptions() {
        assertThatThrownBy(() -> FileLoadsOptions.builder().maxOpenDestinations(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxOpenDestinations");
        assertThatThrownBy(() -> FileLoadsOptions.builder().maxPendingFiles(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPendingFiles");
        assertThatThrownBy(() -> FileLoadsOptions.builder().destinationIdleTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("destinationIdleTimeout");
        assertThatThrownBy(() -> FileLoadsOptions.builder().destinationIdleTimeout(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FileLoadsOptions.builder().maxSerializedRowBytes(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxSerializedRowBytes");
    }

    @Test
    void parquetCompressionIsRejectedUnderAvro() {
        // Rejected rather than ignored: a configuration that says "compress Parquet" on an Avro
        // sink has said something contradictory, and silently dropping it is worse than failing.
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket")
                                        .parquetCompression(ParquetCompression.NONE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parquetCompression")
                .hasMessageContaining("PARQUET");
    }

    @Test
    void parquetCompressionIsAcceptedUnderParquet() {
        // The success side the #289 lesson asks for: a rule that only ever rejects could be
        // rejecting for the wrong reason.
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .stagingFormat(StagingFormat.PARQUET)
                        .parquetCompression(ParquetCompression.NONE)
                        .build();

        assertThat(options.getStagingFormat()).isEqualTo(StagingFormat.PARQUET);
        assertThat(options.getParquetCompression()).isEqualTo(ParquetCompression.NONE);
    }

    @Test
    void parquetIsAcceptedBecauseTheDependenciesAreOnTheTestClasspath() {
        // The classpath probe in build() passes here — parquet-avro and hadoop-common are
        // `provided`, which Maven puts on the test classpath. That is what makes this assertion
        // meaningful rather than vacuous: the probe runs, and it succeeds.
        assertThat(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket")
                                .stagingFormat(StagingFormat.PARQUET)
                                .build()
                                .getStagingFormat())
                .isEqualTo(StagingFormat.PARQUET);
    }

    @Test
    void rejectsNonPositiveMinCheckpointInterval() {
        assertThatThrownBy(() -> FileLoadsOptions.builder().minCheckpointInterval(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minCheckpointInterval");
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .minCheckpointInterval(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FileLoadsOptions.builder().minCheckpointInterval(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void stripsTrailingSlashFromStagingPath() {
        assertThat(FileLoadsOptions.builder().stagingPath("gs://bucket/").build().getStagingPath())
                .isEqualTo("gs://bucket");
        assertThat(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/a/b/")
                                .build()
                                .getStagingPath())
                .isEqualTo("gs://bucket/a/b");
    }

    @Test
    void acceptsBucketOnlyStagingPath() {
        assertThat(FileLoadsOptions.builder().stagingPath("gs://bucket").build().getStagingPath())
                .isEqualTo("gs://bucket");
    }

    @Test
    void rejectsInvalidStagingPath() {
        assertThatThrownBy(() -> FileLoadsOptions.builder().stagingPath("s3://bucket"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gs://");
        assertThatThrownBy(() -> FileLoadsOptions.builder().stagingPath("gs://"))
                .isInstanceOf(IllegalArgumentException.class);
        // The GCS client and load jobs only accept a lowercase scheme.
        assertThatThrownBy(() -> FileLoadsOptions.builder().stagingPath("GS://bucket"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FileLoadsOptions.builder().stagingPath(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void requiresStagingPath() {
        assertThatThrownBy(() -> FileLoadsOptions.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stagingPath");
    }

    @Test
    void pendingFileLimitMustCoverTheOpenDestinationLimit() {
        assertThatThrownBy(
                        () ->
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket")
                                        .maxOpenDestinations(3)
                                        .maxPendingFiles(2)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maxPendingFiles must be >= maxOpenDestinations");
    }

    @Test
    void overridesAreKept() {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .tempDataset("temp_dataset")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();

        assertThat(options.getTempDataset()).isEqualTo("temp_dataset");
        assertThat(options.getWriteDisposition()).isEqualTo(WriteDisposition.WRITE_TRUNCATE_DATA);
    }

    @Test
    void rejectsBlankTempDataset() {
        assertThatThrownBy(() -> FileLoadsOptions.builder().tempDataset(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tempDataset");
    }

    @Test
    void equalsAndHashCode() {
        FileLoadsOptions a = FileLoadsOptions.builder().stagingPath("gs://bucket").build();
        FileLoadsOptions b = FileLoadsOptions.builder().stagingPath("gs://bucket/").build();
        FileLoadsOptions c =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .tempDataset("temp_dataset")
                        .build();
        FileLoadsOptions d =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .minCheckpointInterval(Duration.ofMinutes(10))
                        .build();

        FileLoadsOptions e =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .perDestinationMetrics(true)
                        .build();
        FileLoadsOptions f =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .maxStagingFileBytes(64L * 1024 * 1024)
                        .build();
        FileLoadsOptions g =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .maxPendingFiles(20_000)
                        .build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
        assertThat(a).isNotEqualTo(e);
        assertThat(a).isNotEqualTo(f);
        assertThat(a).isNotEqualTo(g);
    }
}
