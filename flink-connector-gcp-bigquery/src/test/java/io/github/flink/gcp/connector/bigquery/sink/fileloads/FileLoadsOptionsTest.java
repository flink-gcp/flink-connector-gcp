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
        assertThat(options.getLoadJobPollInitialBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_LOAD_JOB_POLL_INITIAL_BACKOFF);
        assertThat(options.getLoadJobPollMaxBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_LOAD_JOB_POLL_MAX_BACKOFF);
        assertThat(options.getSchemaUpdateInitialBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_SCHEMA_UPDATE_INITIAL_BACKOFF);
        assertThat(options.getSchemaUpdateMaxBackoff())
                .isEqualTo(FileLoadsOptions.DEFAULT_SCHEMA_UPDATE_MAX_BACKOFF);
        assertThat(options.getSchemaUpdateMaxAttempts())
                .isEqualTo(FileLoadsOptions.DEFAULT_SCHEMA_UPDATE_MAX_ATTEMPTS);
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
    void theSchemaUpdateScheduleIsDerivedFromTheKnobs() {
        RetrySchedule schedule =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .schemaUpdateInitialBackoff(Duration.ofSeconds(1))
                        .schemaUpdateMaxBackoff(Duration.ofSeconds(4))
                        .schemaUpdateMaxAttempts(3)
                        .build()
                        .toSchemaUpdateSchedule();

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
        assertThatThrownBy(() -> FileLoadsOptions.builder().schemaUpdateMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaUpdateMaxAttempts");
        assertThatThrownBy(() -> FileLoadsOptions.builder().schemaUpdateMaxBackoff(null))
                .isInstanceOf(NullPointerException.class);
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
                                        .schemaUpdateInitialBackoff(Duration.ofSeconds(5))
                                        .schemaUpdateMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("schemaUpdateMaxBackoff");
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
    void overridesAreKept() {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .tempDataset("temp_dataset")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                        .build();

        assertThat(options.getTempDataset()).isEqualTo("temp_dataset");
        assertThat(options.getWriteDisposition()).isEqualTo(WriteDisposition.WRITE_TRUNCATE);
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
                        .schemaUpdateMaxAttempts(3)
                        .build();
        FileLoadsOptions f =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket")
                        .loadJobPollMaxBackoff(Duration.ofMinutes(1))
                        .build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
        assertThat(a).isNotEqualTo(e);
        assertThat(a).isNotEqualTo(f);
    }
}
