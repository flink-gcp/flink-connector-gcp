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

package io.github.flink.gcp.connector.bigquery.sink;

import org.junit.jupiter.api.Test;

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

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }
}
