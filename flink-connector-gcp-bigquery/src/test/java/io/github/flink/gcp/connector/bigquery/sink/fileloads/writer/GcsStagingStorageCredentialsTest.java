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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.StorageOptions;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Credential-loading tests for {@link GcsStagingStorage}. */
class GcsStagingStorageCredentialsTest {

    @TempDir Path tempDir;

    @Test
    void configuredCredentialsAreLoadedWhenTheFirstStagingObjectOpens() {
        String missingPath = tempDir.resolve("missing-gcs-staging-secret.json").toString();
        GcsStagingStorage storage = new GcsStagingStorage(missingPath);

        assertThatThrownBy(() -> storage.createObject("gs://bucket/object"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured BigQuery service-account key file.")
                .hasNoCause()
                .hasMessageNotContaining(missingPath);
    }

    @Test
    void productionOptionsCarryTheConfiguredCredentials() throws Exception {
        StorageOptions options =
                GcsStagingStorage.productionOptions(
                        ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(options.getCredentials())
                .isInstanceOf(ServiceAccountCredentials.class)
                .extracting(
                        credentials -> ((ServiceAccountCredentials) credentials).getClientEmail())
                .isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }
}
