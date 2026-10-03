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

package io.github.flink.gcp.connector.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link BigQueryCredentials}. */
class BigQueryCredentialsTest {

    private static final String CLOUD_PLATFORM_SCOPE =
            "https://www.googleapis.com/auth/cloud-platform";

    @TempDir Path tempDir;

    @Test
    void nullLeavesApplicationDefaultCredentialsInEffect() throws Exception {
        assertThat(BigQueryCredentials.load(null)).isNull();
    }

    @Test
    void loadsAndScopesAServiceAccountKey() throws Exception {
        GoogleCredentials credentials =
                BigQueryCredentials.load(ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
        assertThat(((ServiceAccountCredentials) credentials).getScopes())
                .containsExactly(CLOUD_PLATFORM_SCOPE);
    }

    @Test
    void bigQueryOptionsCarryTheConfiguredCredentials() throws Exception {
        BigQueryOptions options =
                BigQueryCredentials.bigQueryOptions(
                        ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(options.getCredentials() instanceof ServiceAccountCredentials).isTrue();
        assertThat(((ServiceAccountCredentials) options.getCredentials()).getClientEmail())
                .isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }

    @Test
    void anUnreadablePathDoesNotLeakIntoTheFailure() {
        String path = tempDir.resolve("mounted-secret-name.json").toString();

        Throwable failure = catchThrowable(() -> BigQueryCredentials.load(path));

        assertThat(failure)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured BigQuery service-account key file.")
                .hasNoCause();
        assertThat(failure.toString()).doesNotContain(path);
    }
}
