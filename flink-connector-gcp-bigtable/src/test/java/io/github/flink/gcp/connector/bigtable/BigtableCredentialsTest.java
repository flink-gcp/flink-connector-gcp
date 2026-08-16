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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class BigtableCredentialsTest {

    @TempDir Path tempDir;

    @Test
    void nullLeavesApplicationDefaultCredentialsInEffect() throws Exception {
        assertThat(BigtableCredentials.loadData(null)).isNull();
        assertThat(BigtableCredentials.loadDataAndTableAdmin(null)).isNull();
        assertThat(BigtableCredentials.loadAll(null)).isNull();
    }

    @Test
    void loadsAndScopesAServiceAccountForEveryClientFamily() throws Exception {
        CredentialsProvider provider =
                BigtableCredentials.loadAll(ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(provider.getCredentials()).isInstanceOf(ServiceAccountCredentials.class);
        ServiceAccountCredentials credentials =
                (ServiceAccountCredentials) provider.getCredentials();
        assertThat(credentials.getScopes())
                .contains(
                        "https://www.googleapis.com/auth/bigtable.data",
                        "https://www.googleapis.com/auth/bigtable.admin.table",
                        "https://www.googleapis.com/auth/bigtable.admin.instance");
    }

    @Test
    void anUnreadablePathDoesNotLeakIntoTheFailure() {
        String path = tempDir.resolve("mounted-secret-name.json").toString();

        Throwable failure = catchThrowable(() -> BigtableCredentials.loadData(path));

        assertSanitized(failure, path);
    }

    @Test
    void malformedCredentialMaterialDoesNotLeakIntoTheFailure() throws Exception {
        String material = "credential-material-must-not-leak";
        Path keyFile = tempDir.resolve("malformed.json");
        Files.writeString(keyFile, material, StandardCharsets.UTF_8);

        Throwable failure = catchThrowable(() -> BigtableCredentials.loadData(keyFile.toString()));

        assertSanitized(failure, keyFile.toString(), material);
    }

    @Test
    void rejectsAValidNonServiceAccountCredentialWithoutLeakingIt() throws Exception {
        String refreshToken = "refresh-token-must-not-leak";
        String material =
                "{"
                        + "\"type\":\"authorized_user\","
                        + "\"client_id\":\"test-client-id\","
                        + "\"client_secret\":\"test-client-secret\","
                        + "\"refresh_token\":\""
                        + refreshToken
                        + "\"}";
        Path keyFile = tempDir.resolve("authorized-user.json");
        Files.writeString(keyFile, material, StandardCharsets.UTF_8);

        Throwable failure = catchThrowable(() -> BigtableCredentials.loadData(keyFile.toString()));

        assertSanitized(failure, keyFile.toString(), refreshToken);
    }

    private static void assertSanitized(Throwable failure, String... forbidden) {
        assertThat(failure)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured Bigtable service-account key file.")
                .hasNoCause();
        assertThat(failure.toString()).doesNotContain(forbidden);
    }
}
