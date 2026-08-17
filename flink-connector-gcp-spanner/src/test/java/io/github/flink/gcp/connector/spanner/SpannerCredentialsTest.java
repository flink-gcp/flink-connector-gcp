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

package io.github.flink.gcp.connector.spanner;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link SpannerCredentials}. */
class SpannerCredentialsTest {

    @TempDir Path tempDir;

    @Test
    void nullLeavesApplicationDefaultCredentialsInEffect() throws Exception {
        assertThat(SpannerCredentials.load(null)).isNull();
    }

    @Test
    void loadsAndScopesAServiceAccountKey() throws Exception {
        GoogleCredentials loaded =
                SpannerCredentials.load(ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(loaded).isInstanceOf(ServiceAccountCredentials.class);
        assertThat(((ServiceAccountCredentials) loaded).getScopes())
                .containsExactlyInAnyOrderElementsOf(SpannerStubSettings.getDefaultServiceScopes());
    }

    @Test
    void anUnreadablePathDoesNotLeakIntoTheFailure() {
        String path = tempDir.resolve("mounted-secret-name.json").toString();

        Throwable failure = catchThrowable(() -> SpannerCredentials.load(path));

        assertThat(failure)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
        assertThat(failure.toString()).doesNotContain(path);
    }

    @Test
    void malformedCredentialMaterialDoesNotLeakIntoTheFailure() throws Exception {
        String credentialMaterial = "credential-material-must-not-leak";
        Path keyFile = tempDir.resolve("malformed.json");
        Files.writeString(keyFile, credentialMaterial, StandardCharsets.UTF_8);

        Throwable failure = catchThrowable(() -> SpannerCredentials.load(keyFile.toString()));

        assertThat(failure)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
        assertThat(failure.toString())
                .doesNotContain(keyFile.toString())
                .doesNotContain(credentialMaterial);
    }

    @Test
    void validNonServiceAccountCredentialsAreRejectedWithoutLeakingDetails() throws Exception {
        String clientSecret = "client-secret-material-must-not-leak";
        String refreshToken = "refresh-token-material-must-not-leak";
        Path keyFile = tempDir.resolve("authorized-user.json");
        Files.writeString(
                keyFile,
                "{\"type\":\"authorized_user\","
                        + "\"client_id\":\"client-id\","
                        + "\"client_secret\":\""
                        + clientSecret
                        + "\","
                        + "\"refresh_token\":\""
                        + refreshToken
                        + "\"}",
                StandardCharsets.UTF_8);

        Throwable failure = catchThrowable(() -> SpannerCredentials.load(keyFile.toString()));

        assertThat(failure)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
        assertThat(failure.toString())
                .doesNotContain(keyFile.toString())
                .doesNotContain(clientSecret)
                .doesNotContain(refreshToken);
    }
}
