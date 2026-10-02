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

package io.github.flink.gcp.connector.base.auth;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link ServiceAccountKeys}. */
class ServiceAccountKeysTest {

    private static final List<String> SCOPES =
            List.of(
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/example");

    private static final String MESSAGE =
            "Failed to load the configured Example service-account key file.";

    @TempDir Path tempDir;

    @Test
    void nullLeavesApplicationDefaultCredentialsInEffect() throws Exception {
        assertThat(ServiceAccountKeys.load(null, SCOPES, "Example")).isNull();
    }

    @Test
    void loadsAServiceAccountKeyScopedToExactlyTheGivenScopes() throws Exception {
        GoogleCredentials loaded =
                ServiceAccountKeys.load(
                        ServiceAccountKeyFiles.create(tempDir).toString(), SCOPES, "Example");

        assertThat(loaded).isInstanceOf(ServiceAccountCredentials.class);
        ServiceAccountCredentials serviceAccount = (ServiceAccountCredentials) loaded;
        assertThat(serviceAccount.getScopes()).containsExactlyInAnyOrderElementsOf(SCOPES);
        assertThat(serviceAccount.getClientEmail()).isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }

    @Test
    void theFailureMessageNamesTheCallersProduct() {
        String path = tempDir.resolve("absent.json").toString();

        assertThatThrownBy(() -> ServiceAccountKeys.load(path, SCOPES, "Cloud Tasks"))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Cloud Tasks service-account key file.");
    }

    // The exact message is the leak check: Throwable.toString() is the class name and the message,
    // so a message equal to MESSAGE carries no path or key material, and no cause carries them.

    @Test
    void anAbsentPathFailsWithoutItsPath() {
        assertSanitized(tempDir.resolve("mounted-secret-name.json").toString());
    }

    @Test
    void anInvalidPathFailsWithoutItsPath() {
        // Path.of throws InvalidPathException, a RuntimeException whose message echoes the input.
        String path = "mounted-secret-name\0.json";
        assertThatThrownBy(() -> Path.of(path))
                .isInstanceOf(InvalidPathException.class)
                .hasMessageContaining("mounted-secret-name");

        assertSanitized(path);
    }

    @Test
    void malformedCredentialMaterialFailsWithoutTheMaterial() throws Exception {
        assertSanitized(keyFile("malformed.json", "credential-material-must-not-leak"));
    }

    @Test
    void aParserRuntimeExceptionIsSanitizedToo() throws Exception {
        // The parser casts the type field to a String, so a numeric one throws ClassCastException.
        String keyFile = keyFile("numeric-type.json", "{\"type\":1}");
        assertThatThrownBy(
                        () -> {
                            try (InputStream input = Files.newInputStream(Path.of(keyFile))) {
                                ServiceAccountCredentials.fromStream(input);
                            }
                        })
                .isInstanceOf(ClassCastException.class);

        assertSanitized(keyFile);
    }

    @Test
    void validNonServiceAccountCredentialsAreRejectedWithoutTheirSecrets() throws Exception {
        assertSanitized(
                keyFile(
                        "authorized-user.json",
                        "{\"type\":\"authorized_user\","
                                + "\"client_id\":\"client-id\","
                                + "\"client_secret\":\"client-secret-material-must-not-leak\","
                                + "\"refresh_token\":\"refresh-token-material-must-not-leak\"}"));
    }

    @Test
    void rejectsNullScopesAndANullProduct() {
        assertThatThrownBy(() -> ServiceAccountKeys.load(null, null, "Example"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("scopes must not be null");
        assertThatThrownBy(() -> ServiceAccountKeys.load(null, SCOPES, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("product must not be null");
    }

    private String keyFile(String name, String content) throws IOException {
        Path keyFile = tempDir.resolve(name);
        Files.writeString(keyFile, content, StandardCharsets.UTF_8);
        return keyFile.toString();
    }

    private static void assertSanitized(String path) {
        Throwable failure = catchThrowable(() -> ServiceAccountKeys.load(path, SCOPES, "Example"));

        assertThat(failure).isInstanceOf(IOException.class).hasMessage(MESSAGE).hasNoCause();
        assertThat(failure.getSuppressed()).isEmpty();
    }
}
