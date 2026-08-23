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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.tasks.v2.CloudTasksSettings;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Loads credentials for Cloud Tasks clients. */
@Internal
final class CloudTasksCredentials {

    private CloudTasksCredentials() {}

    /**
     * Loads a service-account key file, or returns {@code null} to leave application-default
     * credentials in effect.
     *
     * <p>The path and parser failure are deliberately absent from the exception. A path can expose
     * a mounted secret's name, and parser exceptions can echo credential material. The actionable
     * distinction is whether the configured file could be loaded at all.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @return a fixed provider for the service account, or {@code null} for ADC
     * @throws IOException if the configured file cannot be read as a service-account key
     */
    @Nullable
    static CredentialsProvider load(@Nullable String serviceAccountKeyFile) throws IOException {
        if (serviceAccountKeyFile == null) {
            return null;
        }
        try (InputStream input = Files.newInputStream(Path.of(serviceAccountKeyFile))) {
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(input);
            return FixedCredentialsProvider.create(
                    credentials.createScoped(CloudTasksSettings.getDefaultServiceScopes()));
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "Failed to load the configured Cloud Tasks service-account key file.");
        }
    }
}
