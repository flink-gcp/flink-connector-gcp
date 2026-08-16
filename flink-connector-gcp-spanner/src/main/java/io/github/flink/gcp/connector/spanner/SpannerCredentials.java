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

import org.apache.flink.annotation.Internal;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Loads credentials shared by the Spanner sink, bounded source, and table lookup paths. */
@Internal
public final class SpannerCredentials {

    private SpannerCredentials() {}

    /**
     * Loads a service-account key file, or returns {@code null} to preserve application-default
     * credentials.
     *
     * <p>The path and parser failure are deliberately absent from the exception. A path can expose
     * a mounted secret's name, and parser exceptions can echo credential material.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @return scoped service-account credentials, or {@code null} for ADC
     * @throws IOException if the configured file cannot be read as a service-account key
     */
    @Nullable
    public static GoogleCredentials load(@Nullable String serviceAccountKeyFile)
            throws IOException {
        if (serviceAccountKeyFile == null) {
            return null;
        }
        try (InputStream input = Files.newInputStream(Path.of(serviceAccountKeyFile))) {
            return ServiceAccountCredentials.fromStream(input)
                    .createScoped(SpannerStubSettings.getDefaultServiceScopes());
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "Failed to load the configured Spanner service-account key file.");
        }
    }
}
