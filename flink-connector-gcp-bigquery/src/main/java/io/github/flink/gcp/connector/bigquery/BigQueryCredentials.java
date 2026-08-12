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

package io.github.flink.gcp.connector.bigquery;

import org.apache.flink.annotation.Internal;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/** Loads credentials shared by the BigQuery and Cloud Storage client families. */
@Internal
public final class BigQueryCredentials {

    private static final String CLOUD_PLATFORM_SCOPE =
            "https://www.googleapis.com/auth/cloud-platform";

    private BigQueryCredentials() {}

    /**
     * Loads a service-account key file, or returns {@code null} to leave application-default
     * credentials in effect.
     *
     * <p>The path and parser failure are deliberately absent from the exception. A path can expose
     * a mounted secret's name, and parser exceptions can echo credential material. The actionable
     * distinction is whether the configured file could be loaded at all.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @return the scoped service-account credentials, or {@code null} for ADC
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
                    .createScoped(Collections.singleton(CLOUD_PLATFORM_SCOPE));
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "Failed to load the configured BigQuery service-account key file.");
        }
    }

    /** Builds BigQuery REST client options carrying the configured service-account credentials. */
    public static BigQueryOptions bigQueryOptions(String serviceAccountKeyFile) throws IOException {
        return BigQueryOptions.newBuilder().setCredentials(load(serviceAccountKeyFile)).build();
    }
}
