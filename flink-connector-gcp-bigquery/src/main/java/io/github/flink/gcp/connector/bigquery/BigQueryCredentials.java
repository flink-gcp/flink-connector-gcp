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

import org.apache.flink.annotation.Internal;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import io.github.flink.gcp.connector.base.auth.ServiceAccountKeys;

import javax.annotation.Nullable;

import java.io.IOException;
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
     * <p>A failure names only the product, never the path or the parser's exception; {@link
     * ServiceAccountKeys#load(String, java.util.Collection, String)} owns that rule.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @return the scoped service-account credentials, or {@code null} for ADC
     * @throws IOException if the configured file cannot be read as a service-account key
     */
    @Nullable
    public static GoogleCredentials load(@Nullable String serviceAccountKeyFile)
            throws IOException {
        return ServiceAccountKeys.load(
                serviceAccountKeyFile, Collections.singleton(CLOUD_PLATFORM_SCOPE), "BigQuery");
    }

    /** Builds BigQuery REST client options carrying the configured service-account credentials. */
    public static BigQueryOptions bigQueryOptions(String serviceAccountKeyFile) throws IOException {
        return BigQueryOptions.newBuilder().setCredentials(load(serviceAccountKeyFile)).build();
    }
}
