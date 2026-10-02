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

package io.github.flink.gcp.connector.pubsub;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import io.github.flink.gcp.connector.base.auth.ServiceAccountKeys;

import javax.annotation.Nullable;

import java.io.IOException;

/** Loads the credentials shared by the Pub/Sub source and sink client families. */
@Internal
public final class PubSubCredentials {

    private PubSubCredentials() {}

    /**
     * Loads a service-account key file, or returns {@code null} to leave application-default
     * credentials in effect.
     *
     * <p>A failure names only the product, never the path or the parser's exception; {@link
     * ServiceAccountKeys#load(String, java.util.Collection, String)} owns that rule.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @return a fixed provider for the service account, or {@code null} for ADC
     * @throws IOException if the configured file cannot be read as a service-account key
     */
    @Nullable
    public static CredentialsProvider load(@Nullable String serviceAccountKeyFile)
            throws IOException {
        GoogleCredentials credentials =
                ServiceAccountKeys.load(
                        serviceAccountKeyFile,
                        PublisherStubSettings.getDefaultServiceScopes(),
                        "Pub/Sub");
        return credentials == null ? null : FixedCredentialsProvider.create(credentials);
    }
}
