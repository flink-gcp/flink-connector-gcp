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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.annotation.Internal;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.stub.FirestoreStubSettings;
import io.github.flink.gcp.connector.base.auth.ServiceAccountKeys;

import javax.annotation.Nullable;

import java.io.IOException;

/** Loads the credentials every direction of the Firestore connector opens its client with. */
@Internal
public final class FirestoreCredentials {

    private FirestoreCredentials() {}

    /**
     * Loads a service-account key file, or returns {@code null} to preserve application-default
     * credentials.
     *
     * <p>A failure names only the product, never the path or the parser's exception; {@link
     * ServiceAccountKeys#load(String, java.util.Collection, String)} owns that rule.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @return scoped service-account credentials, or {@code null} for ADC
     * @throws IOException if the configured file cannot be read as a service-account key
     */
    @Nullable
    public static GoogleCredentials load(@Nullable String serviceAccountKeyFile)
            throws IOException {
        return ServiceAccountKeys.load(
                serviceAccountKeyFile,
                FirestoreStubSettings.getDefaultServiceScopes(),
                "Firestore");
    }
}
