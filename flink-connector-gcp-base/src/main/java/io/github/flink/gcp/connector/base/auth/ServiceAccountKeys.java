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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Loads the service-account key file a connector is configured with.
 *
 * <p>Each connector keeps its own entry point, which decides the scopes its clients need and
 * whether to wrap the result in a gax credentials provider. This class owns what they share: a
 * {@code null} path leaves application-default credentials in effect, and a failure is reported
 * without the path or the parser's exception (ADR-0174).
 */
@Internal
public final class ServiceAccountKeys {

    private ServiceAccountKeys() {}

    /**
     * Loads and scopes a service-account key file, or returns {@code null} to leave
     * application-default credentials in effect.
     *
     * <p>The path and parser failure are deliberately absent from the exception. A path can expose
     * a mounted secret's name, and parser exceptions can echo credential material. The actionable
     * distinction is whether the configured file could be loaded at all.
     *
     * @param serviceAccountKeyFile the configured key-file path, or {@code null} for ADC
     * @param scopes the OAuth scopes the loaded credentials are restricted to
     * @param product the product name the failure message carries, such as {@code "Spanner"}
     * @return scoped service-account credentials, or {@code null} for ADC
     * @throws IOException if the configured file cannot be read as a service-account key
     */
    @Nullable
    public static GoogleCredentials load(
            @Nullable String serviceAccountKeyFile, Collection<String> scopes, String product)
            throws IOException {
        Preconditions.checkNotNull(scopes, "scopes must not be null");
        Preconditions.checkNotNull(product, "product must not be null");
        if (serviceAccountKeyFile == null) {
            return null;
        }
        try (InputStream input = Files.newInputStream(Path.of(serviceAccountKeyFile))) {
            return ServiceAccountCredentials.fromStream(input).createScoped(scopes);
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "Failed to load the configured " + product + " service-account key file.");
        }
    }
}
