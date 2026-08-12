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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/** Loads credentials for the Bigtable data and admin client families. */
@Internal
public final class BigtableCredentials {

    private BigtableCredentials() {}

    /** Loads credentials for a data client, or returns {@code null} to preserve ADC. */
    @Nullable
    public static CredentialsProvider loadData(@Nullable String serviceAccountKeyFile)
            throws IOException {
        return load(serviceAccountKeyFile, dataScopes());
    }

    /** Loads one provider shared by data and table-admin clients. */
    @Nullable
    public static CredentialsProvider loadDataAndTableAdmin(@Nullable String serviceAccountKeyFile)
            throws IOException {
        return load(serviceAccountKeyFile, dataScopes(), tableAdminScopes());
    }

    /** Loads one provider shared by data, table-admin and instance-admin clients. */
    @Nullable
    public static CredentialsProvider loadAll(@Nullable String serviceAccountKeyFile)
            throws IOException {
        return load(serviceAccountKeyFile, dataScopes(), tableAdminScopes(), instanceAdminScopes());
    }

    @SafeVarargs
    @Nullable
    private static CredentialsProvider load(
            @Nullable String serviceAccountKeyFile, Collection<String>... scopeGroups)
            throws IOException {
        if (serviceAccountKeyFile == null) {
            return null;
        }
        Set<String> scopes = new LinkedHashSet<>();
        for (Collection<String> group : scopeGroups) {
            scopes.addAll(group);
        }
        try (InputStream input = Files.newInputStream(Path.of(serviceAccountKeyFile))) {
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(input);
            return FixedCredentialsProvider.create(credentials.createScoped(scopes));
        } catch (IOException | RuntimeException e) {
            // A path can disclose a mounted secret's name, and parser failures can echo material.
            throw new IOException(
                    "Failed to load the configured Bigtable service-account key file.");
        }
    }

    private static Collection<String> dataScopes() {
        return scopesOf(BigtableDataSettings.newBuilder().getCredentialsProvider());
    }

    private static Collection<String> tableAdminScopes() {
        return scopesOf(BigtableTableAdminSettings.newBuilder().getCredentialsProvider());
    }

    private static Collection<String> instanceAdminScopes() {
        return scopesOf(BigtableInstanceAdminSettings.newBuilder().getCredentialsProvider());
    }

    private static Collection<String> scopesOf(CredentialsProvider provider) {
        if (!(provider instanceof GoogleCredentialsProvider)) {
            throw new IllegalStateException(
                    "The Bigtable client no longer exposes its default credential scopes through"
                            + " GoogleCredentialsProvider.");
        }
        return ((GoogleCredentialsProvider) provider).getScopesToApply();
    }
}
