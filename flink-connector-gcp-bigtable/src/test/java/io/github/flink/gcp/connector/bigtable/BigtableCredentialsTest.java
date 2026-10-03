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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class BigtableCredentialsTest {

    @TempDir Path tempDir;

    @Test
    void nullLeavesApplicationDefaultCredentialsInEffect() throws Exception {
        assertThat(BigtableCredentials.loadData(null)).isNull();
        assertThat(BigtableCredentials.loadDataAndTableAdmin(null)).isNull();
        assertThat(BigtableCredentials.loadTableAdmin(null)).isNull();
        assertThat(BigtableCredentials.loadAll(null)).isNull();
    }

    @Test
    void loadsAndScopesAServiceAccountForEveryClientFamily() throws Exception {
        CredentialsProvider provider =
                BigtableCredentials.loadAll(ServiceAccountKeyFiles.create(tempDir).toString());

        assertThat(provider.getCredentials()).isInstanceOf(ServiceAccountCredentials.class);
        ServiceAccountCredentials credentials =
                (ServiceAccountCredentials) provider.getCredentials();
        assertThat(credentials.getScopes())
                .contains(
                        "https://www.googleapis.com/auth/bigtable.data",
                        "https://www.googleapis.com/auth/bigtable.admin.table",
                        "https://www.googleapis.com/auth/bigtable.admin.instance");
    }

    @Test
    void eachEntryPointScopesToExactlyItsClientFamilies() throws Exception {
        String keyFile = ServiceAccountKeyFiles.create(tempDir).toString();
        Collection<String> data =
                vendorScopes(BigtableDataSettings.newBuilder().getCredentialsProvider());
        Collection<String> tableAdmin =
                vendorScopes(BigtableTableAdminSettings.newBuilder().getCredentialsProvider());
        Collection<String> instanceAdmin =
                vendorScopes(BigtableInstanceAdminSettings.newBuilder().getCredentialsProvider());

        assertThat(scopesOf(BigtableCredentials.loadData(keyFile)))
                .containsExactlyInAnyOrderElementsOf(union(data))
                .doesNotContain("https://www.googleapis.com/auth/bigtable.admin.table");
        assertThat(scopesOf(BigtableCredentials.loadTableAdmin(keyFile)))
                .containsExactlyInAnyOrderElementsOf(union(tableAdmin))
                .doesNotContain("https://www.googleapis.com/auth/bigtable.data")
                .doesNotContain("https://www.googleapis.com/auth/bigtable.admin.instance");
        assertThat(scopesOf(BigtableCredentials.loadDataAndTableAdmin(keyFile)))
                .containsExactlyInAnyOrderElementsOf(union(data, tableAdmin))
                .doesNotContain("https://www.googleapis.com/auth/bigtable.admin.instance");
        assertThat(scopesOf(BigtableCredentials.loadAll(keyFile)))
                .containsExactlyInAnyOrderElementsOf(union(data, tableAdmin, instanceAdmin));
    }

    @Test
    void anUnreadablePathDoesNotLeakIntoTheFailure() {
        String path = tempDir.resolve("mounted-secret-name.json").toString();

        Throwable failure = catchThrowable(() -> BigtableCredentials.loadData(path));

        assertThat(failure)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Bigtable service-account key file.")
                .hasNoCause();
        assertThat(failure.toString()).doesNotContain(path);
    }

    private static Collection<String> scopesOf(CredentialsProvider provider) throws IOException {
        return ((ServiceAccountCredentials) provider.getCredentials()).getScopes();
    }

    private static Collection<String> vendorScopes(CredentialsProvider provider) {
        return ((GoogleCredentialsProvider) provider).getScopesToApply();
    }

    @SafeVarargs
    private static Set<String> union(Collection<String>... groups) {
        Set<String> union = new LinkedHashSet<>();
        for (Collection<String> group : groups) {
            union.addAll(group);
        }
        return union;
    }
}
