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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;

import javax.annotation.Nullable;

import java.util.List;

/** Point reads through a client owned by one lookup function instance. */
final class SpannerDatabaseRowLookup implements SpannerRowLookup {
    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    private final String table;
    private final List<String> columns;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private transient GoogleCredentials credentialsOverride;
    @Nullable private transient Spanner spanner;
    @Nullable private transient DatabaseClient client;

    SpannerDatabaseRowLookup(
            SpannerDatabase database,
            String table,
            List<String> columns,
            @Nullable String emulatorEndpoint) {
        this(database, table, columns, emulatorEndpoint, null);
    }

    SpannerDatabaseRowLookup(
            SpannerDatabase database,
            String table,
            List<String> columns,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this.database = database;
        this.table = table;
        this.columns = columns;
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    /** Returns the exact table passed to the Spanner point-read APIs. */
    @VisibleForTesting
    String table() {
        return table;
    }

    /** Returns the serialized credential path without reading it. */
    @VisibleForTesting
    @Nullable
    String serviceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Override
    public void open() throws Exception {
        spanner = SpannerClients.open(database, settings());
        client =
                spanner.getDatabaseClient(
                        DatabaseId.of(
                                database.getProject(),
                                database.getInstance(),
                                database.getDatabase()));
    }

    @VisibleForTesting
    SpannerOptions settings() throws Exception {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null ? null : EmulatorEndpoint.parse(emulatorEndpoint);
        if (credentialsOverride == null && serviceAccountKeyFile != null) {
            credentialsOverride = SpannerCredentials.load(serviceAccountKeyFile);
        }
        return SpannerClients.settings(database, endpoint, credentialsOverride);
    }

    @Override
    public Struct read(Key key) {
        return client().singleUse().readRow(table, key, columns);
    }

    @Override
    public ApiFuture<Struct> readAsync(Key key) {
        return client().singleUse().readRowAsync(table, key, columns);
    }

    @Override
    public void close() {
        if (spanner != null) {
            spanner.close();
            spanner = null;
            client = null;
        }
    }

    private DatabaseClient client() {
        if (client == null) {
            throw new IllegalStateException("The Spanner row lookup has not been opened.");
        }
        return client;
    }
}
