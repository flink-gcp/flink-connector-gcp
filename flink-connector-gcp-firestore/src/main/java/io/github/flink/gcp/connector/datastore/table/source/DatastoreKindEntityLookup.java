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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.cloud.datastore.v1.DatastoreClient;
import com.google.cloud.datastore.v1.DatastoreSettings;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.PropertyMask;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.DatastoreClients;
import io.github.flink.gcp.connector.datastore.DatastoreCredentials;
import io.github.flink.gcp.connector.datastore.table.DatastoreConnectorOptions;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 * Looks entities up through a generated client owned by one lookup function instance, each answer
 * limited to the properties of the columns read.
 *
 * <p>The generated client's {@code Lookup} returns a future, which the library's {@code Datastore}
 * does not offer; the call keeps the generated client's default retry settings for {@code Lookup}.
 * Nothing is set on the request's read options, so a lookup reads strongly, as of the moment it
 * reaches the service.
 */
@Internal
final class DatastoreKindEntityLookup implements DatastoreEntityLookup {

    private static final long serialVersionUID = 1L;

    /** The path a property mask names to return the key alone. */
    private static final String KEY_PATH = "__key__";

    private final DatabaseDestination database;
    private final String[] paths;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private transient DatastoreClient client;
    @Nullable private transient volatile PropertyMask mask;

    /**
     * Creates the lookup.
     *
     * @param database the database
     * @param properties the property names to read, each one top-level property; an empty array
     *     reads none
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the service
     * @param serviceAccountKeyFile the key file, or {@code null} for application default ones
     */
    DatastoreKindEntityLookup(
            DatabaseDestination database,
            String[] properties,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this.database = database;
        this.paths = maskPaths(properties);
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    /** Returns the property mask's paths. */
    @VisibleForTesting
    String[] paths() {
        return paths.clone();
    }

    /** Returns the serialized credential path without reading it. */
    @VisibleForTesting
    @Nullable
    String serviceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Override
    public void open() throws Exception {
        client = DatastoreClient.create(settings());
    }

    /** Builds the settings the one client is opened on; tests inspect them without a client. */
    @VisibleForTesting
    DatastoreSettings settings() throws Exception {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint,
                                DatastoreConnectorOptions.EMULATOR_ENDPOINT.key());
        return DatastoreClients.lookupSettings(
                endpoint,
                endpoint == null ? DatastoreCredentials.load(serviceAccountKeyFile) : null);
    }

    /**
     * The property mask's paths: each name one quoted top-level property, so a name holding a dot
     * is one property, not a path into an embedded entity, with a backquote or a backslash in it
     * escaped by a backslash. A read of no property asks for the key alone.
     */
    @VisibleForTesting
    static String[] maskPaths(String[] properties) {
        if (properties.length == 0) {
            return new String[] {KEY_PATH};
        }
        String[] paths = new String[properties.length];
        for (int i = 0; i < properties.length; i++) {
            paths[i] = "`" + properties[i].replace("\\", "\\\\").replace("`", "\\`") + "`";
        }
        return paths;
    }

    @Override
    public ApiFuture<LookupResponse> lookupAsync(Key key) {
        DatastoreClient opened = client;
        if (opened == null) {
            throw new IllegalStateException("The Datastore entity lookup has not been opened.");
        }
        return opened.lookupCallable().futureCall(request(key));
    }

    /** Builds the one request a lookup of the key sends, with no read options. */
    @VisibleForTesting
    LookupRequest request(Key key) {
        PropertyMask built = mask;
        if (built == null) {
            // Built once per instance after deserialization; a retry may arrive on a gRPC thread.
            built = PropertyMask.newBuilder().addAllPaths(Arrays.asList(paths)).build();
            mask = built;
        }
        return LookupRequest.newBuilder()
                .setProjectId(database.getProject())
                .setDatabaseId(database.getDatabaseId())
                .addKeys(key)
                .setPropertyMask(built)
                .build();
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
