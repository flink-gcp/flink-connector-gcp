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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.auth.Credentials;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.DatastoreClients;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** Opens the production {@link DatastoreDatabaseAccess}: a client for the configured database. */
@Internal
public final class DefaultDatastoreDatabaseAccessFactory implements DatastoreDatabaseAccessFactory {

    private final DatabaseDestination database;
    private final Duration requestTimeout;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final Credentials credentials;

    /**
     * Creates the factory.
     *
     * @param database the database to write to
     * @param requestTimeout the bound on each call, the commit and the lookup alike
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentials credentials loaded from a configured key file, or {@code null} for ADC
     */
    public DefaultDatastoreDatabaseAccessFactory(
            DatabaseDestination database,
            Duration requestTimeout,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentials) {
        this.database = database;
        this.requestTimeout = requestTimeout;
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentials = credentials;
    }

    @Override
    public DatastoreDatabaseAccess create() throws IOException {
        return new DatastoreServiceAdapter(
                DatastoreClients.open(
                        database,
                        DatastoreClients.settings(
                                database, emulatorEndpoint, credentials, requestTimeout)));
    }
}
