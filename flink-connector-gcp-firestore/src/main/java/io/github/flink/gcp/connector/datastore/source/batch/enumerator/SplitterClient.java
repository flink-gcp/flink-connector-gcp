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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.v1.DatastoreSettings;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The HTTP client the client library's query splitter takes, built on first use and closed once.
 *
 * <p>{@code QuerySplitter} runs its {@code __scatter__} sampling through the {@code
 * datastore-v1-proto-client} {@link Datastore}, an HTTP client of its own, which neither the gRPC
 * client the rest of the source reads through nor a subclass can stand in for: its constructor is
 * package-private. This builds one through the same factory the client library's own HTTP transport
 * ({@code HttpDatastoreRpc}) uses: the service at its default host with the credentials adapted to
 * HTTP requests, and an emulator as a plaintext local host with no credentials — for any emulator
 * endpoint, as everywhere in this connector, where the library does so only for a loopback one
 * ({@code docs/adr/0177}). The splitter is the only caller, and only while a read is planned.
 *
 * <p>The client makes one attempt per call, which is the proto client's own behaviour (the
 * credentials adapter alone re-sends a request once after refreshing an expired token), and gives
 * each call the proto client's read timeout. Closing the holder refuses later calls rather than
 * building a client nothing would close; the transport holds no pooled resource, but is shut down
 * all the same.
 */
@Internal
final class SplitterClient implements AutoCloseable {

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    @Nullable private Credentials credentials;
    @Nullable private NetHttpTransport transport;
    @Nullable private Datastore client;
    private boolean closed;

    /**
     * Creates the holder.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    SplitterClient(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /**
     * Takes the credentials the owner loaded, or {@code null} to use application-default
     * credentials.
     */
    synchronized void useCredentials(@Nullable Credentials credentials) {
        Preconditions.checkState(
                !closed, "The Datastore query splitter's client was closed before use.");
        this.credentials = credentials;
    }

    /**
     * Returns the client, building it on first use.
     *
     * @param project the project the splitter reads; the proto client addresses it
     * @return the client, which this holder owns
     * @throws IOException if the holder was closed, or application-default credentials cannot be
     *     loaded
     */
    synchronized Datastore get(String project) throws IOException {
        if (closed) {
            throw new IOException(
                    "The Datastore query splitter's client for project "
                            + project
                            + " was closed before use.");
        }
        if (client == null) {
            NetHttpTransport newTransport = new NetHttpTransport();
            DatastoreOptions.Builder options =
                    new DatastoreOptions.Builder().projectId(project).transport(newTransport);
            if (emulatorEndpoint != null) {
                options.localHost(emulatorEndpoint.getTarget());
            } else {
                options.initializer(new HttpCredentialsAdapter(credentials()));
            }
            client = DatastoreFactory.get().create(options.build());
            transport = newTransport;
        }
        return client;
    }

    private Credentials credentials() throws IOException {
        if (credentials != null) {
            return credentials;
        }
        return GoogleCredentials.getApplicationDefault()
                .createScoped(DatastoreSettings.getDefaultServiceScopes());
    }

    @Override
    public void close() throws IOException {
        NetHttpTransport toShutDown;
        synchronized (this) {
            closed = true;
            toShutDown = transport;
            transport = null;
            client = null;
            credentials = null;
        }
        if (toShutDown != null) {
            toShutDown.shutdown();
        }
    }
}
