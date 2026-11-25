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
import org.apache.flink.annotation.VisibleForTesting;
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
import io.github.flink.gcp.connector.datastore.DatastoreClients;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
 * ({@code docs/adr/0177}). The emulator endpoint reaches it as an HTTP authority ({@link
 * #httpAuthority(EmulatorEndpoint)}). The splitter is the only caller, and only while a read is
 * planned.
 *
 * <p>The client makes one attempt per call, which is the proto client's own behaviour (the
 * credentials adapter alone re-sends a request once after refreshing an expired token), and gives
 * each call the proto client's read timeout. Closing the holder refuses later calls rather than
 * building a client nothing would close; the transport holds no pooled resource, but is shut down
 * all the same.
 */
@Internal
final class SplitterClient implements AutoCloseable {

    /** The scheme of a target that gRPC's DNS resolver reads, such as {@code dns:///host:port}. */
    private static final String DNS_SCHEME = "dns";

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
                options.localHost(httpAuthority(emulatorEndpoint));
            } else {
                options.initializer(new HttpCredentialsAdapter(credentials()));
            }
            client = DatastoreFactory.get().create(options.build());
            transport = newTransport;
        }
        return client;
    }

    /**
     * Returns the emulator endpoint as the {@code host:port} authority that the proto client's
     * {@code localHost} takes, from which it builds {@code
     * http://<authority>/v1/projects/<project>} (datastore-v1-proto-client 3.11.0).
     *
     * <p>The Datastore-mode gRPC clients take the endpoint as a channel target, so this reduces
     * such a target to the {@code host:port} it dials (#2338): an {@code http://} or {@code
     * https://} scheme is removed as it is for those clients ({@link
     * DatastoreClients#withoutHttpScheme(EmulatorEndpoint)}), and a target that is a URI with
     * gRPC's {@code dns} scheme becomes its decoded path without the leading slash, or for an
     * opaque URI the part after the scheme. Anything else is returned as it is. ADR-0208 records
     * which spellings gRPC accepts and why the proto client cannot take them as written.
     *
     * @param endpoint the emulator endpoint
     * @return the endpoint's {@code host:port}
     */
    @VisibleForTesting
    static String httpAuthority(EmulatorEndpoint endpoint) {
        String target = DatastoreClients.withoutHttpScheme(endpoint).getTarget();
        URI uri;
        try {
            uri = new URI(target);
        } catch (URISyntaxException e) {
            return target;
        }
        if (!DNS_SCHEME.equals(uri.getScheme())) {
            return target;
        }
        if (uri.isOpaque()) {
            return uri.getSchemeSpecificPart();
        }
        String path = uri.getPath();
        return path.startsWith("/") ? path.substring(1) : target;
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
