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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.annotation.Internal;

import com.google.auth.Credentials;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.LazyDatastoreClient;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Reads pages through a {@code google-cloud-datastore} client's RPC object.
 *
 * <p>The call is synchronous and keeps the client library's retry settings, so a page that meets a
 * transient failure is retried with the same request, which reads the same entities at a fixed read
 * time. The library's blocking call does not answer a thread interrupt; the call returns when it
 * succeeds or the retry settings' total timeout runs out.
 */
@Internal
public final class ClientQueryPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    private final LazyDatastoreClient client;

    /**
     * Creates the reader.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public ClientQueryPageReader(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.client = new LazyDatastoreClient("page reader", emulatorEndpoint);
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        client.useCredentials(credentials);
    }

    @Override
    public QueryResultBatch read(DatabaseDestination database, RunQueryRequest page)
            throws IOException {
        try {
            return client.rpc(database).runQuery(page).getBatch();
        } catch (RuntimeException e) {
            throw new IOException("Failed to read a page of Datastore entities.", e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
