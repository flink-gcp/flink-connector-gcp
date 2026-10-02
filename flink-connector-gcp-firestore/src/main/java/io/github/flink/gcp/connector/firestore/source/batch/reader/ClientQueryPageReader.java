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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import org.apache.flink.annotation.Internal;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.LazyFirestoreClient;
import io.github.flink.gcp.connector.firestore.source.batch.ReadTimeQueries;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Reads pages through a {@code google-cloud-firestore} client, at the split's read time.
 *
 * <p>A read at a read time answers with the whole result rather than a stream, which is why every
 * page is bounded by a {@code limit} ({@link ReadTimeQueries}).
 */
@Internal
public final class ClientQueryPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    private final LazyFirestoreClient client;

    /**
     * Creates the reader.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public ClientQueryPageReader(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.client = new LazyFirestoreClient("page reader", emulatorEndpoint);
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        client.useCredentials(credentials);
    }

    @Override
    public Query query(DatabaseDestination database, RunQueryRequest request) throws IOException {
        try {
            return Query.fromProto(client.get(database), request);
        } catch (IllegalArgumentException e) {
            throw new IOException("Cannot read a Firestore query of " + database + ".", e);
        }
    }

    @Override
    public List<? extends DocumentSnapshot> read(Query page, Timestamp readTime)
            throws IOException {
        return ReadTimeQueries.get(page, readTime, "Failed to read a page of Firestore documents.")
                .getDocuments();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
