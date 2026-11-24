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
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.v1.DatastoreClient;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * The production {@link DatastoreDatabaseAccess}, over the generated Datastore client and its lease
 * on the connector's gRPC event-loop group, which closing it releases.
 *
 * <p>A commit is one non-transactional {@code Commit} request with the writes in the order they
 * were given, each in the encoding the writer made when it buffered the write. The client library's
 * {@code Batch} is not used: it cannot carry a merge's property mask. A request never holds two
 * writes to one key, because the writer flushes before a key repeats.
 *
 * <p>Every call is the generated client's synchronous one, so a failure is the gax {@code
 * ApiException} carrying the status, an id allocation's included.
 */
@Internal
final class DatastoreServiceAdapter implements DatastoreDatabaseAccess {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreServiceAdapter.class);

    private final DatabaseDestination database;
    private final GrpcEventLoops.LeasedClient<DatastoreClient> client;

    /**
     * Creates the adapter, which owns the client.
     *
     * @param database the database every request names
     * @param client the client with its lease
     */
    DatastoreServiceAdapter(
            DatabaseDestination database, GrpcEventLoops.LeasedClient<DatastoreClient> client) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.client = Preconditions.checkNotNull(client, "client must not be null");
    }

    @Override
    public void commit(List<EncodedMutation> writes) {
        CommitRequest.Builder request =
                CommitRequest.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId())
                        .setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
        for (EncodedMutation write : writes) {
            request.addMutations(write.getEncoding());
        }
        client.client().commitCallable().call(request.build());
    }

    @Override
    @Nullable
    public Entity lookup(Key key) {
        LookupResponse response =
                client.client()
                        .lookupCallable()
                        .call(
                                LookupRequest.newBuilder()
                                        .setProjectId(key.getProjectId())
                                        .setDatabaseId(key.getDatabaseId())
                                        .addKeys(EntityProtos.key(key))
                                        .build());
        if (response.getFoundCount() == 0) {
            return null;
        }
        try {
            return Entity.fromPb(response.getFound(0).getEntity());
        } catch (RuntimeException e) {
            // A value the client library cannot read, such as an array inside an array, which the
            // service has both stored and refused in a Standard-edition database (2026-10), so a
            // database may hold one: the lookup was still answered, which is all a caller
            // confirming the database asks.
            LOG.debug("The entity stored under {} cannot be read by the client library.", key, e);
            return null;
        }
    }

    /**
     * Allocates one id per key in one {@code AllocateIds} request to the adapter's database, and
     * reads each complete key the service returns through the client library's own decoding.
     */
    @Override
    public List<Key> allocateIds(List<IncompleteKey> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }
        AllocateIdsRequest.Builder request =
                AllocateIdsRequest.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId());
        for (IncompleteKey key : keys) {
            request.addKeys(EntityProtos.key(key));
        }
        AllocateIdsResponse response = client.client().allocateIdsCallable().call(request.build());
        List<Key> allocated = new ArrayList<>(response.getKeysCount());
        for (com.google.datastore.v1.Key key : response.getKeysList()) {
            allocated.add(completeKey(key));
        }
        return allocated;
    }

    /**
     * Reads a complete key the service returned. {@code Key.fromPb} is package-private, and a
     * key-only entity is the public way through it.
     */
    private static Key completeKey(com.google.datastore.v1.Key key) {
        return Entity.fromPb(com.google.datastore.v1.Entity.newBuilder().setKey(key).build())
                .getKey();
    }

    @Override
    public void close() {
        client.close();
    }
}
