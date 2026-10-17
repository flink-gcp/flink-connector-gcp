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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The production {@link DatastoreDatabaseAccess}, over the client library's {@link Datastore},
 * which closing it releases.
 *
 * <p>A commit is one non-transactional {@code Commit} request, sent through the client's own RPC
 * object, with the writes in the order they were given, each in the encoding the writer made when
 * it buffered the write. The client library's {@code Batch} is not used: it cannot carry a merge's
 * property mask. A request never holds two writes to one key, because the writer flushes before a
 * key repeats.
 */
@Internal
final class DatastoreServiceAdapter implements DatastoreDatabaseAccess {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreServiceAdapter.class);

    private final Datastore datastore;

    DatastoreServiceAdapter(Datastore datastore) {
        this.datastore = Preconditions.checkNotNull(datastore, "datastore must not be null");
    }

    @Override
    public void commit(List<EncodedMutation> writes) {
        DatastoreOptions options = datastore.getOptions();
        CommitRequest.Builder request =
                CommitRequest.newBuilder()
                        .setProjectId(options.getProjectId())
                        .setDatabaseId(options.getDatabaseId())
                        .setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
        for (EncodedMutation write : writes) {
            request.addMutations(write.getEncoding());
        }
        rpc().commit(request.build());
    }

    private DatastoreRpc rpc() {
        return (DatastoreRpc) datastore.getOptions().getRpc();
    }

    /**
     * Sends one {@code Lookup} for the key. Not {@code Datastore.get}: the client library re-sends
     * a lookup for as long as the service defers the key, with no bound, and the writer needs one
     * bounded call — any answer, the deferral included, shows the database is there. The call goes
     * through the client's own RPC object, so it takes the client's single-attempt settings and
     * opens nothing.
     */
    @Override
    @Nullable
    public Entity lookup(Key key) {
        LookupResponse response =
                rpc().lookup(
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
            // service stored until it began refusing one in 2026-10: the lookup was still
            // answered, which is all a caller confirming the database asks.
            LOG.debug("The entity stored under {} cannot be read by the client library.", key, e);
            return null;
        }
    }

    /** Sends one {@code AllocateIds}, which takes the client's single-attempt settings. */
    @Override
    public List<Key> allocateIds(List<IncompleteKey> keys) {
        return datastore.allocateId(keys.toArray(new IncompleteKey[0]));
    }

    @Override
    public void close() throws Exception {
        datastore.close();
    }
}
