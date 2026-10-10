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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.Batch;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.PartitionId;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

import java.util.List;

/**
 * The production {@link DatastoreDatabaseAccess}, over the client library's {@link Datastore},
 * which closing it releases.
 *
 * <p>A commit goes through the client's {@link Batch}, which sends every write in one {@code
 * Commit} request in non-transactional mode, grouped by operation (inserts, updates, upserts,
 * deletes) rather than in the order they were added. The batch refuses a second write to a key it
 * already holds, or merges the two; the writer never hands it one, because it flushes before a key
 * repeats.
 */
@Internal
final class DatastoreServiceAdapter implements DatastoreDatabaseAccess {

    private final Datastore datastore;

    DatastoreServiceAdapter(Datastore datastore) {
        this.datastore = Preconditions.checkNotNull(datastore, "datastore must not be null");
    }

    @Override
    public void commit(List<DatastoreMutation> writes) {
        Batch batch = datastore.newBatch();
        for (DatastoreMutation write : writes) {
            switch (write.getOperation()) {
                case UPSERT:
                    batch.put(entity(write));
                    break;
                case INSERT:
                    batch.add(entity(write));
                    break;
                case UPDATE:
                    // The batch takes an update only as an Entity, the complete-key subtype; the
                    // copy carries the same key and properties.
                    batch.update(Entity.newBuilder(write.getKey(), entity(write)).build());
                    break;
                case DELETE:
                    batch.delete(write.getKey());
                    break;
                default:
                    throw new IllegalStateException(
                            "Unknown Datastore operation " + write.getOperation());
            }
        }
        batch.submit();
    }

    private static FullEntity<Key> entity(DatastoreMutation write) {
        return Preconditions.checkNotNull(
                write.getEntity(), "A %s carries an entity", write.getOperation());
    }

    /**
     * Sends one {@code Lookup} for the key. Not {@code Datastore.get}: the client library re-sends
     * a lookup for as long as the service defers the key, with no bound, and the writer needs one
     * bounded call — any answer, the deferral included, shows the database is there. The call goes
     * through the client's own RPC object, so it takes the client's single-attempt settings and
     * opens nothing.
     */
    @Override
    public void lookup(Key key) {
        DatastoreOptions options = datastore.getOptions();
        ((DatastoreRpc) options.getRpc())
                .lookup(
                        LookupRequest.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setDatabaseId(key.getDatabaseId())
                                .addKeys(toProto(key))
                                .build());
    }

    /** Sends one {@code AllocateIds}, which takes the client's single-attempt settings. */
    @Override
    public List<Key> allocateIds(List<IncompleteKey> keys) {
        return datastore.allocateId(keys.toArray(new IncompleteKey[0]));
    }

    /**
     * Encodes a key as the protobuf the client library would send for it; the library's own
     * conversion is package-private.
     */
    @VisibleForTesting
    static com.google.datastore.v1.Key toProto(Key key) {
        com.google.datastore.v1.Key.Builder proto =
                com.google.datastore.v1.Key.newBuilder()
                        .setPartitionId(
                                PartitionId.newBuilder()
                                        .setProjectId(key.getProjectId())
                                        .setDatabaseId(key.getDatabaseId())
                                        .setNamespaceId(key.getNamespace()));
        for (PathElement ancestor : key.getAncestors()) {
            proto.addPath(
                    element(
                            ancestor.getKind(),
                            ancestor.hasId() ? ancestor.getId() : null,
                            ancestor.getName()));
        }
        proto.addPath(element(key.getKind(), key.hasId() ? key.getId() : null, key.getName()));
        return proto.build();
    }

    private static com.google.datastore.v1.Key.PathElement element(
            String kind, Long id, String name) {
        com.google.datastore.v1.Key.PathElement.Builder element =
                com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind);
        if (id != null) {
            element.setId(id);
        } else if (name != null) {
            element.setName(name);
        }
        return element.build();
    }

    @Override
    public void close() throws Exception {
        datastore.close();
    }
}
