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

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Value;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the {@code CommitRequest} the adapter sends: the emulator serves any database id and the
 * gated suite is the only other reader of the request's fields, so the header, the order and the
 * encodings are checked here against a recording RPC object. A lookup's answer is checked against
 * the same kind of object, because neither the emulator nor the service still stores the entity the
 * client library cannot read.
 */
class DatastoreServiceAdapterTest {

    @Test
    void aCommitIsOneNonTransactionalRequestToTheClientsDatabaseInTheWritersOrder()
            throws Exception {
        // The adapter encodes nothing: it sends the encodings the writer made when it buffered
        // the writes, the same instances, which is what keeps a write encoded once.
        List<CommitRequest> sent = new ArrayList<>();
        DatastoreRpc rpc =
                (DatastoreRpc)
                        Proxy.newProxyInstance(
                                DatastoreRpc.class.getClassLoader(),
                                new Class<?>[] {DatastoreRpc.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "commit":
                                            sent.add((CommitRequest) args[0]);
                                            return CommitResponse.getDefaultInstance();
                                        case "close":
                                            return null;
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
        DatastoreOptions options =
                DatastoreOptions.newBuilder()
                        .setProjectId("p")
                        .setDatabaseId("orders-db")
                        .setCredentials(NoCredentials.getInstance())
                        .setServiceRpcFactory(ignored -> rpc)
                        .build();
        Key first = Key.newBuilder("p", "K", "b", "orders-db").build();
        Key second = Key.newBuilder("p", "K", "a", "orders-db").build();
        List<EncodedMutation> writes =
                List.of(
                        EncodedMutation.of(DatastoreMutation.delete(first)),
                        EncodedMutation.of(
                                DatastoreMutation.upsertMerge(
                                        Entity.newBuilder(second).set("v", 1L).build())));

        try (DatastoreServiceAdapter adapter = new DatastoreServiceAdapter(options.getService())) {
            adapter.commit(writes);
        }

        assertThat(sent)
                .singleElement()
                .isEqualTo(
                        CommitRequest.newBuilder()
                                .setProjectId("p")
                                .setDatabaseId("orders-db")
                                .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
                                .addMutations(EntityProtos.mutation(writes.get(0).getWrite()))
                                .addMutations(EntityProtos.mutation(writes.get(1).getWrite()))
                                .build());
        assertThat(sent.get(0).getMutations(0)).isSameAs(writes.get(0).getEncoding());
        assertThat(sent.get(0).getMutations(1)).isSameAs(writes.get(1).getEncoding());
    }

    @Test
    void aLookupAnswersAnEntityTheClientLibraryCannotReadAsNoEntity() throws Exception {
        // An array inside an array, which the service stored until it began refusing one in
        // 2026-10, so a database may still hold one. The lookup was answered, which is all the
        // writer confirming the database asks.
        Value nested =
                Value.newBuilder()
                        .setArrayValue(
                                ArrayValue.newBuilder()
                                        .addValues(
                                                Value.newBuilder()
                                                        .setArrayValue(
                                                                ArrayValue.newBuilder()
                                                                        .addValues(
                                                                                Value.newBuilder()
                                                                                        .setIntegerValue(
                                                                                                1)))))
                        .build();

        assertThat(lookUp(nested)).isNull();
    }

    @Test
    void aLookupAnswersAReadableEntityAsTheEntity() throws Exception {
        Entity found = lookUp(Value.newBuilder().setIntegerValue(1).build());

        assertThat(found).isNotNull();
        assertThat(found.getLong("v")).isEqualTo(1L);
    }

    /** Looks up one key against an RPC object that finds it holding {@code v} = the value. */
    private static Entity lookUp(Value value) throws Exception {
        Key key = Key.newBuilder("p", "K", "a", "orders-db").build();
        LookupResponse response =
                LookupResponse.newBuilder()
                        .addFound(
                                EntityResult.newBuilder()
                                        .setEntity(
                                                com.google.datastore.v1.Entity.newBuilder()
                                                        .setKey(EntityProtos.key(key))
                                                        .putProperties("v", value)))
                        .build();
        DatastoreRpc rpc =
                (DatastoreRpc)
                        Proxy.newProxyInstance(
                                DatastoreRpc.class.getClassLoader(),
                                new Class<?>[] {DatastoreRpc.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "lookup":
                                            return response;
                                        case "close":
                                            return null;
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
        DatastoreOptions options =
                DatastoreOptions.newBuilder()
                        .setProjectId("p")
                        .setDatabaseId("orders-db")
                        .setCredentials(NoCredentials.getInstance())
                        .setServiceRpcFactory(ignored -> rpc)
                        .build();
        try (DatastoreServiceAdapter adapter = new DatastoreServiceAdapter(options.getService())) {
            return adapter.lookup(key);
        }
    }
}
