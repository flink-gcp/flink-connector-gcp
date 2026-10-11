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
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the {@code CommitRequest} the adapter sends: the emulator serves any database id and the
 * gated suite is the only other reader of the request's fields, so the header and the order are
 * checked here against a recording RPC object.
 */
class DatastoreServiceAdapterTest {

    @Test
    void aCommitIsOneNonTransactionalRequestToTheClientsDatabaseInTheWritersOrder()
            throws Exception {
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
        List<DatastoreMutation> writes =
                List.of(
                        DatastoreMutation.delete(first),
                        DatastoreMutation.upsertMerge(
                                Entity.newBuilder(second).set("v", 1L).build()));

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
                                .addMutations(EntityProtos.mutation(writes.get(0)))
                                .addMutations(EntityProtos.mutation(writes.get(1)))
                                .build());
    }
}
