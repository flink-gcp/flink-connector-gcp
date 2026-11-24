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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.v1.DatastoreSettings;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Value;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.DatastoreClients;
import io.github.flink.gcp.connector.datastore.FakeDatastoreStub;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the {@code CommitRequest} the adapter sends: the emulator serves any database id and the
 * gated suite is the only other reader of the request's fields, so the header, the order and the
 * encodings are checked here against a recording stub of the generated client. A lookup's answer is
 * checked against the same kind of stub, because neither the emulator nor the service still stores
 * the entity the client library cannot read, and so is an id allocation's.
 */
class DatastoreServiceAdapterTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "orders-db");

    /**
     * The sink's client makes one attempt per call, bounded by the request timeout, on every method
     * the sink uses: the sink owns its retry loop.
     */
    @Test
    void theFactorysClientMakesOneAttemptBoundedByTheRequestTimeout() {
        Duration timeout = Duration.ofSeconds(7);
        DatastoreSettings.Builder settings =
                new DefaultDatastoreDatabaseAccessFactory(
                                DATABASE, timeout, EmulatorEndpoint.parse("localhost:1", "e"), null)
                        .settings();

        assertThat(settings.commitSettings().getRetrySettings())
                .isEqualTo(DatastoreClients.singleAttempt(timeout));
        assertThat(settings.lookupSettings().getRetrySettings())
                .isEqualTo(DatastoreClients.singleAttempt(timeout));
        assertThat(settings.allocateIdsSettings().getRetrySettings())
                .isEqualTo(DatastoreClients.singleAttempt(timeout));
    }

    @Test
    void aCommitIsOneNonTransactionalRequestToTheClientsDatabaseInTheWritersOrder()
            throws Exception {
        // The adapter encodes nothing: it sends the encodings the writer made when it buffered
        // the writes, the same instances, which is what keeps a write encoded once.
        FakeDatastoreStub stub =
                new FakeDatastoreStub().onCommit(request -> CommitResponse.getDefaultInstance());
        // Keys of another project and database than the adapter's: the request names the
        // adapter's database, which the writer routes keys to before they reach it.
        Key first = Key.newBuilder("q", "K", "b", "other-db").build();
        Key second = Key.newBuilder("q", "K", "a", "other-db").build();
        List<EncodedMutation> writes =
                List.of(
                        EncodedMutation.of(DatastoreMutation.delete(first)),
                        EncodedMutation.of(
                                DatastoreMutation.upsertMerge(
                                        Entity.newBuilder(second).set("v", 1L).build())));

        try (DatastoreServiceAdapter adapter = new DatastoreServiceAdapter(DATABASE, stub.open())) {
            adapter.commit(writes);
        }

        assertThat(stub.requests)
                .singleElement()
                .isEqualTo(
                        CommitRequest.newBuilder()
                                .setProjectId("p")
                                .setDatabaseId("orders-db")
                                .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
                                .addMutations(EntityProtos.mutation(writes.get(0).getWrite()))
                                .addMutations(EntityProtos.mutation(writes.get(1).getWrite()))
                                .build());
        CommitRequest sent = (CommitRequest) stub.requests.get(0);
        assertThat(sent.getMutations(0)).isSameAs(writes.get(0).getEncoding());
        assertThat(sent.getMutations(1)).isSameAs(writes.get(1).getEncoding());
    }

    @Test
    void aLookupAnswersAnEntityTheClientLibraryCannotReadAsNoEntity() throws Exception {
        // An array inside an array, which the service has both stored and refused in a
        // Standard-edition database (2026-10), so a database may hold one. The lookup was
        // answered, which is all the writer confirming the database asks.
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

    /** Looks up one key against a stub that finds it holding {@code v} = the value. */
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
        FakeDatastoreStub stub = new FakeDatastoreStub().onLookup(request -> response);
        try (DatastoreServiceAdapter adapter = new DatastoreServiceAdapter(DATABASE, stub.open())) {
            return adapter.lookup(key);
        }
    }

    /**
     * An id allocation names the adapter's database and sends each key's encoding; the keys the
     * service completes keep their namespace and ancestors through the client library's decoding.
     */
    @Test
    void anAllocationSendsTheKeysAndReadsBackCompleteKeysInOrder() {
        // A key template of another project and database than the adapter's, so that the request
        // can only name the adapter's.
        IncompleteKey incomplete =
                IncompleteKey.newBuilder("q", "Child")
                        .setDatabaseId("other-db")
                        .setNamespace("ns")
                        .addAncestor(PathElement.of("Parent", "root"))
                        .build();
        Key firstAllocated =
                Key.newBuilder("q", "Child", 7L, "other-db")
                        .setNamespace("ns")
                        .addAncestor(PathElement.of("Parent", "root"))
                        .build();
        Key secondAllocated =
                Key.newBuilder("q", "Child", 3L, "other-db")
                        .setNamespace("ns")
                        .addAncestor(PathElement.of("Parent", "root"))
                        .build();
        FakeDatastoreStub stub =
                new FakeDatastoreStub()
                        .onAllocateIds(
                                request ->
                                        AllocateIdsResponse.newBuilder()
                                                .addKeys(EntityProtos.key(firstAllocated))
                                                .addKeys(EntityProtos.key(secondAllocated))
                                                .build());

        List<Key> allocated;
        try (DatastoreServiceAdapter adapter = new DatastoreServiceAdapter(DATABASE, stub.open())) {
            allocated = adapter.allocateIds(List.of(incomplete, incomplete));
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        assertThat(allocated).containsExactly(firstAllocated, secondAllocated);
        assertThat(stub.requests)
                .singleElement()
                .isEqualTo(
                        AllocateIdsRequest.newBuilder()
                                .setProjectId("p")
                                .setDatabaseId("orders-db")
                                .addKeys(EntityProtos.key(incomplete))
                                .addKeys(EntityProtos.key(incomplete))
                                .build());
    }
}
