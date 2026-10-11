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

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import com.google.firestore.admin.v1.Database;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreRealGcpITCase;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Measures what the real service answers each shape of refused commit with in Datastore mode, and
 * which of the rest of a refused commit is applied: the facts {@link DatastoreErrorClassifier} and
 * the writer's solo confirmation rest on, which {@code DatastoreRejectionITCase} measured against
 * the emulator only.
 *
 * <p>Commits go through the production {@link DatastoreDatabaseAccess}, built without an emulator
 * endpoint and so over application-default credentials, whose client makes one attempt per call.
 * Where the service and the emulator disagree, the test asserts the service and its comment names
 * the emulator's answer.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class DatastoreRejectionRealGcpITCase extends AbstractDatastoreRealGcpITCase {

    private static final String LONG_STRING = "x".repeat(1_600);

    private DatastoreDatabaseAccess access;
    private String kind;

    @BeforeEach
    void openAccess() throws Exception {
        access = RejectionProbes.open(database());
        kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "existing")).set("v", 1L).build());
    }

    @AfterEach
    void closeAccess() throws Exception {
        access.close();
    }

    @Test
    void anInsertOfAnExistingKeyIsAlreadyExistsAndTheCommitAppliesTheOtherWrites() {
        // The emulator applies none of the other writes of such a commit; the service applies all
        // of them, so the refused commit is not atomic, only the refused write is left out.
        assertThat(code(DatastoreMutation.insert(entity("existing")))).isEqualTo("ALREADY_EXISTS");
        assertThat(
                        code(
                                upsert("a1"),
                                DatastoreMutation.insert(entity("a-new")),
                                DatastoreMutation.insert(entity("existing")),
                                upsert("a2")))
                .isEqualTo("ALREADY_EXISTS");
        assertThat(read(key(kind, "a1"))).isNotNull();
        assertThat(read(key(kind, "a-new"))).as("an insert beside the refused one").isNotNull();
        assertThat(read(key(kind, "a2"))).isNotNull();
        assertThat(read(key(kind, "existing")).getLong("v")).isEqualTo(1L);
    }

    @Test
    void anUpdateOfAMissingKeyIsNotFoundAndTheCommitAppliesTheOtherWrites() {
        // As above: the emulator applies none of them.
        // The client's Batch sends inserts first, so the insert here precedes the refused update
        // on the wire and the upserts follow it.
        assertThat(code(DatastoreMutation.update(entity("missing")))).isEqualTo("NOT_FOUND");
        assertThat(
                        code(
                                upsert("b1"),
                                DatastoreMutation.update(entity("missing")),
                                DatastoreMutation.insert(entity("b-new")),
                                upsert("b2")))
                .isEqualTo("NOT_FOUND");
        assertThat(read(key(kind, "b-new"))).as("the insert before it").isNotNull();
        assertThat(read(key(kind, "b1"))).isNotNull();
        assertThat(read(key(kind, "b2"))).isNotNull();
        assertThat(read(key(kind, "missing"))).isNull();
    }

    @Test
    void aDeleteOfAMissingKeyIsApplied() {
        assertThat(code(DatastoreMutation.delete(key(kind, "missing")))).isEqualTo("applied");
    }

    @Test
    void dataTheServiceRefusesIsInvalidArgumentAndTheCommitAppliesNothing() {
        // Unlike a state refusal above, and unlike the emulator, which applies the writes before an
        // entity over 1 MiB.
        assertRefusedInACommit(
                "long",
                DatastoreMutation.upsert(
                        Entity.newBuilder(key(kind, "long")).set("s", LONG_STRING).build()),
                "INVALID_ARGUMENT");
        assertRefusedInACommit(
                "big",
                DatastoreMutation.upsert(
                        Entity.newBuilder(key(kind, "big"))
                                .set("s", unindexed("y".repeat(1_100_000)))
                                .build()),
                "INVALID_ARGUMENT");
        assertRefusedInACommit(
                "kind",
                DatastoreMutation.upsert(
                        Entity.newBuilder(
                                        Key.newBuilder(
                                                        PROJECT,
                                                        "__x__",
                                                        "n",
                                                        database().getDatabaseId())
                                                .build())
                                .set("v", 1L)
                                .build()),
                "INVALID_ARGUMENT");
        assertRefusedInACommit(
                "property",
                DatastoreMutation.upsert(
                        Entity.newBuilder(key(kind, "rp")).set("__p__", 1L).build()),
                "INVALID_ARGUMENT");
    }

    @Test
    void anUnindexedLongStringIsApplied() {
        assertThat(
                        code(
                                DatastoreMutation.upsert(
                                        Entity.newBuilder(key(kind, "long"))
                                                .set("s", unindexed(LONG_STRING))
                                                .build())))
                .isEqualTo("applied");
    }

    @Test
    void aKeyOfAnotherProjectOrDatabaseIsInvalidArgument() {
        assertRefusedInACommit(
                "project",
                DatastoreMutation.upsert(
                        Entity.newBuilder(
                                        Key.newBuilder(
                                                        "other-project",
                                                        kind,
                                                        "op",
                                                        database().getDatabaseId())
                                                .build())
                                .set("v", 1L)
                                .build()),
                "INVALID_ARGUMENT");
        assertRefusedInACommit(
                "database",
                DatastoreMutation.upsert(
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, "od", "other-db").build())
                                .set("v", 1L)
                                .build()),
                "INVALID_ARGUMENT");
    }

    @Test
    void aCommitOverTenMebibytesIsApplied() {
        // The service documents a 10 MiB request limit, and the writer budgets 9,000,000 bytes per
        // commit on that premise; twelve entities of 900 KiB are one commit of about 10.5 MiB, and
        // the service applies it, as the emulator does.
        List<DatastoreMutation> writes = new ArrayList<>();
        String nineHundredKiB = "z".repeat(900 * 1024);
        for (int i = 0; i < 12; i++) {
            writes.add(
                    DatastoreMutation.upsert(
                            Entity.newBuilder(key(kind, "m" + i))
                                    .set("s", unindexed(nineHundredKiB))
                                    .build()));
        }
        assertThat(code(writes.toArray(new DatastoreMutation[0]))).isEqualTo("applied");
    }

    @Test
    void anArrayInsideAnArrayIsStoredButTheClientLibraryCannotReadIt() {
        // Sent as a raw commit: the client library will not build such a value, and it refuses to
        // read the one the service stores, which is why the datastore table keeps refusing the
        // type.
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
        com.google.datastore.v1.Entity entity =
                com.google.datastore.v1.Entity.newBuilder()
                        .setKey(
                                com.google.datastore.v1.Key.newBuilder()
                                        .setPartitionId(
                                                PartitionId.newBuilder()
                                                        .setProjectId(PROJECT)
                                                        .setDatabaseId(database().getDatabaseId()))
                                        .addPath(
                                                com.google.datastore.v1.Key.PathElement.newBuilder()
                                                        .setKind(kind)
                                                        .setName("nested")))
                        .putProperties("v", nested)
                        .build();

        @Nullable StatusCode.Code code = null;
        try {
            rpc().commit(
                            CommitRequest.newBuilder()
                                    .setProjectId(PROJECT)
                                    .setDatabaseId(database().getDatabaseId())
                                    .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
                                    .addMutations(Mutation.newBuilder().setUpsert(entity))
                                    .build());
        } catch (RuntimeException e) {
            code = DatastoreErrorClassifier.statusCode(e);
        }
        assertThat(code).isNull();
        assertThatThrownBy(() -> client().get(key(kind, "nested")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot contain another list");
        // The writer's lookup still answers, with nothing it can compare an insert to.
        assertThat(access.lookup(key(kind, "nested"))).isNull();
    }

    @Test
    void aLookupAnswersTheStoredEntityOrNothing() {
        assertThat(access.lookup(key(kind, "nothing-here"))).isNull();
        assertThat(access.lookup(key(kind, "existing")))
                .isEqualTo(Entity.newBuilder(key(kind, "existing")).set("v", 1L).build());
    }

    @Test
    void aDatabaseThatDoesNotExistRefusesTheLookupAndTheUpdate() throws Exception {
        // Unmeasurable on the emulator, which serves any database id. The writer routes an update's
        // NOT_FOUND only after a lookup of its key is answered, so a missing database, which
        // answers NOT_FOUND to the update as well, must refuse the lookup.
        assertDatabaseRefuses("flink-absent-" + System.nanoTime());
    }

    @Test
    void aMalformedDatabaseIdRefusesTheLookupAndTheUpdate() throws Exception {
        assertDatabaseRefuses("Bad_Id");
    }

    private void assertDatabaseRefuses(String databaseId) throws Exception {
        try (DatastoreDatabaseAccess other =
                RejectionProbes.open(DatabaseDestination.of(PROJECT, databaseId))) {
            Key missing = Key.newBuilder(PROJECT, kind, "x", databaseId).build();
            assertThat(lookupStatus(other, missing)).isEqualTo(StatusCode.Code.NOT_FOUND);
            assertThat(
                            RejectionProbes.outcome(
                                    other,
                                    DatastoreMutation.update(Entity.newBuilder(missing).build())))
                    .isEqualTo("NOT_FOUND");
        }
    }

    @Test
    void aNativeModeDatabaseServesTheDatastoreApi() throws Exception {
        // Google's documentation calls the modes separate APIs; the service answered both a commit
        // and a lookup through the Datastore API in a Native-mode database.
        DatabaseDestination nativeMode = createDatabase(Database.DatabaseType.FIRESTORE_NATIVE);
        try (DatastoreDatabaseAccess nativeAccess = RejectionProbes.open(nativeMode)) {
            Key key = Key.newBuilder(PROJECT, kind, "n", nativeMode.getDatabaseId()).build();
            assertThat(
                            RejectionProbes.outcome(
                                    nativeAccess,
                                    DatastoreMutation.upsert(
                                            Entity.newBuilder(key).set("v", 1L).build())))
                    .isEqualTo("applied");
            assertThat(nativeAccess.lookup(key))
                    .isEqualTo(Entity.newBuilder(key).set("v", 1L).build());
        }
    }

    /**
     * Sends the mutation alone, then between two valid upserts, and asserts both answers and that
     * the refused commit applied neither neighbour.
     */
    private void assertRefusedInACommit(String label, DatastoreMutation refused, String expected) {
        assertThat(code(refused)).as(label + " alone").isEqualTo(expected);
        assertThat(code(upsert(label + "-1"), refused, upsert(label + "-2")))
                .as(label + " in a commit")
                .isEqualTo(expected);
        assertThat(read(key(kind, label + "-1"))).as(label + ": the upsert before it").isNull();
        assertThat(read(key(kind, label + "-2"))).as(label + ": the upsert after it").isNull();
    }

    @Nullable
    /** The status the lookup was refused with; a lookup that is answered fails the assertion. */
    private static StatusCode.Code lookupStatus(DatastoreDatabaseAccess access, Key key) {
        try {
            access.lookup(key);
        } catch (RuntimeException e) {
            StatusCode.Code code = DatastoreErrorClassifier.statusCode(e);
            if (code == null) {
                throw new AssertionError("The lookup failed without a status", e);
            }
            return code;
        }
        throw new AssertionError("The lookup of " + key + " was answered");
    }

    private String code(DatastoreMutation... writes) {
        return RejectionProbes.outcome(access, writes);
    }

    private Entity entity(String name) {
        return Entity.newBuilder(key(kind, name)).set("v", 3L).build();
    }

    private DatastoreMutation upsert(String name) {
        return DatastoreMutation.upsert(Entity.newBuilder(key(kind, name)).set("v", 2L).build());
    }

    private static StringValue unindexed(String value) {
        return StringValue.newBuilder(value).setExcludeFromIndexes(true).build();
    }
}
