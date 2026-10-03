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
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Measures what the emulator answers each shape of refused commit with, and which of the rest of a
 * refused commit is applied — the facts {@link DatastoreErrorClassifier} and the writer's solo
 * confirmation rest on.
 *
 * <p>Commits go through the production {@link DatastoreDatabaseAccess}, whose client makes one
 * attempt per call, so each outcome is the service's first answer. Emulator evidence only
 * (2026-10-03, google-cloud-cli 587.0.0-emulators in Datastore mode, google-cloud-datastore 3.4.0):
 * the real service's answers are the gated suite's to confirm, and the connector's docs page
 * records where the two are known to differ.
 */
class DatastoreRejectionITCase extends AbstractDatastoreEmulatorITCase {

    private static final String LONG_STRING = "x".repeat(1_600);

    private DatastoreDatabaseAccess access;
    private String kind;

    @BeforeEach
    void openAccess() throws Exception {
        access = open(database());
        kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "existing")).set("v", 1L).build());
    }

    @AfterEach
    void closeAccess() throws Exception {
        access.close();
    }

    @Test
    void anInsertOfAnExistingKeyIsAlreadyExistsAndTheCommitAppliesNothing() {
        assertThat(code(DatastoreMutation.insert(entity("existing")))).isEqualTo("ALREADY_EXISTS");
        assertThat(code(upsert("a1"), DatastoreMutation.insert(entity("existing")), upsert("a2")))
                .isEqualTo("ALREADY_EXISTS");
        assertThat(read(key(kind, "a1"))).isNull();
        assertThat(read(key(kind, "a2"))).isNull();
    }

    @Test
    void anUpdateOfAMissingKeyIsNotFoundAndTheCommitAppliesNothing() {
        assertThat(code(DatastoreMutation.update(entity("missing")))).isEqualTo("NOT_FOUND");
        assertThat(code(upsert("b1"), DatastoreMutation.update(entity("missing")), upsert("b2")))
                .isEqualTo("NOT_FOUND");
        assertThat(read(key(kind, "b1"))).isNull();
        assertThat(read(key(kind, "b2"))).isNull();
    }

    @Test
    void aDeleteOfAMissingKeyIsApplied() {
        assertThat(code(DatastoreMutation.delete(key(kind, "missing")))).isEqualTo("applied");
    }

    @Test
    void dataTheServiceRefusesIsInvalidArgumentAndAnOversizedEntityAppliesTheWritesBeforeIt() {
        assertRefusedInACommit(
                "long",
                DatastoreMutation.upsert(
                        Entity.newBuilder(key(kind, "long")).set("s", LONG_STRING).build()),
                "INVALID_ARGUMENT",
                false);
        assertRefusedInACommit(
                "big",
                DatastoreMutation.upsert(
                        Entity.newBuilder(key(kind, "big"))
                                .set("s", unindexed("y".repeat(1_100_000)))
                                .build()),
                "INVALID_ARGUMENT",
                true);
        assertRefusedInACommit(
                "kind",
                DatastoreMutation.upsert(
                        Entity.newBuilder(Key.newBuilder(PROJECT, "__x__", "n").build())
                                .set("v", 1L)
                                .build()),
                "INVALID_ARGUMENT",
                false);
        assertRefusedInACommit(
                "property",
                DatastoreMutation.upsert(
                        Entity.newBuilder(key(kind, "rp")).set("__p__", 1L).build()),
                "INVALID_ARGUMENT",
                false);
    }

    /**
     * The partial application follows the request's order, which the client's {@code Batch} groups
     * by operation: the delete added first is sent last, after the oversized upsert that stops the
     * commit, while the insert added last is sent first and applied.
     */
    @Test
    void aPartlyAppliedCommitFollowsTheRequestsOperationOrderNotTheWritersOrder() {
        client().put(Entity.newBuilder(key(kind, "to-delete")).set("v", 1L).build());

        assertThat(
                        code(
                                DatastoreMutation.delete(key(kind, "to-delete")),
                                DatastoreMutation.upsert(
                                        Entity.newBuilder(key(kind, "big"))
                                                .set("s", unindexed("y".repeat(1_100_000)))
                                                .build()),
                                DatastoreMutation.insert(entity("inserted"))))
                .isEqualTo("INVALID_ARGUMENT");
        assertThat(read(key(kind, "inserted"))).as("the insert, sent first").isNotNull();
        assertThat(read(key(kind, "to-delete"))).as("the delete, sent last").isNotNull();
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
                        Entity.newBuilder(Key.newBuilder("other-project", kind, "op").build())
                                .set("v", 1L)
                                .build()),
                "INVALID_ARGUMENT",
                false);
        assertRefusedInACommit(
                "database",
                DatastoreMutation.upsert(
                        Entity.newBuilder(Key.newBuilder(PROJECT, kind, "od", "other-db").build())
                                .set("v", 1L)
                                .build()),
                "INVALID_ARGUMENT",
                false);
    }

    /**
     * Sends the mutation alone, then between two valid upserts, and asserts both answers and which
     * neighbour the refused commit applied: never the one after it, and the one before it only
     * where {@code beforeApplied} says so.
     */
    private void assertRefusedInACommit(
            String label, DatastoreMutation refused, String expected, boolean beforeApplied) {
        assertThat(code(refused)).as(label + " alone").isEqualTo(expected);
        assertThat(code(upsert(label + "-1"), refused, upsert(label + "-2")))
                .as(label + " in a commit")
                .isEqualTo(expected);
        assertThat(read(key(kind, label + "-1")) != null)
                .as(label + ": the upsert before it applied")
                .isEqualTo(beforeApplied);
        assertThat(read(key(kind, label + "-2"))).as(label + ": the upsert after it").isNull();
    }

    /** A deviation: the service documents a 10 MiB request limit, which the emulator ignores. */
    @Test
    void theEmulatorEnforcesNoRequestSize() {
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
    void aLookupOfAMissingKeyIsAnswered() {
        assertThatCode(() -> access.lookup(key(kind, "nothing-here"))).doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                access.lookup(
                                        Key.newBuilder(PROJECT, kind, "nothing-here")
                                                .setNamespace("tenant")
                                                .addAncestor(
                                                        com.google.cloud.datastore.PathElement.of(
                                                                "Parent", 3L))
                                                .build()))
                .doesNotThrowAnyException();
    }

    /**
     * A deviation: the emulator serves any database id, so a missing database cannot be shown here.
     * The writer's lookup after an update's {@code NOT_FOUND} rests on the service refusing both
     * for a missing database, which the gated suite measures.
     */
    @Test
    void theEmulatorServesADatabaseThatWasNeverCreated() throws Exception {
        try (DatastoreDatabaseAccess other = open(DatabaseDestination.of(PROJECT, "no-such-db"))) {
            Key missing = Key.newBuilder(PROJECT, kind, "x", "no-such-db").build();
            assertThatCode(() -> other.lookup(missing)).doesNotThrowAnyException();
            assertThat(outcome(other, DatastoreMutation.update(Entity.newBuilder(missing).build())))
                    .isEqualTo("NOT_FOUND");
        }
    }

    private static DatastoreDatabaseAccess open(DatabaseDestination database) throws Exception {
        return new DefaultDatastoreDatabaseAccessFactory(
                        database,
                        Duration.ofSeconds(30),
                        EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"),
                        null)
                .create();
    }

    private String code(DatastoreMutation... writes) {
        return outcome(access, writes);
    }

    private static String outcome(DatastoreDatabaseAccess access, DatastoreMutation... writes) {
        try {
            access.commit(List.of(writes));
            return "applied";
        } catch (RuntimeException e) {
            @Nullable StatusCode.Code code = DatastoreErrorClassifier.statusCode(e);
            // A failure without a status keeps its own description, so an assertion names it.
            return code != null ? code.name() : "no status: " + e;
        }
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
