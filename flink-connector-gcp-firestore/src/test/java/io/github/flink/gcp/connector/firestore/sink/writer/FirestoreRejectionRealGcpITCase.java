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

package io.github.flink.gcp.connector.firestore.sink.writer;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreRealGcpITCase;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what the real service answers each shape of refused write with, and whether it answers
 * for the write or for the whole request: the facts {@link FirestoreErrorClassifier} and the
 * writer's isolation pass rest on, which {@code FirestoreRejectionITCase} measured against the
 * emulator only.
 *
 * <p>Writes go through the production {@link FirestoreDatabaseAccess}, built without an emulator
 * endpoint and so over application-default credentials, with retries disabled, so each outcome is
 * the service's first answer. Where the service and the emulator disagree, the test asserts the
 * service and its comment names the emulator's answer.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class FirestoreRejectionRealGcpITCase extends AbstractFirestoreRealGcpITCase {

    private static final Timestamp STALE = Timestamp.ofTimeSecondsAndNanos(1, 0);

    private FirestoreDatabaseAccess access;
    private String collection;

    @BeforeEach
    void openAccess() throws Exception {
        access = RejectionProbes.open(database(), null);
        collection = uniqueCollection();
        client().document(collection + "/existing").set(Map.of("v", 1L)).get();
    }

    @AfterEach
    void closeAccess() throws Exception {
        access.close();
    }

    @Test
    void aCreateOfAnExistingDocumentIsRefusedWithAlreadyExists() throws Exception {
        assertThat(solo(access, FirestoreWrite.create(path("existing"), Map.of("v", 2L))))
                .isEqualTo(StatusCode.Code.ALREADY_EXISTS);
    }

    @Test
    void anUpdateOfAMissingDocumentIsRefusedWithNotFound() throws Exception {
        assertThat(solo(access, FirestoreWrite.update(path("missing"), Map.of("v", 2L))))
                .isEqualTo(StatusCode.Code.NOT_FOUND);
    }

    @Test
    void aStalePreconditionOnAnExistingDocumentIsRefusedWithFailedPrecondition() throws Exception {
        assertThat(solo(access, FirestoreWrite.update(path("existing"), Map.of("v", 2L), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(solo(access, FirestoreWrite.delete(path("existing"), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
    }

    @Test
    void aStalePreconditionOnAMissingDocumentIsRefusedWithFailedPrecondition() throws Exception {
        // The emulator answers FAILED_PRECONDITION here too, rather than NOT_FOUND.
        assertThat(solo(access, FirestoreWrite.update(path("missing"), Map.of("v", 2L), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(solo(access, FirestoreWrite.delete(path("missing"), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
    }

    @Test
    void aCurrentPreconditionAppliesAndADeleteOfAMissingDocumentSucceeds() throws Exception {
        Timestamp current = read(path("existing")).getUpdateTime();

        assertThat(solo(access, FirestoreWrite.update(path("existing"), Map.of("w", 2L), current)))
                .isNull();
        assertThat(solo(access, FirestoreWrite.delete(path("missing")))).isNull();
    }

    @Test
    void everyInvalidDocumentIsRefusedWithInvalidArgument() throws Exception {
        for (FirestoreWrite invalid : invalidWrites()) {
            assertThat(solo(access, invalid))
                    .as("%s", invalid.getDocumentPath())
                    .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
        }
    }

    @Test
    void anArrayInsideAnArrayIsStored() throws Exception {
        // The emulator refuses this write with INVALID_ARGUMENT, and the vendor documentation says
        // that "in Standard edition databases, an array cannot contain another array value as one
        // of its elements". The suite's databases are Standard edition, and the service stores it.
        assertThat(solo(access, FirestoreWrite.set(path("na"), Map.of("v", List.of(List.of(1L))))))
                .isNull();
        assertThat(read(path("na")).get("v")).isEqualTo(List.of(List.of(1L)));
    }

    @Test
    void stateRefusalsAnswerOnlyTheirOwnWriteInABatch() throws Exception {
        Map<FirestoreWrite, StatusCode.Code> refusals =
                Map.of(
                        FirestoreWrite.create(path("existing"), Map.of("v", 2L)),
                        StatusCode.Code.ALREADY_EXISTS,
                        FirestoreWrite.update(path("missing"), Map.of("v", 2L)),
                        StatusCode.Code.NOT_FOUND,
                        FirestoreWrite.update(path("existing"), Map.of("v", 2L), STALE),
                        StatusCode.Code.FAILED_PRECONDITION);

        for (Map.Entry<FirestoreWrite, StatusCode.Code> refusal : refusals.entrySet()) {
            List<StatusCode.Code> outcomes =
                    batch(
                            access,
                            goodWrites(5, refusal.getKey().getOperation().name()),
                            refusal.getKey());

            assertThat(outcomes.subList(0, 5)).as("%s", refusal.getKey()).containsOnlyNulls();
            assertThat(outcomes.get(5)).as("%s", refusal.getKey()).isEqualTo(refusal.getValue());
        }
    }

    @Test
    void anInvalidArgumentAnswersEveryWriteOfTheRequest() throws Exception {
        // The reason the writer confirms an INVALID_ARGUMENT alone before routing it. The emulator
        // was measured for the reserved field name only.
        for (FirestoreWrite invalid : invalidWrites()) {
            assertThat(batch(access, goodWrites(5, "fanned"), invalid))
                    .as("%s", invalid.getDocumentPath())
                    .containsOnly(StatusCode.Code.INVALID_ARGUMENT);
        }
    }

    @Test
    void aRequestOverTenMebibytesIsApplied() throws Exception {
        // The service documents a 10 MiB request limit, and the writer budgets 9 MiB per request
        // on that premise. Twelve documents of 900 KiB, each within its own limit, are one request
        // of about 10.5 MiB, and the service applied it, as the emulator does. Sent as one raw
        // BatchWrite, so the request's size is what the assertion says rather than a premise about
        // how BulkWriter cuts its batches.
        String database = "projects/" + PROJECT + "/databases/" + database().getDatabaseId();
        Value payload = Value.newBuilder().setStringValue("y".repeat(900 * 1024)).build();
        BatchWriteRequest.Builder request = BatchWriteRequest.newBuilder().setDatabase(database);
        for (int i = 0; i < 12; i++) {
            request.addWrites(
                    Write.newBuilder()
                            .setUpdate(
                                    Document.newBuilder()
                                            .setName(database + "/documents/" + path("large" + i))
                                            .putFields("v", payload)));
        }
        assertThat(request.build().getSerializedSize()).isGreaterThan(10 * 1024 * 1024);

        BatchWriteResponse response;
        try (FirestoreClient raw = FirestoreClient.create()) {
            response = raw.batchWrite(request.build());
        }

        assertThat(response.getStatusList())
                .hasSize(12)
                .allSatisfy(status -> assertThat(status.getCode()).isZero());
    }

    @Test
    void everyWriteToADatabaseThatDoesNotExistIsRefusedWithNotFound() throws Exception {
        // Unmeasurable on the emulator, which serves any database id. A well-formed id that names
        // no database: the writer fails the job on NOT_FOUND rather than routing it, because the
        // status cannot tell a missing document from a missing database.
        try (FirestoreDatabaseAccess missing =
                RejectionProbes.open(
                        DatabaseDestination.of(PROJECT, "flink-absent-" + System.nanoTime()),
                        null)) {
            assertThat(batch(missing, goodWrites(3, "absent"), null))
                    .containsOnly(StatusCode.Code.NOT_FOUND);
        }
    }

    @Test
    void everyWriteToAMalformedDatabaseIdIsRefusedForTheRequest() throws Exception {
        // An id outside the service's grammar (upper case and an underscore). If the service
        // answered INVALID_ARGUMENT, the writer would confirm every write alone and route each,
        // which is ADR-0127's per-record case for checking the grammar in DatabaseDestination.
        try (FirestoreDatabaseAccess malformed =
                RejectionProbes.open(DatabaseDestination.of(PROJECT, "Bad_Id"), null)) {
            assertThat(batch(malformed, goodWrites(3, "malformed"), null))
                    .containsOnly(StatusCode.Code.NOT_FOUND);
        }
    }

    @Test
    void everyWriteToADatastoreModeDatabaseIsRefusedWithFailedPrecondition() throws Exception {
        DatabaseDestination datastoreMode = createDatabase(Database.DatabaseType.DATASTORE_MODE);
        try (FirestoreDatabaseAccess wrongMode = RejectionProbes.open(datastoreMode, null)) {
            assertThat(batch(wrongMode, goodWrites(3, "mode"), null))
                    .containsOnly(StatusCode.Code.FAILED_PRECONDITION);
        }
    }

    private String path(String id) {
        return collection + "/" + id;
    }

    /** One write of each shape the service refuses as invalid, each within the request limit. */
    private List<FirestoreWrite> invalidWrites() {
        Object deep = 1L;
        for (int depth = 0; depth < 25; depth++) {
            deep = Map.of("d", deep);
        }
        return List.of(
                FirestoreWrite.set(path("r"), Map.of("__x__", 1L)),
                FirestoreWrite.set(path("__x__"), Map.of("v", 1L)),
                FirestoreWrite.set(path("big"), Map.of("v", "x".repeat(1024 * 1024))),
                FirestoreWrite.set(path("deep"), Map.of("v", deep)));
    }

    private List<FirestoreWrite> goodWrites(int count, String prefix) {
        List<FirestoreWrite> writes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            writes.add(FirestoreWrite.set(path(prefix + i), Map.of("v", 1L)));
        }
        return writes;
    }

    @Nullable
    private static StatusCode.Code solo(FirestoreDatabaseAccess access, FirestoreWrite write)
            throws Exception {
        return RejectionProbes.solo(access, write);
    }

    private static List<StatusCode.Code> batch(
            FirestoreDatabaseAccess access,
            List<FirestoreWrite> writes,
            @Nullable FirestoreWrite last)
            throws Exception {
        return RejectionProbes.batch(access, writes, last);
    }
}
