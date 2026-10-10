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
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what the emulator answers each shape of refused write with, and whether it answers for
 * the write or for the whole request — the two facts {@link FirestoreErrorClassifier} and the
 * writer's isolation pass rest on.
 *
 * <p>Writes go through the production {@link FirestoreDatabaseAccess} with retries disabled, so
 * each outcome is the service's first answer. Emulator evidence only (2026-09-27, google-cloud-cli
 * 583.0.0-emulators, google-cloud-firestore 3.46.0): the real service's answers are the gated
 * suite's to confirm, and the connector's docs page records where the two are known to differ.
 */
class FirestoreRejectionITCase extends AbstractFirestoreEmulatorITCase {

    private static final Timestamp STALE = Timestamp.ofTimeSecondsAndNanos(1, 0);

    private FirestoreDatabaseAccess access;
    private String collection;

    @BeforeEach
    void openAccess() throws Exception {
        access =
                RejectionProbes.open(
                        database(), EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"));
        collection = uniqueCollection();
        client().document(collection + "/existing").set(Map.of("v", 1L)).get();
    }

    @AfterEach
    void closeAccess() throws Exception {
        access.close();
    }

    @Test
    void aCreateOfAnExistingDocumentIsRefusedWithAlreadyExists() throws Exception {
        assertThat(solo(FirestoreWrite.create(path("existing"), Map.of("v", 2L))))
                .isEqualTo(StatusCode.Code.ALREADY_EXISTS);
    }

    @Test
    void anUpdateOfAMissingDocumentIsRefusedWithNotFound() throws Exception {
        assertThat(solo(FirestoreWrite.update(path("missing"), Map.of("v", 2L))))
                .isEqualTo(StatusCode.Code.NOT_FOUND);
    }

    @Test
    void aStalePreconditionIsRefusedWithFailedPreconditionWhetherOrNotTheDocumentExists()
            throws Exception {
        assertThat(solo(FirestoreWrite.update(path("existing"), Map.of("v", 2L), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(solo(FirestoreWrite.delete(path("existing"), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(solo(FirestoreWrite.update(path("missing"), Map.of("v", 2L), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(solo(FirestoreWrite.delete(path("missing"), STALE)))
                .isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
    }

    @Test
    void aCurrentPreconditionAppliesAndADeleteOfAMissingDocumentSucceeds() throws Exception {
        Timestamp current = read(path("existing")).getUpdateTime();

        assertThat(solo(FirestoreWrite.update(path("existing"), Map.of("w", 2L), current)))
                .isNull();
        assertThat(solo(FirestoreWrite.delete(path("missing")))).isNull();
    }

    @Test
    void everyInvalidDocumentIsRefusedWithInvalidArgument() throws Exception {
        Object deep = 1L;
        for (int depth = 0; depth < 25; depth++) {
            deep = Map.of("d", deep);
        }

        assertThat(solo(FirestoreWrite.set(path("r"), Map.of("__x__", 1L))))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
        assertThat(solo(FirestoreWrite.set(path("__x__"), Map.of("v", 1L))))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
        assertThat(solo(FirestoreWrite.set(path("big"), Map.of("v", "x".repeat(1024 * 1024)))))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
        assertThat(solo(FirestoreWrite.set(path("na"), Map.of("v", List.of(List.of(1L))))))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
        assertThat(solo(FirestoreWrite.set(path("deep"), Map.of("v", deep))))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
    }

    @Test
    void stateRefusalsAnswerOnlyTheirOwnWriteInABatch() throws Exception {
        List<FirestoreWrite> refusals =
                List.of(
                        FirestoreWrite.create(path("existing"), Map.of("v", 2L)),
                        FirestoreWrite.update(path("missing"), Map.of("v", 2L)),
                        FirestoreWrite.update(path("existing"), Map.of("v", 2L), STALE));

        for (FirestoreWrite refusal : refusals) {
            List<StatusCode.Code> outcomes =
                    batch(goodWrites(5, refusal.getOperation().name()), refusal);

            assertThat(outcomes.subList(0, 5)).as("%s", refusal).containsOnlyNulls();
            assertThat(outcomes.get(5)).as("%s", refusal).isNotNull();
        }
    }

    @Test
    void anInvalidArgumentAnswersEveryWriteOfTheRequest() throws Exception {
        // The reason the writer confirms an INVALID_ARGUMENT alone before routing it.
        List<StatusCode.Code> outcomes =
                batch(goodWrites(5, "fanned"), FirestoreWrite.set(path("r"), Map.of("__x__", 1L)));

        assertThat(outcomes).containsOnly(StatusCode.Code.INVALID_ARGUMENT);
    }

    @Test
    void theEmulatorDoesNotEnforceTheRequestSizeLimit() throws Exception {
        // A deviation from the service, which documents a 10 MiB request limit: twelve documents of
        // 900 KiB are one request of about 10.5 MiB, and the emulator applies it. The writer's
        // request budget is therefore untested against a refusal here.
        String payload = "y".repeat(900 * 1024);
        List<FirestoreWrite> writes = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            writes.add(FirestoreWrite.set(path("large" + i), Map.of("v", payload)));
        }

        assertThat(batch(writes, null)).containsOnlyNulls();
    }

    private String path(String id) {
        return collection + "/" + id;
    }

    private List<FirestoreWrite> goodWrites(int count, String prefix) {
        List<FirestoreWrite> writes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            writes.add(FirestoreWrite.set(path(prefix + i), Map.of("v", 1L)));
        }
        return writes;
    }

    @Nullable
    private StatusCode.Code solo(FirestoreWrite write) throws Exception {
        return RejectionProbes.solo(access, write);
    }

    private List<StatusCode.Code> batch(List<FirestoreWrite> writes, @Nullable FirestoreWrite last)
            throws Exception {
        return RejectionProbes.batch(access, writes, last);
    }
}
