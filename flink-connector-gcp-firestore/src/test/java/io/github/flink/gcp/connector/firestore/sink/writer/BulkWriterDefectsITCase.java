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

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.BulkWriterOptions;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the two {@code BulkWriter} defects {@link FirestoreWriter} works around, against the client
 * library as the build resolves it — so a release that fixes either fails here, and the workaround
 * is then re-examined rather than kept on faith (google-cloud-firestore 3.46.0, measured
 * 2026-09-27).
 *
 * <p>Both are client-side logic, so the emulator is a faithful stage for them. Each asserts an
 * absence with a short wait beside a positive control that completes in milliseconds on the same
 * emulator: the wait bounds how long the absence is observed, not how long a healthy write takes.
 */
class BulkWriterDefectsITCase extends AbstractFirestoreEmulatorITCase {

    /** How long a write that the defect strands is watched for, beside a control that answers. */
    private static final long ABSENCE_SECONDS = 3;

    private ScheduledExecutorService executor;
    private String collection;

    @BeforeEach
    void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        collection = uniqueCollection();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void failedWritesKeepTheirPendingSlotsUntilNothingMoreIsSent() throws Exception {
        List<ApiFuture<WriteResult>> seed = new ArrayList<>();
        BulkWriter seeding = bulkWriter();
        for (int i = 0; i < FirestoreWriter.PENDING_OPERATION_LIMIT; i++) {
            seed.add(seeding.set(client().document(collection + "/d" + i), Map.of("v", 1L)));
        }
        seeding.flush();
        for (ApiFuture<WriteResult> future : seed) {
            future.get(30, TimeUnit.SECONDS);
        }

        BulkWriter bulkWriter = bulkWriter();
        refuse(bulkWriter, 0, FirestoreWriter.PENDING_OPERATION_LIMIT - 1);

        // One slot short of the ceiling the same BulkWriter still sends: the ceiling is 500.
        ApiFuture<WriteResult> below =
                bulkWriter.set(client().document(collection + "/below"), Map.of("v", 1L));
        bulkWriter.flush();
        assertThat(below.get(30, TimeUnit.SECONDS)).isNotNull();

        refuse(bulkWriter, FirestoreWriter.PENDING_OPERATION_LIMIT - 1, 1);
        ApiFuture<WriteResult> stranded =
                bulkWriter.set(client().document(collection + "/after"), Map.of("v", 1L));
        bulkWriter.flush();
        assertThatThrownBy(() -> stranded.get(ABSENCE_SECONDS, TimeUnit.SECONDS))
                .as("a write after %s failures", FirestoreWriter.PENDING_OPERATION_LIMIT)
                .isInstanceOf(TimeoutException.class);

        BulkWriter replacement = bulkWriter();
        ApiFuture<WriteResult> control =
                replacement.set(client().document(collection + "/control"), Map.of("v", 1L));
        replacement.flush();
        assertThat(control.get(30, TimeUnit.SECONDS)).isNotNull();
    }

    @Test
    void aSynchronousRefusalStrandsAWriteItsRequestApplied() throws Exception {
        BulkWriter bulkWriter = bulkWriter();
        ApiFuture<WriteResult> before =
                bulkWriter.set(client().document(collection + "/before"), Map.of("v", 1L));
        assertThatThrownBy(
                        () ->
                                bulkWriter.set(
                                        client().document(collection + "/refused"),
                                        Map.of("v", (short) 1)))
                .isInstanceOf(IllegalArgumentException.class);
        ApiFuture<WriteResult> after =
                bulkWriter.set(client().document(collection + "/after"), Map.of("v", 1L));
        bulkWriter.flush();

        assertThat(before.get(30, TimeUnit.SECONDS)).isNotNull();
        assertThatThrownBy(() -> after.get(ABSENCE_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);
        assertThat(read(collection + "/after").exists()).isTrue();
    }

    /** Sends {@code count} creates of seeded documents from {@code first}, each refused. */
    private void refuse(BulkWriter bulkWriter, int first, int count) {
        List<ApiFuture<WriteResult>> refused = new ArrayList<>();
        for (int i = first; i < first + count; i++) {
            refused.add(bulkWriter.create(client().document(collection + "/d" + i), Map.of()));
        }
        bulkWriter.flush();
        for (ApiFuture<WriteResult> future : refused) {
            assertThatThrownBy(() -> future.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
        }
    }

    private BulkWriter bulkWriter() {
        return client().bulkWriter(BulkWriterOptions.builder().setExecutor(executor).build());
    }
}
