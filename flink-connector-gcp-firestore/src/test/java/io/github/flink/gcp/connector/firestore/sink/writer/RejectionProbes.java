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
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Sends writes through the production {@link FirestoreDatabaseAccess} with retries disabled and
 * reports each one's status, for the rejection measurements against the emulator and the service.
 */
final class RejectionProbes {

    private RejectionProbes() {}

    /**
     * Opens an access to the database with a single attempt per write, both in the writer's retry
     * policy and in the transport's, so each outcome is the service's first answer; the emulator's
     * when an endpoint is given.
     */
    static FirestoreDatabaseAccess open(
            DatabaseDestination database, @Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return new DefaultFirestoreDatabaseAccessFactory(
                        database,
                        FirestoreWriterOptions.builder().retryMaxAttempts(1).build(),
                        emulatorEndpoint,
                        null)
                .create(
                        new BulkWriterRetryPolicy(
                                1,
                                new BulkWriterRetryPolicy.Observer() {
                                    @Override
                                    public void attemptFailed() {}

                                    @Override
                                    public void retrying() {}
                                }));
    }

    /** Sends one write as the only write of its request; returns its status, null if applied. */
    @Nullable
    static StatusCode.Code solo(FirestoreDatabaseAccess access, FirestoreWrite write)
            throws Exception {
        ApiFuture<WriteResult> future = access.submit(write);
        access.sendOutstanding();
        return outcome(future);
    }

    /**
     * Sends the writes, and {@code last} if given, as one request; returns their statuses in order,
     * null for each write applied.
     */
    static List<StatusCode.Code> batch(
            FirestoreDatabaseAccess access,
            List<FirestoreWrite> writes,
            @Nullable FirestoreWrite last)
            throws Exception {
        List<ApiFuture<WriteResult>> futures = new ArrayList<>();
        for (FirestoreWrite write : writes) {
            futures.add(access.submit(write));
        }
        if (last != null) {
            futures.add(access.submit(last));
        }
        access.sendOutstanding();
        List<StatusCode.Code> outcomes = new ArrayList<>();
        for (ApiFuture<WriteResult> future : futures) {
            outcomes.add(outcome(future));
        }
        return outcomes;
    }

    @Nullable
    private static StatusCode.Code outcome(ApiFuture<WriteResult> future) throws Exception {
        try {
            future.get(60, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return FirestoreErrorClassifier.statusCode(e.getCause());
        }
    }
}
