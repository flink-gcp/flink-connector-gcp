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
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;

/**
 * Commits mutations through the production {@link DatastoreDatabaseAccess}, whose client makes one
 * attempt per call, and reports each commit's status, for the rejection measurements against the
 * emulator and the service.
 */
final class RejectionProbes {

    private RejectionProbes() {}

    /**
     * Opens an access to the emulator's database, with the 30-second request timeout the emulator
     * tests have always used.
     */
    static DatastoreDatabaseAccess open(DatabaseDestination database, EmulatorEndpoint emulator)
            throws Exception {
        return new DefaultDatastoreDatabaseAccessFactory(
                        database, Duration.ofSeconds(30), emulator, null)
                .create();
    }

    /** Opens an access to the service's database. */
    static DatastoreDatabaseAccess open(DatabaseDestination database) throws Exception {
        return service(database).create();
    }

    /**
     * The production access factory for the service, with a 60-second request timeout: a commit of
     * about 10.5 MiB took longer than 30 seconds to send from a residential uplink.
     */
    static DefaultDatastoreDatabaseAccessFactory service(DatabaseDestination database) {
        return new DefaultDatastoreDatabaseAccessFactory(
                database, Duration.ofSeconds(60), null, null);
    }

    /**
     * Commits the mutations as one request and returns the status name, {@code "applied"}, or the
     * failure's own description when it carries no status, so an assertion names it.
     */
    static String outcome(DatastoreDatabaseAccess access, DatastoreMutation... writes) {
        try {
            access.commit(List.of(writes));
            return "applied";
        } catch (RuntimeException e) {
            @Nullable StatusCode.Code code = DatastoreErrorClassifier.statusCode(e);
            return code != null ? code.name() : "no status: " + e;
        }
    }
}
