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

package io.github.flink.gcp.connector.firestore;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.DatabaseName;
import com.google.firestore.admin.v1.ProjectName;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates, deletes and sweeps the ephemeral named databases the gated real-service suite runs in
 * (#1546). Public, and the mode a parameter, because the Datastore-mode harness shares this naming
 * scheme and this sweep from the other package root.
 *
 * <p>A database is created per gated class rather than kept standing, for the reasons {@code
 * docs/adr/0044} gives for Bigtable: a crashed run leaves nothing that the next one reads, two
 * forks of the integration-test execution never share one, and a class stays runnable by hand. The
 * service makes this cheap: measured on 2026-10-10 in {@code us-central1}, a create took 2.0 to 2.6
 * seconds in either mode, a delete of a database holding at most one document 2.3 to 13.4 seconds,
 * and a write four seconds after the create was applied. An id cannot be reused for about five
 * minutes after its database is deleted (the service answered {@code FAILED_PRECONDITION: Database
 * ID '…' is not available in project '…'. Please retry in 298 seconds.}), which the per-creation
 * timestamp and run id in every name avoid.
 *
 * <p>Teardown deletes what a class created. A crash can bypass it, so every id carries its creation
 * time and {@link #sweepStale(FirestoreAdminClient, String)} deletes this suite's databases older
 * than {@link #STALE_AFTER} before a class creates its own. {@code scripts/sweep-e2e.sh} does not
 * sweep these databases, and that is deliberate: it exists for fixtures billed while they stand,
 * such as a Bigtable instance, and an abandoned database bills only for the few kilobytes it
 * stores. The next gated class's sweep reclaims it.
 */
public final class EphemeralDatabases {

    private static final Logger LOG = LoggerFactory.getLogger(EphemeralDatabases.class);

    /**
     * Identifies a database as this suite's, for the sweep. A database id is 4–63 characters of
     * lowercase letters, digits and hyphens, starting with a letter and ending with a letter or a
     * digit; {@code flink-it-} plus ten digits of epoch seconds, an eight-character run id and a
     * sequence number fits with room to spare.
     */
    private static final String DATABASE_PREFIX = "flink-it-";

    /** Where the databases go: the region the other IT resources already use. */
    private static final String LOCATION = "us-central1";

    /** A database older than this belongs to a run that crashed; see the class javadoc. */
    private static final Duration STALE_AFTER = Duration.ofHours(2);

    /** How long a delete waits for its database to stop being served. */
    private static final Duration ABSENCE_WAIT = Duration.ofMinutes(2);

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private static final String RUN_ID = TestNames.runId();

    private static final AtomicInteger SEQUENCE = new AtomicInteger();

    private EphemeralDatabases() {}

    /**
     * Returns a new database id carrying this moment's epoch seconds, for {@link
     * #create(FirestoreAdminClient, String, Database.DatabaseType, String)}. A caller records the
     * id before creating the database, so a create that fails after the service accepted it still
     * leaves an id to delete.
     *
     * @return a database id no other run or creation uses
     */
    public static String newId() {
        return DATABASE_PREFIX
                + Instant.now().getEpochSecond()
                + "-"
                + RUN_ID
                + "-"
                + SEQUENCE.incrementAndGet();
    }

    /**
     * Creates a Standard-edition database of the given mode, with delete protection and
     * point-in-time recovery off.
     *
     * @param admin the admin client
     * @param project the project to create it in
     * @param type the database's mode
     * @param id the database's id, from {@link #newId()}
     */
    public static void create(
            FirestoreAdminClient admin, String project, Database.DatabaseType type, String id)
            throws ExecutionException, InterruptedException {
        LOG.info("Creating ephemeral {} database {} in {}", type, id, LOCATION);
        admin.createDatabaseAsync(
                        ProjectName.of(project),
                        Database.newBuilder()
                                .setLocationId(LOCATION)
                                .setType(type)
                                // Set rather than defaulted: the edition decides some of what the
                                // suite measures, such as whether an array may hold an array.
                                .setDatabaseEdition(Database.DatabaseEdition.STANDARD)
                                .setDeleteProtectionState(
                                        Database.DeleteProtectionState.DELETE_PROTECTION_DISABLED)
                                .setPointInTimeRecoveryEnablement(
                                        Database.PointInTimeRecoveryEnablement
                                                .POINT_IN_TIME_RECOVERY_DISABLED)
                                .build(),
                        id)
                .get();
    }

    /**
     * Deletes a database this suite created; a database already gone is not an error.
     *
     * <p>Only the service's acceptance of the delete is awaited, then the database's absence, not
     * the deletion's long-running operation. Measured on 2026-10-10: the operation of a database
     * that held about 11 MiB had not finished when the client library's polling gave up at its
     * five-minute ceiling, which cancels the future; the database was no longer listed when the run
     * ended, and a database whose delete was accepted is gone from {@code getDatabase} within a
     * second.
     *
     * @param admin the admin client
     * @param project the project it is in
     * @param id the database's id
     */
    public static void delete(FirestoreAdminClient admin, String project, String id)
            throws ExecutionException, InterruptedException {
        DatabaseName name = DatabaseName.of(project, id);
        try {
            admin.deleteDatabaseAsync(name).getInitialFuture().get();
        } catch (ExecutionException e) {
            if (isNotFound(e.getCause())) {
                return;
            }
            throw e;
        }
        long started = System.nanoTime();
        while (System.nanoTime() - started < ABSENCE_WAIT.toNanos()) {
            try {
                admin.getDatabase(name);
            } catch (ApiException e) {
                if (isNotFound(e)) {
                    LOG.info(
                            "Database {} gone {} ms after its delete was accepted",
                            id,
                            (System.nanoTime() - started) / 1_000_000);
                } else {
                    // The delete was accepted; the wait only confirms it, so a failed look is no
                    // reason to fail the class whose tests already ran.
                    LOG.warn("Could not confirm that database {} is gone", id, e);
                }
                return;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        LOG.warn("Database {} still present {} after its delete was accepted", id, ABSENCE_WAIT);
    }

    private static boolean isNotFound(Throwable failure) {
        return failure instanceof ApiException
                && ((ApiException) failure).getStatusCode().getCode() == StatusCode.Code.NOT_FOUND;
    }

    /**
     * Deletes this suite's databases older than {@link #STALE_AFTER}, so a run killed before its
     * teardown leaves a database that the next run removes rather than one standing indefinitely.
     *
     * <p>Two forks sweeping at once can both pick the same database; the loser's delete fails and
     * is logged. An id that carries no parsable timestamp is left alone: this deletes databases,
     * and a name it cannot date is a name it does not understand.
     *
     * @param admin the admin client
     * @param project the project to sweep
     */
    public static void sweepStale(FirestoreAdminClient admin, String project) {
        Instant cutoff = Instant.now().minus(STALE_AFTER);
        for (Database database : admin.listDatabases(ProjectName.of(project)).getDatabasesList()) {
            String id = DatabaseName.parse(database.getName()).getDatabase();
            Instant created = createdAt(id);
            if (created == null || !created.isBefore(cutoff)) {
                continue;
            }
            LOG.warn("Sweeping stale database {}, created {}", id, created);
            try {
                delete(admin, project, id);
            } catch (ExecutionException | RuntimeException e) {
                LOG.warn("Failed to sweep {}", id, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** The creation time encoded in a database id, or null if this suite did not create it. */
    @Nullable
    private static Instant createdAt(String id) {
        if (!id.startsWith(DATABASE_PREFIX)) {
            return null;
        }
        String remainder = id.substring(DATABASE_PREFIX.length());
        int end = remainder.indexOf('-');
        try {
            return Instant.ofEpochSecond(
                    Long.parseLong(end < 0 ? remainder : remainder.substring(0, end)));
        } catch (RuntimeException e) {
            return null;
        }
    }
}
