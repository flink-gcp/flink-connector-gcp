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

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.firestore.admin.v1.Database;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Shared harness for the gated integration tests that run against the real Firestore service in
 * Native mode: what the emulator cannot show, such as which status the service answers each refused
 * write with, how it partitions a collection group, and which read times it accepts. Every emulator
 * test passes an emulator endpoint, so this is also the only place the production client
 * construction over application-default credentials runs.
 *
 * <p>Clients authenticate with application-default credentials; the project comes from {@code
 * FIRESTORE_IT_PROJECT}. There is no companion variable, because nothing persistent is provisioned:
 * each class creates a Native-mode database of its own through {@link EphemeralDatabases} and
 * deletes it in {@link AfterAll}, together with any further database it created through {@link
 * #createDatabase(Database.DatabaseType)}. {@link EphemeralDatabases} records why per class and
 * what the service measured.
 *
 * <p>The gating annotations, {@code @EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT",
 * matches = ".+")} and {@code @Tag("gated")}, are on each concrete subclass rather than here:
 * {@code scripts/e2e-gated-its.sh} discovers the suite by parsing the annotation on each file and
 * then expects a surefire report per matching file, which an abstract class never produces, and
 * {@code --check-tags} checks both annotations per file.
 *
 * <p>The timeout runs in a separate thread because the default mode cannot end a wait that ignores
 * interruption; ADR-0119 records the measurement.
 */
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
public abstract class AbstractFirestoreRealGcpITCase {

    /** The project the suite runs against; null when the gate is off (the tests then skip). */
    protected static final String PROJECT = System.getenv("FIRESTORE_IT_PROJECT");

    /**
     * Every database the class created or tried to, in creation order. Copy-on-write, because a
     * test timed out on its own thread may still add to it while {@link AfterAll} reads it.
     */
    private static final List<String> CREATED = new CopyOnWriteArrayList<>();

    private static FirestoreAdminClient admin;
    private static DatabaseDestination database;
    private static Firestore client;

    @BeforeAll
    protected static void createDatabaseAndClient() throws Exception {
        admin = FirestoreAdminClient.create();
        EphemeralDatabases.sweepStale(admin, PROJECT);
        database = createDatabase(Database.DatabaseType.FIRESTORE_NATIVE);
        client = open(database);
    }

    @AfterAll
    protected static void deleteDatabasesAndCloseClients() throws Exception {
        try (AutoCloseable closeAdmin = () -> Closers.closeAll(admin)) {
            // Delete every database before closing the admin client that deletes them, each one
            // attempted whatever the one before it did.
            List<AutoCloseable> deletions = new ArrayList<>();
            deletions.add(() -> Closers.closeAll(client));
            for (String id : CREATED) {
                deletions.add(() -> EphemeralDatabases.delete(admin, PROJECT, id));
            }
            Closers.closeAll(deletions);
        } finally {
            CREATED.clear();
            admin = null;
            client = null;
            database = null;
        }
    }

    /**
     * Creates a further ephemeral database of the given mode, deleted with the class's own.
     *
     * @param type the database's mode
     * @return the new database
     */
    protected static DatabaseDestination createDatabase(Database.DatabaseType type)
            throws Exception {
        String id = EphemeralDatabases.newId();
        CREATED.add(id);
        EphemeralDatabases.create(admin, PROJECT, type, id);
        return DatabaseDestination.of(PROJECT, id);
    }

    /** Returns the class's Native-mode database. */
    protected static DatabaseDestination database() {
        return database;
    }

    /** Returns a client of the class's database, for arranging and reading documents directly. */
    protected static Firestore client() {
        return client;
    }

    /**
     * Opens a client of the given database over application-default credentials; the caller closes
     * it.
     */
    private static Firestore open(DatabaseDestination destination) {
        return FirestoreOptions.newBuilder()
                .setProjectId(destination.getProject())
                .setDatabaseId(destination.getDatabaseId())
                .build()
                .getService();
    }

    /** Returns a collection id no other test uses. */
    protected static String uniqueCollection() {
        return TestNames.unique("c").toLowerCase(Locale.ROOT);
    }

    /** Reads one document through the direct client. */
    protected static DocumentSnapshot read(String documentPath) throws Exception {
        return client.document(documentPath).get().get(30, TimeUnit.SECONDS);
    }
}
