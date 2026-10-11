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

package io.github.flink.gcp.connector.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.datastore.v1.PartitionId;
import com.google.firestore.admin.v1.Database;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.firestore.EphemeralDatabases;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Shared harness for the gated integration tests that run against the real Firestore service in
 * Datastore mode: what the emulator cannot show, such as which status the service answers each
 * refused commit with, how the splitter cuts a kind, and how the service orders and pages keys. It
 * is also the only place the Datastore-mode client construction over application-default
 * credentials runs.
 *
 * <p>The Native-mode harness's counterpart, sharing {@link EphemeralDatabases}: the project comes
 * from {@code FIRESTORE_IT_PROJECT}, and each class creates a Datastore-mode database of its own
 * and deletes it in {@link AfterAll}, together with any further database it created through {@link
 * #createDatabase(Database.DatabaseType)}.
 *
 * <p>The gating annotations, {@code @EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT",
 * matches = ".+")} and {@code @Tag("gated")}, are on each concrete subclass rather than here, for
 * the reason {@code AbstractFirestoreRealGcpITCase} gives.
 */
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
public abstract class AbstractDatastoreRealGcpITCase {

    /** The project the suite runs against; null when the gate is off (the tests then skip). */
    protected static final String PROJECT = System.getenv("FIRESTORE_IT_PROJECT");

    /**
     * Every database the class created or tried to, in creation order. Copy-on-write, because a
     * test timed out on its own thread may still add to it while {@link AfterAll} reads it.
     */
    private static final List<String> CREATED = new CopyOnWriteArrayList<>();

    private static FirestoreAdminClient admin;
    private static DatabaseDestination database;
    private static Datastore client;

    @BeforeAll
    protected static void createDatabaseAndClient() throws Exception {
        admin = FirestoreAdminClient.create();
        EphemeralDatabases.sweepStale(admin, PROJECT);
        database = createDatabase(Database.DatabaseType.DATASTORE_MODE);
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

    /** Returns the class's Datastore-mode database. */
    protected static DatabaseDestination database() {
        return database;
    }

    /** Returns a client of the class's database, for arranging and reading entities directly. */
    protected static Datastore client() {
        return client;
    }

    /** Returns the client's RPC layer, for a request the client does not expose. */
    protected static DatastoreRpc rpc() {
        return (DatastoreRpc) client.getOptions().getRpc();
    }

    /** Returns the class's database as a request's partition, in the default namespace. */
    protected static PartitionId partition() {
        return PartitionId.newBuilder()
                .setProjectId(PROJECT)
                .setDatabaseId(database.getDatabaseId())
                .build();
    }

    /** Encodes a key of the class's database as the protobuf a raw request carries. */
    protected static com.google.datastore.v1.Key keyProto(Key key) {
        return com.google.datastore.v1.Key.newBuilder()
                .setPartitionId(partition())
                .addPath(
                        com.google.datastore.v1.Key.PathElement.newBuilder()
                                .setKind(key.getKind())
                                .setName(key.getName()))
                .build();
    }

    /** Returns a kind no other test uses. */
    protected static String uniqueKind() {
        return TestNames.unique("K");
    }

    /** Returns a key of the given kind with a name, in the class's database. */
    protected static Key key(String kind, String name) {
        return Key.newBuilder(PROJECT, kind, name, database.getDatabaseId()).build();
    }

    /** Reads one entity through the direct client, or {@code null} when there is none. */
    @Nullable
    protected static Entity read(Key key) {
        return client.get(key);
    }

    private static Datastore open(DatabaseDestination destination) {
        return DatastoreOptions.newBuilder()
                .setProjectId(destination.getProject())
                .setDatabaseId(destination.getDatabaseId())
                .build()
                .getService();
    }
}
