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

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Base for the Datastore-mode emulator integration tests: one emulator container per test class,
 * started in Datastore mode, and a fresh kind per test so that tests sharing the container cannot
 * see each other's entities.
 *
 * <p>The container is the Firestore emulator under {@code --database-mode=datastore-mode}; the
 * legacy Datastore emulator, which Google's documentation directs Datastore-mode users away from,
 * is not used. An emulator is a convenience, never evidence about the service: where the two
 * disagree, the real service decides. The deviations these tests found are recorded on the
 * connector's docs page.
 */
@Testcontainers
@Timeout(300)
public abstract class AbstractDatastoreEmulatorITCase {

    protected static final String PROJECT = "it-project";

    @Container
    static final FirestoreEmulatorContainer EMULATOR =
            FirestoreEmulatorContainers.newContainer().withFlags("--database-mode=datastore-mode");

    private static Datastore client;

    @BeforeAll
    static void openClient() {
        client =
                DatastoreOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setHost(emulatorEndpoint())
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService();
    }

    @AfterAll
    static void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    /** Returns the emulator's endpoint as {@code host:port}. */
    protected static String emulatorEndpoint() {
        return EMULATOR.getEmulatorEndpoint();
    }

    /** Returns the database the tests write to. */
    protected static DatabaseDestination database() {
        return DatabaseDestination.of(PROJECT);
    }

    /** Returns a client of the emulator, for arranging and reading entities directly. */
    protected static Datastore client() {
        return client;
    }

    /** Returns a kind no other test uses. */
    protected static String uniqueKind() {
        return TestNames.unique("K");
    }

    /** Returns a key of the given kind with a name, in the tests' database. */
    protected static Key key(String kind, String name) {
        return Key.newBuilder(PROJECT, kind, name).build();
    }

    /** Reads every entity of a kind through the direct client. */
    protected static List<Entity> readAll(String kind) {
        QueryResults<Entity> results =
                client.run(Query.newEntityQueryBuilder().setKind(kind).build());
        List<Entity> entities = new ArrayList<>();
        results.forEachRemaining(entities::add);
        return entities;
    }

    /** Reads one entity through the direct client, or {@code null} when there is none. */
    @Nullable
    protected static Entity read(Key key) {
        return client.get(key);
    }
}
