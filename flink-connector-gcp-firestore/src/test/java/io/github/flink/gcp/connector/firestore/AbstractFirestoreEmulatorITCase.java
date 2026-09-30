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
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Base for the Firestore emulator integration tests: one emulator container per test class, and a
 * fresh collection per test so that tests sharing the container cannot see each other's documents.
 *
 * <p>An emulator is a convenience, never evidence about the service: where the two disagree, the
 * real service decides. The deviations these tests found are recorded on the connector's docs page.
 */
@Testcontainers
@Timeout(300)
public abstract class AbstractFirestoreEmulatorITCase {

    protected static final String PROJECT = "it-project";

    @Container
    static final FirestoreEmulatorContainer EMULATOR = FirestoreEmulatorContainers.newContainer();

    private static Firestore client;

    @BeforeAll
    static void openClient() {
        client =
                FirestoreOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setEmulatorHost(emulatorEndpoint())
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

    /** Returns a client of the emulator, for arranging and reading documents directly. */
    protected static Firestore client() {
        return client;
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
