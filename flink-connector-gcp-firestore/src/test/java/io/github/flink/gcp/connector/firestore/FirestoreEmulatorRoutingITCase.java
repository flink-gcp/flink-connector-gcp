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

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An emulator client's calls carry no {@code x-goog-request-params} header, since its channel is
 * the connector's rather than one gax built (#2343, docs/adr/0208); the emulator routes each
 * request by the database the request names. A document the connector writes to a named database is
 * found there by the client library, which sends the header, and not in the default database.
 *
 * <p>The library's clients here run on gRPC's shared event-loop group, which is why this is not
 * part of {@code FirestoreEventLoopITCase}.
 */
@Testcontainers
@Timeout(120)
class FirestoreEmulatorRoutingITCase {

    @Container
    static final FirestoreEmulatorContainer EMULATOR = FirestoreEmulatorContainers.newContainer();

    @Test
    void aDocumentWrittenToANamedDatabaseIsFoundOnlyThere() throws Exception {
        DatabaseDestination named = DatabaseDestination.of("p", "named");
        EmulatorEndpoint endpoint =
                EmulatorEndpoint.parse(EMULATOR.getEmulatorEndpoint(), "emulatorEndpoint");
        String path = TestNames.unique("c").toLowerCase(Locale.ROOT) + "/d";
        try (GrpcEventLoops.LeasedClient<Firestore> client =
                FirestoreClients.open(
                        named, endpoint, FirestoreClients.settings(named, endpoint, null, null))) {
            client.client().document(path).set(Map.of("n", 1L)).get(30, TimeUnit.SECONDS);
        }

        for (String database : new String[] {named.getDatabaseId(), "(default)"}) {
            try (Firestore library =
                    FirestoreOptions.newBuilder()
                            .setProjectId("p")
                            .setDatabaseId(database)
                            .setCredentials(new FirestoreOptions.EmulatorCredentials())
                            .setEmulatorHost(EMULATOR.getEmulatorEndpoint())
                            .build()
                            .getService()) {
                assertThat(library.document(path).get().get(30, TimeUnit.SECONDS).exists())
                        .as("the document in database %s", database)
                        .isEqualTo(database.equals(named.getDatabaseId()));
            }
        }
    }
}
