/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared harness for integration tests against the Bigtable emulator: the container, and
 * harness-owned admin and data clients for creating tables and reading rows back.
 *
 * <p>The emulator implements {@code MutateRows} and the table admin surface, which is what the sink
 * needs; it is a convenience for fast feedback and not evidence about the service, so nothing here
 * asserts a rejection the service would produce — the real-GCP suite ({@code #218}) owns that.
 *
 * <p>Each test creates a table of its own, so the tests neither see one another's rows nor depend
 * on an order.
 */
@Testcontainers
@Timeout(180)
abstract class AbstractBigtableEmulatorITCase {

    static final String PROJECT = "it-project";

    /** Emulator instances are opaque path segments; no instance has to exist. */
    static final String INSTANCE = "it-instance";

    static final String FAMILY = "cf";

    private static final DockerImageName IMAGE =
            DockerImageName.parse(
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    @Container
    static final BigtableEmulatorContainer EMULATOR = new BigtableEmulatorContainer(IMAGE);

    private static BigtableTableAdminClient adminClient;
    private static BigtableDataClient dataClient;

    @BeforeAll
    static void startClients() throws IOException {
        adminClient =
                BigtableTableAdminClient.create(
                        BigtableTableAdminSettings.newBuilderForEmulator(
                                        EMULATOR.getHost(), EMULATOR.getEmulatorPort())
                                .setProjectId(PROJECT)
                                .setInstanceId(INSTANCE)
                                .build());
        dataClient =
                BigtableDataClient.create(
                        BigtableDataSettings.newBuilderForEmulator(
                                        EMULATOR.getHost(), EMULATOR.getEmulatorPort())
                                .setProjectId(PROJECT)
                                .setInstanceId(INSTANCE)
                                .build());
    }

    @AfterAll
    static void stopClients() {
        if (dataClient != null) {
            dataClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    /** Returns the emulator endpoint in the {@code host:port} form the sink builder takes. */
    static String emulatorEndpoint() {
        return EMULATOR.getHost() + ":" + EMULATOR.getEmulatorPort();
    }

    /** Creates a table with the shared column family and returns its destination. */
    static TableDestination createTable(String tableId) {
        adminClient.createTable(CreateTableRequest.of(tableId).addFamily(FAMILY));
        return TableDestination.of(PROJECT, INSTANCE, tableId);
    }

    /** Reads every row of the table, in row-key order. */
    static List<Row> readRows(TableDestination destination) {
        List<Row> rows = new ArrayList<>();
        dataClient.readRows(Query.create(destination.getTable())).forEach(rows::add);
        return rows;
    }
}
