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

package io.github.flink.gcp.connector.bigtable;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
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
public abstract class AbstractBigtableEmulatorITCase {

    protected static final String PROJECT = "it-project";

    /** Emulator instances are opaque path segments; no instance has to exist. */
    protected static final String INSTANCE = "it-instance";

    protected static final String FAMILY = "cf";

    private static final DockerImageName IMAGE =
            DockerImageName.parse(
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    @Container
    protected static final BigtableEmulatorContainer EMULATOR =
            new BigtableEmulatorContainer(IMAGE);

    private static BigtableTableAdminClient adminClient;
    private static BigtableDataClient dataClient;

    @BeforeAll
    protected static void startClients() throws IOException {
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
    protected static void stopClients() {
        if (dataClient != null) {
            dataClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    /** Returns the emulator endpoint in the {@code host:port} form the sink builder takes. */
    protected static String emulatorEndpoint() {
        return EMULATOR.getHost() + ":" + EMULATOR.getEmulatorPort();
    }

    /** Creates a table with the shared column family and returns its destination. */
    protected static TableDestination createTable(String tableId) {
        return createTable(tableId, FAMILY);
    }

    /**
     * Creates a table with the given column families and returns its destination.
     *
     * @param tableId the table to create
     * @param families the column families it gets, at least one
     * @return the destination naming it
     */
    protected static TableDestination createTable(String tableId, String... families) {
        CreateTableRequest request = CreateTableRequest.of(tableId);
        for (String family : families) {
            request.addFamily(family);
        }
        adminClient.createTable(request);
        return TableDestination.of(PROJECT, INSTANCE, tableId);
    }

    /** Returns the live table description, for asserting what auto-creation actually made. */
    protected static com.google.cloud.bigtable.admin.v2.models.Table describeTable(String tableId) {
        return adminClient.getTable(tableId);
    }

    /** Reads every row of the table, in row-key order. */
    protected static List<Row> readRows(TableDestination destination) {
        List<Row> rows = new ArrayList<>();
        // The TargetId overload, as the production factory uses: Query.create(String) is
        // deprecated.
        dataClient.readRows(Query.create(TableId.of(destination.getTable()))).forEach(rows::add);
        return rows;
    }

    /**
     * Writes one cell per given row key, so a read test has something to find.
     *
     * <p>Through the harness's own client rather than through the sink: a source test that seeded
     * its rows with the sink under test would fail for two reasons at once.
     *
     * @param destination the table to write into
     * @param rowKeys the row keys to write, each with a single cell holding its own key as the
     *     value
     */
    protected static void seedRows(TableDestination destination, String... rowKeys) {
        for (String rowKey : rowKeys) {
            dataClient.mutateRow(
                    RowMutation.create(TableId.of(destination.getTable()), rowKey)
                            .setCell(FAMILY, "q", rowKey));
        }
    }

    /** Returns what the emulator answers {@code SampleRowKeys} with, for the deviation suite. */
    protected static List<KeyOffset> sampleRowKeys(TableDestination destination) {
        return dataClient.sampleRowKeys(TableId.of(destination.getTable()));
    }

    /** Reads one range directly, for the deviation suite to measure what the emulator does. */
    protected static List<Row> readRange(TableDestination destination, ByteStringRange range) {
        List<Row> rows = new ArrayList<>();
        dataClient
                .readRows(Query.create(TableId.of(destination.getTable())).range(range))
                .forEach(rows::add);
        return rows;
    }

    /** Reads the whole table under one filter, for the deviation suite. */
    protected static List<Row> readRows(TableDestination destination, Filters.Filter filter) {
        List<Row> rows = new ArrayList<>();
        dataClient
                .readRows(Query.create(TableId.of(destination.getTable())).filter(filter))
                .forEach(rows::add);
        return rows;
    }
}
