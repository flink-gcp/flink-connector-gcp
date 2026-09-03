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

package io.github.flink.gcp.connector.bigtable;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableEmulatorContainers;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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

    @Container
    protected static final BigtableEmulatorContainer EMULATOR =
            BigtableEmulatorContainers.newContainer();

    private static BigtableTableAdminClient adminClient;
    private static BigtableDataClient dataClient;

    @BeforeAll
    protected static void startClients() throws IOException {
        adminClient = BigtableTestClients.adminClient(EMULATOR, PROJECT, INSTANCE);
        dataClient = BigtableTestClients.dataClient(EMULATOR, PROJECT, INSTANCE);
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

    /**
     * Writes one cell directly, for read tests that need a shape the sink cannot produce — an
     * undeclared family or qualifier, or a specific cell version.
     */
    protected static void writeCell(
            TableDestination destination,
            String rowKey,
            String family,
            String qualifier,
            String value) {
        dataClient.mutateRow(
                RowMutation.create(TableId.of(destination.getTable()), rowKey)
                        .setCell(family, qualifier, value));
    }

    /** Writes one cell under an arbitrary binary row key. */
    protected static void writeCell(
            TableDestination destination,
            ByteString rowKey,
            String family,
            String qualifier,
            String value) {
        dataClient.mutateRow(
                RowMutation.create(TableId.of(destination.getTable()), rowKey)
                        .setCell(family, qualifier, value));
    }

    /** Writes one cell at an explicit version timestamp, in microseconds. */
    protected static void writeCell(
            TableDestination destination,
            String rowKey,
            String family,
            String qualifier,
            long timestampMicros,
            String value) {
        dataClient.mutateRow(
                RowMutation.create(TableId.of(destination.getTable()), rowKey)
                        .setCell(family, qualifier, timestampMicros, value));
    }

    /** Returns what the emulator answers {@code SampleRowKeys} with, for the deviation suite. */
    protected static List<KeyOffset> sampleRowKeys(TableDestination destination) {
        return dataClient.sampleRowKeys(TableId.of(destination.getTable()));
    }

    /**
     * Appends to one cell through {@code ReadModifyWriteRow}, for the deviation suite. This is the
     * one write path on which the emulator still accepts an empty row key, so it is the only way
     * left to reach the state the service cannot produce.
     */
    protected static void appendCell(
            TableDestination destination, String rowKey, String qualifier, String value) {
        dataClient.readModifyWriteRow(
                ReadModifyWriteRow.create(TableId.of(destination.getTable()), rowKey)
                        .append(FAMILY, qualifier, value));
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
