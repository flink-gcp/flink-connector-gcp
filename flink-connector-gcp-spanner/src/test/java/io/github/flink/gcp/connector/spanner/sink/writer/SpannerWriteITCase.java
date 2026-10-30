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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.sink.FailedMutation;
import io.github.flink.gcp.connector.spanner.sink.OrderingMode;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.SpannerSinkBuilder;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the write path against the Spanner emulator, run against both dialects.
 *
 * <p>These go through the sink's production {@code createWriter(WriterInitContext)}, so the client,
 * the schema read and the batch write are all the real ones.
 *
 * <p>Every identifier here is lower case and unquoted on purpose. GoogleSQL matches identifiers
 * case-insensitively and PostgreSQL folds unquoted ones to lower case, so one set of names — and
 * therefore one set of mutations and one query — serves both dialects.
 *
 * <p>Writers are closed from {@link #closeWriters()} rather than at the end of each test. Each one
 * holds a real {@code Spanner} service handle with its channels and session pool, integration tests
 * share one JVM across classes, and a writer left open by a failing assertion would turn one real
 * failure into a cascade of unrelated ones.
 */
class SpannerWriteITCase extends AbstractSpannerEmulatorITCase {

    private final List<SinkWriter<String>> writers = new ArrayList<>();

    /** The context the last writer was created with, whose metrics a test can read back. */
    private StubWriterInitContext lastContext;

    @AfterEach
    void closeWriters() throws Exception {
        List<AutoCloseable> open = new ArrayList<>(writers);
        writers.clear();
        Closers.closeAll(open);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void appliesEveryMutationOfABatch(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);
        SinkWriter<String> writer = writer(database, SpannerWriterOptions.defaults(), null);

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.write("c", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(names(database)).containsExactly("a", "b", "c");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void sendsSeveralBatchesWhenTheLimitIsSmall(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);
        SinkWriter<String> writer =
                writer(database, SpannerWriterOptions.builder().maxBatchMutations(2).build(), null);

        for (String name : new String[] {"a", "b", "c", "d", "e"}) {
            writer.write(name, TestContexts.NO_OP);
        }
        writer.flush(false);

        assertThat(names(database)).containsExactly("a", "b", "c", "d", "e");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void aReplayedInsertOrUpdateIsHarmless(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);
        SinkWriter<String> writer = writer(database, SpannerWriterOptions.defaults(), null);

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);
        // The same record again, as a restart from an earlier checkpoint would deliver it.
        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(names(database)).containsExactly("a");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void routesTheOneMutationTheServiceRefusesAndKeepsTheRest(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);
        // Seed the row the insert below collides with.
        client(database).write(List.of(insert("b")));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        database,
                        SpannerWriterOptions.defaults(),
                        handler,
                        (element, context) -> insert(element));

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.write("c", TestContexts.NO_OP);
        writer.flush(false);

        // The refusal is per group: the other two mutations of the same request still landed,
        // which is what batchWriteAtLeastOnce buys over a plain commit.
        assertThat(names(database)).containsExactly("a", "b", "c");
        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getErrorMessage()).contains("ALREADY_EXISTS");
        assertThat(handler.handled.get(0).getTable()).isEqualTo("orders");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void deletesRowsToo(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);
        client(database).write(List.of(insert("a"), insert("b")));
        SinkWriter<String> writer =
                writer(
                        database,
                        SpannerWriterOptions.defaults(),
                        null,
                        (element, context) -> Mutation.delete("orders", Key.of(element)));

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(names(database)).containsExactly("b");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void readsTheSecondaryIndexCoverageOutOfTheRealSchema(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);

        CellWeights weights;
        try (SpannerDatabaseAccess access =
                new DefaultSpannerDatabaseAccessFactory(
                                database,
                                SpannerWriterOptions.defaults(),
                                EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"))
                        .create()) {
            weights = access.readCellWeights();
        }

        // One secondary index covers name, and the primary-key index is excluded — so a mutation
        // writing id and name costs 1 + 2 rather than 2.
        assertThat(weights.knows("orders")).isTrue();
        assertThat(weights.weigh(insert("a"))).isEqualTo(3);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void perKeyOrderAppliesEachRowsMutationsInTheOrderReceived(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);
        // One batch could hold every mutation below, so only the per-key flush separates them.
        SinkWriter<String> writer =
                writer(
                        database,
                        SpannerWriterOptions.builder().orderingMode(OrderingMode.PER_KEY).build(),
                        null,
                        SpannerWriteITCase::roundMutation);

        List<String> expected = writeRounds(writer);
        writer.flush(false);

        assertThat(names(database)).containsExactlyElementsOf(expected);
        // One request per round: every round repeats each row once. The emulator applies one
        // request's groups in order, so this count, not the rows, is what fails when the primary
        // keys read at open do not reach the writer (each mutation would then be its own request).
        assertThat(
                        lastContext
                                .getSinkWriterMetricGroup()
                                .counterValue(SpannerMetricNames.BATCHES_SENT))
                .isEqualTo((long) ROUNDS);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void readsThePrimaryKeysOutOfTheRealSchema(Dialect dialect) throws Exception {
        DatabaseDestination database = ordersDatabase(dialect);

        PrimaryKeys keys;
        try (SpannerDatabaseAccess access =
                new DefaultSpannerDatabaseAccessFactory(
                                database,
                                SpannerWriterOptions.defaults(),
                                EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"))
                        .create()) {
            keys = access.readPrimaryKeys();
        }

        // The secondary index on name is not the key: a write names its row by id alone.
        Mutation write =
                Mutation.newInsertOrUpdateBuilder("orders")
                        .set("id")
                        .to("a")
                        .set("name")
                        .to("z")
                        .build();
        assertThat(keys.footprintOf(write).isWholeTable()).isFalse();
        BatchedRows batch = new BatchedRows();
        batch.add(keys.footprintOf(write));
        assertThat(batch.overlaps(keys.footprintOf(Mutation.delete("orders", Key.of("a")))))
                .isTrue();
        assertThat(batch.overlaps(keys.footprintOf(Mutation.delete("orders", Key.of("z")))))
                .isFalse();
    }

    @Test
    void perKeyOrdersATableWithNoKeyColumnAgainstItselfOnly() throws Exception {
        // GoogleSQL only: PostgreSQL databases cannot declare a table without a primary key.
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE orders (id STRING(64) NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (id)",
                        "CREATE TABLE singleton (v STRING(64)) PRIMARY KEY ()");
        SinkWriter<String> writer =
                writer(
                        database,
                        SpannerWriterOptions.builder().orderingMode(OrderingMode.PER_KEY).build(),
                        null,
                        SpannerWriteITCase::ordersOrSingleton);

        for (String element : new String[] {"o:a", "s:x", "o:b", "s:y", "o:c"}) {
            writer.write(element, TestContexts.NO_OP);
        }
        writer.flush(false);

        assertThat(names(database)).containsExactly("a", "b", "c");
        assertThat(query(database, "SELECT v FROM singleton"))
                .singleElement()
                .satisfies(row -> assertThat(row.getString(0)).isEqualTo("y"));
        // The second singleton write repeats the table's one row, so the batch is sent before it,
        // and nothing else is: two requests. A table the primary-key read does not list is ordered
        // against the whole batch instead, which sends each singleton write alone, between two
        // flushes: five requests.
        assertThat(
                        lastContext
                                .getSinkWriterMetricGroup()
                                .counterValue(SpannerMetricNames.BATCHES_SENT))
                .isEqualTo(2L);
    }

    // ---------------------------------------------------------------- helpers

    private static final int ROWS = 20;
    private static final int ROUNDS = 10;

    /**
     * Writes {@value #ROUNDS} rounds over {@value #ROWS} rows, each round touching every row once,
     * with a third of the changes deletes. Returns the names the rows end with, in id order.
     */
    private static List<String> writeRounds(SinkWriter<String> writer) throws Exception {
        List<String> expected = new ArrayList<>();
        for (int round = 0; round < ROUNDS; round++) {
            for (int row = 0; row < ROWS; row++) {
                boolean delete = (row + round) % 3 == 0;
                writer.write(
                        String.format("r%02d:%d:%s", row, round, delete ? "delete" : "upsert"),
                        TestContexts.NO_OP);
            }
        }
        for (int row = 0; row < ROWS; row++) {
            int last = ROUNDS - 1;
            if ((row + last) % 3 != 0) {
                expected.add(String.format("r%02d:%d", row, last));
            }
        }
        return expected;
    }

    /** Turns {@code id:round:op} into an upsert naming the round, or a delete of the row. */
    private static Mutation roundMutation(String element, SinkWriter.Context context) {
        String[] parts = element.split(":");
        if (parts[2].equals("delete")) {
            return Mutation.delete("orders", Key.of(parts[0]));
        }
        return Mutation.newInsertOrUpdateBuilder("orders")
                .set("id")
                .to(parts[0])
                .set("name")
                .to(parts[0] + ":" + parts[1])
                .build();
    }

    /** Turns {@code o:id} into an upsert of {@code orders} and {@code s:v} into the singleton. */
    private static Mutation ordersOrSingleton(String element, SinkWriter.Context context) {
        String[] parts = element.split(":");
        if (parts[0].equals("s")) {
            return Mutation.newInsertOrUpdateBuilder("singleton").set("v").to(parts[1]).build();
        }
        return insertOrUpdate(parts[1]);
    }

    private static DatabaseDestination ordersDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE orders (id varchar(64) NOT NULL PRIMARY KEY, name varchar(64))",
                    "CREATE INDEX orders_by_name ON orders (name)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE orders (id STRING(64) NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                "CREATE INDEX orders_by_name ON orders (name)");
    }

    private SinkWriter<String> writer(
            DatabaseDestination database,
            SpannerWriterOptions options,
            @Nullable FailureHandler<? super FailedMutation> handler)
            throws Exception {
        return writer(database, options, handler, (element, context) -> insertOrUpdate(element));
    }

    private SinkWriter<String> writer(
            DatabaseDestination database,
            SpannerWriterOptions options,
            @Nullable FailureHandler<? super FailedMutation> handler,
            SpannerMutationSerializationSchema<String> serializer)
            throws Exception {
        SpannerSinkBuilder<String> builder =
                SpannerSink.<String>builder()
                        .database(database)
                        .serializer(serializer)
                        .writerOptions(options)
                        .emulatorEndpoint(emulatorEndpoint());
        if (handler != null) {
            builder.failedMutationHandler(handler);
        }
        Sink<String> sink = builder.build();
        lastContext = new StubWriterInitContext(0);
        SinkWriter<String> writer = sink.createWriter(lastContext);
        writers.add(writer);
        return writer;
    }

    private static Mutation insertOrUpdate(String element) {
        return Mutation.newInsertOrUpdateBuilder("orders")
                .set("id")
                .to(element)
                .set("name")
                .to(element)
                .build();
    }

    private static Mutation insert(String element) {
        return Mutation.newInsertBuilder("orders")
                .set("id")
                .to(element)
                .set("name")
                .to(element)
                .build();
    }

    private static List<String> names(DatabaseDestination database) {
        List<String> names = new ArrayList<>();
        for (Struct row : query(database, "SELECT name FROM orders ORDER BY id")) {
            names.add(row.getString(0));
        }
        return names;
    }

    /** Collects what the writer routed. */
    private static final class RecordingHandler implements FailureHandler<FailedMutation> {

        private static final long serialVersionUID = 1L;

        private final transient List<FailedMutation> handled = new ArrayList<>();

        @Override
        public void handle(FailedMutation element) {
            handled.add(element);
        }
    }
}
