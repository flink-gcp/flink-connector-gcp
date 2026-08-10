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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the batch source against the emulator, inside a Flink MiniCluster.
 *
 * <p>This is the only coverage of the path a real job takes: the source is serialized into the job
 * graph, the enumerator opens a real batch transaction and asks the service to plan it, and every
 * reader rejoins that snapshot through the production client.
 *
 * <p>What the emulator does <em>not</em> show is on the connector's docs page. Two things matter
 * here: it ignores both partition hints, and its partitionability check is its own — it refuses
 * queries the real service accepts, so a query shape passing here is evidence about this connector
 * and not about Spanner.
 */
class SpannerSourceEmulatorITCase extends AbstractSpannerEmulatorITCase {

    private static final int ROWS = 50;

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void readsEveryRowOfAQuery(Dialect dialect) throws Exception {
        SpannerDatabase database = seededDatabase(dialect);

        List<Long> ids =
                run(database, SpannerReadOperation.query(Statement.of("SELECT id FROM singers")));

        assertThat(ids).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void readsEveryRowOfATableRead(Dialect dialect) throws Exception {
        SpannerDatabase database = seededDatabase(dialect);

        List<Long> ids =
                run(
                        database,
                        SpannerReadOperation.read(
                                "singers", KeySet.all(), Collections.singletonList("id")));

        assertThat(ids).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void readsOnlyTheKeysAKeySetNames() throws Exception {
        SpannerDatabase database = seededDatabase(Dialect.GOOGLE_STANDARD_SQL);

        List<Long> ids =
                run(
                        database,
                        SpannerReadOperation.read(
                                "singers",
                                KeySet.range(KeyRange.closedOpen(Key.of(10L), Key.of(13L))),
                                Collections.singletonList("id")));

        assertThat(ids).containsExactlyInAnyOrder(10L, 11L, 12L);
    }

    @Test
    void readsThroughAnIndex() throws Exception {
        SpannerDatabase database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        singersDdl(Dialect.GOOGLE_STANDARD_SQL),
                        "CREATE INDEX singers_by_name ON singers (name)");
        seed(database);

        List<Long> ids =
                run(
                        database,
                        SpannerReadOperation.readUsingIndex(
                                "singers",
                                "singers_by_name",
                                KeySet.all(),
                                Collections.singletonList("id")));

        assertThat(ids).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void readsAProjectionOfSeveralColumns() throws Exception {
        SpannerDatabase database = seededDatabase(Dialect.GOOGLE_STANDARD_SQL);

        List<Long> ids =
                run(
                        database,
                        SpannerReadOperation.read(
                                "singers", KeySet.all(), Arrays.asList("id", "name")),
                        // Reading a second column proves the projection reached the wire: a row
                        // that did not carry it would fail here rather than come back empty.
                        row -> row.getString("name").isEmpty() ? -1L : row.getLong("id"));

        assertThat(ids).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void anEmptyResultIsAJobThatFinishes() throws Exception {
        // The emulator plans an empty partition on every run, so an empty table exercises the same
        // path a normal one does — and a source that treated "no rows" as an error would hang here.
        SpannerDatabase database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL, singersDdl(Dialect.GOOGLE_STANDARD_SQL));

        List<Long> ids =
                run(database, SpannerReadOperation.query(Statement.of("SELECT id FROM singers")));

        assertThat(ids).isEmpty();
    }

    @Test
    void aStaleReadSeesTheDataThatWasCommittedBeforeIt() throws Exception {
        SpannerDatabase database = seededDatabase(Dialect.GOOGLE_STANDARD_SQL);

        // Exact staleness is one of the three bounds a batch read may take; the assertion is that
        // it reaches the service and the read succeeds, not that the emulator honours the delay.
        List<Long> ids =
                run(
                        database,
                        SpannerReadOperation.query(Statement.of("SELECT id FROM singers")),
                        SpannerSourceEmulatorITCase::id,
                        builder ->
                                builder.timestampBound(
                                        TimestampBound.ofExactStaleness(0, TimeUnit.SECONDS)));

        assertThat(ids).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void aPartitionHintIsAcceptedEvenThoughTheEmulatorIgnoresIt() throws Exception {
        // Measured 2026-08-10: the emulator plans two partitions whatever the hints say. What this
        // asserts is that asking for them changes nothing about the rows, which is the only claim
        // the emulator can support.
        SpannerDatabase database = seededDatabase(Dialect.GOOGLE_STANDARD_SQL);

        List<Long> ids =
                run(
                        database,
                        SpannerReadOperation.query(Statement.of("SELECT id FROM singers")),
                        SpannerSourceEmulatorITCase::id,
                        builder -> builder.maxPartitions(8).partitionSizeBytes(1024));

        assertThat(ids).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void aQueryTheServiceWillNotPartitionFailsTheJobWithItsOwnMessage() throws Exception {
        // The emulator's check is stricter than the service's, so what this pins is the
        // connector's half: the service's INVALID_ARGUMENT reaches the user unwrapped, naming the
        // read that could not be planned.
        SpannerDatabase database = seededDatabase(Dialect.GOOGLE_STANDARD_SQL);

        assertThatThrownBy(
                        () ->
                                run(
                                        database,
                                        SpannerReadOperation.query(
                                                Statement.of(
                                                        "SELECT COUNT(*) AS id FROM singers"))))
                .rootCause()
                .hasMessageContaining("INVALID_ARGUMENT")
                .hasMessageContaining("partitionable");
    }

    private static SpannerDatabase seededDatabase(Dialect dialect) throws Exception {
        SpannerDatabase database = createDatabase(dialect, singersDdl(dialect));
        seed(database);
        return database;
    }

    /** The one table these tests read, spelled the way each dialect spells it. */
    private static String singersDdl(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE singers (id bigint NOT NULL PRIMARY KEY, name varchar(64))"
                : "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)";
    }

    private static void seed(SpannerDatabase database) {
        List<Mutation> rows = new ArrayList<>(ROWS);
        for (long id = 0; id < ROWS; id++) {
            rows.add(
                    Mutation.newInsertOrUpdateBuilder("singers")
                            .set("id")
                            .to(id)
                            .set("name")
                            .to("singer-" + id)
                            .build());
        }
        client(database).write(rows);
    }

    private static List<Long> allIds() {
        return LongStream.range(0, ROWS).boxed().collect(Collectors.toList());
    }

    private static long id(Struct row) {
        return row.getLong("id");
    }

    private static List<Long> run(SpannerDatabase database, SpannerReadOperation operation)
            throws Exception {
        return run(database, operation, SpannerSourceEmulatorITCase::id);
    }

    private static List<Long> run(
            SpannerDatabase database, SpannerReadOperation operation, ReadId readId)
            throws Exception {
        return run(database, operation, readId, builder -> builder);
    }

    private static List<Long> run(
            SpannerDatabase database,
            SpannerReadOperation operation,
            ReadId readId,
            UnaryOperator<SpannerSourceBuilder<Long>> configure)
            throws Exception {
        Configuration configuration = new Configuration();
        // A retry would hide a read failure behind a green job, which is the one outcome these
        // tests must not produce.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<Long> collected =
                env.fromSource(
                                configure
                                        .apply(
                                                SpannerSource.<Long>builder()
                                                        .database(database)
                                                        .readOperation(operation)
                                                        .deserializer(new IdDeserializer(readId))
                                                        .emulatorEndpoint(emulatorEndpoint()))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "spanner")
                        .executeAndCollect()) {
            collected.forEachRemaining(ids::add);
        }
        return ids;
    }

    /** How one test turns a row into the id it asserts on. */
    @FunctionalInterface
    private interface ReadId extends java.io.Serializable {
        long apply(Struct row);
    }

    /** A deserializer built from a test's own row mapping. */
    private static final class IdDeserializer implements SpannerStructDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        private final ReadId readId;

        private IdDeserializer(ReadId readId) {
            this.readId = readId;
        }

        @Override
        @Nullable
        public Long deserialize(Struct row) {
            return readId.apply(row);
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
