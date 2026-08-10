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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.source.query.BigQueryQueryRunner;
import io.github.flink.gcp.connector.bigquery.source.query.QueryJobIdentity;
import io.github.flink.gcp.connector.bigquery.source.query.QueryResult;
import io.github.flink.gcp.connector.bigquery.source.query.QuerySpec;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The query path against BigQuery itself, which is the only place it can be covered.
 *
 * <p>The emulator cannot stand in here on either half: the query job is a REST call it answers
 * differently, and the anonymous result dataset is BigQuery's own mechanism rather than an API this
 * connector drives. What the cases below hold is what the query path exists for and what it claims:
 *
 * <ul>
 *   <li>a <b>view</b> is readable through a query and is not readable as a table — the second half
 *       being the whole reason the first exists;
 *   <li>the failure a user meets when they point {@code table(...)} at a view names {@code
 *       query(...)}, which pins the mapping against BigQuery's real wording rather than against the
 *       one recorded on 2026-08-10;
 *   <li>both landing places produce the same rows, so choosing one is a choice about cost and
 *       ownership and never about what is read.
 * </ul>
 *
 * <p>The result table the named-dataset case leaves behind carries the expiration the runner sets,
 * and the gated dataset's own default expiration is the backstop — the same arrangement {@code
 * RealBigQuery.deleteTables} documents for a crashed run.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryQuerySourceRealGcpITCase {

    private static final String TABLE = TestNames.unique("query_source");
    private static final String VIEW = TestNames.unique("query_source_view");
    private static final int ROWS = 20;

    /** Half the rows, so a query that filters is visibly not a whole-table read. */
    private static final int THRESHOLD = 10;

    private static final String READER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"}]}";

    @BeforeAll
    static void seed() throws Exception {
        RealBigQuery.createTable(
                TABLE,
                Schema.of(
                        Field.newBuilder("id", StandardSQLTypeName.INT64)
                                .setMode(Field.Mode.REQUIRED)
                                .build()));
        String values =
                IntStream.range(0, ROWS)
                        .mapToObj(id -> "(" + id + ")")
                        .collect(Collectors.joining(", "));
        RealBigQuery.queryRows(
                "INSERT INTO " + RealBigQuery.tablePath(TABLE) + " (id) VALUES " + values);
        RealBigQuery.queryRows(
                "CREATE VIEW "
                        + RealBigQuery.tablePath(VIEW)
                        + " AS SELECT id FROM "
                        + RealBigQuery.tablePath(TABLE)
                        + " WHERE id >= "
                        + THRESHOLD);
    }

    @AfterAll
    static void cleanUp() throws Exception {
        RealBigQuery.queryRows("DROP VIEW IF EXISTS " + RealBigQuery.tablePath(VIEW));
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void readsAViewThroughAQuery() throws Exception {
        assertThat(read(UnaryOperator.identity())).containsExactlyInAnyOrderElementsOf(expected());
    }

    @Test
    void readsAViewIntoANamedDatasetToo() throws Exception {
        // The other landing place, and the one that creates a table: same rows, so the choice
        // between them is about cost and ownership rather than about what is read.
        assertThat(read(builder -> builder.queryResultDataset(RealBigQuery.dataset())))
                .containsExactlyInAnyOrderElementsOf(expected());
    }

    @Test
    void materializesAViewNamedAsATableWhenAskedTo() throws Exception {
        // The opt-in path: table(...) plus materializeViews(), with no query written by hand. The
        // projection is folded into the SELECT this connector generates, so this also exercises
        // that a materialized view is read through a reader schema narrower than the view.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<GenericRecord> records =
                env.fromSource(
                                BigQuerySource.<GenericRecord>builder()
                                        .table(RealBigQuery.destination(VIEW))
                                        .materializeViews()
                                        .selectedFields("id")
                                        .deserializer(
                                                BigQueryRowDeserializer.genericRecord(
                                                        READER_SCHEMA))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "bigquery")
                        .executeAndCollect()) {
            records.forEachRemaining(row -> ids.add((Long) row.get("id")));
        }

        assertThat(ids).containsExactlyInAnyOrderElementsOf(expected());
    }

    @Test
    void readsAnOrdinaryTableDirectlyEvenWhenViewsAreMaterialized() throws Exception {
        // The other half of the opt-in, and the one a cost question turns on: the metadata call
        // says "table", and the read goes straight to it with no query job.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<GenericRecord> records =
                env.fromSource(
                                BigQuerySource.<GenericRecord>builder()
                                        .table(RealBigQuery.destination(TABLE))
                                        .materializeViews()
                                        .deserializer(
                                                BigQueryRowDeserializer.genericRecord(
                                                        READER_SCHEMA))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "bigquery")
                        .executeAndCollect()) {
            records.forEachRemaining(row -> ids.add((Long) row.get("id")));
        }

        assertThat(ids).hasSize(ROWS);
    }

    @Test
    void refusesToReadAViewAsATableAndSaysWhatToDoInstead() throws Exception {
        // The measurement the hint rests on, kept as a test: if BigQuery ever answers this
        // differently, the sentence stops being attached and this is where that shows up.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Source<GenericRecord, ?, ?> source =
                BigQuerySource.<GenericRecord>builder()
                        .table(RealBigQuery.destination(VIEW))
                        .deserializer(BigQueryRowDeserializer.genericRecord(READER_SCHEMA))
                        .build();

        assertThatThrownBy(
                        () -> {
                            try (CloseableIterator<GenericRecord> records =
                                    env.fromSource(
                                                    source,
                                                    WatermarkStrategy.noWatermarks(),
                                                    "bigquery")
                                            .executeAndCollect()) {
                                records.forEachRemaining(row -> {});
                            }
                        })
                // The whole chain, not rootCause(): the hint sits on the IOException the session
                // creator throws, and the root cause under it is BigQuery's own ApiException,
                // whose message says nothing about this connector's builder.
                .hasStackTraceContaining("query(...)")
                .hasStackTraceContaining("non-table entities");
    }

    @Test
    void reusesTheQueryJobAcrossTwoRunsUnderTheSameIdentity() throws Exception {
        // The reuse behaviours only the service can answer: that BigQuery accepts the
        // deterministic id's shape, that a second attempt finds the finished job under it by
        // jobs.get, and that the completed job's metadata still names the anonymous table the
        // first run landed in. The second run() submits nothing — one query is billed.
        //
        // The identity's digest covers the SQL, and the SQL names this run's unique fixture
        // table, so ids cannot collide across weekly runs however long BigQuery keeps them. A
        // window rollover between the two calls is covered too, by the previous-bucket attach —
        // which is exactly the production claim.
        QuerySpec spec =
                new QuerySpec(
                        "SELECT id FROM " + RealBigQuery.tablePath(TABLE) + " WHERE id < 3",
                        RealBigQuery.project(),
                        // The location is load-bearing, not configuration hygiene: without it the
                        // second attempt's jobs.get sees only the US multi-region and this
                        // dataset is regional — the measurement that made queryLocation(...) a
                        // requirement of the reuse knob.
                        RealBigQuery.datasetLocation(),
                        null);
        QuerySpec reusable =
                spec.withJobIdentity(
                        QueryJobIdentity.of(
                                "query-source-reattach-it",
                                spec,
                                Duration.ofHours(1),
                                System.currentTimeMillis()));

        QueryResult first = new BigQueryQueryRunner(null).run(reusable);
        // A fresh runner, as a failed-over JobManager's enumerator would build one.
        QueryResult second = new BigQueryQueryRunner(null).run(reusable);

        assertThat(first.isReattached()).isFalse();
        assertThat(second.isReattached()).isTrue();
        assertThat(second.getTable()).isEqualTo(first.getTable());
    }

    @Test
    void readsAViewThroughAQueryWithTheReuseKnobOn() throws Exception {
        // The production path end to end: the enumerator derives the id from the real job's
        // name — whatever the local environment called it, read out of the metric variables —
        // and BigQuery accepts the sanitised form. The rows prove the session read the landed
        // table; the id acceptance is the half no unit test can claim.
        assertThat(
                        read(
                                builder ->
                                        builder.queryLocation(RealBigQuery.datasetLocation())
                                                .reuseQueryResultWithin(Duration.ofHours(1))))
                .containsExactlyInAnyOrderElementsOf(expected());
    }

    /** Runs a job reading the view through a query, with the given knobs applied. */
    private static List<Long> read(UnaryOperator<BigQuerySourceBuilder<GenericRecord>> customizer)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        BigQuerySourceBuilder<GenericRecord> builder =
                BigQuerySource.<GenericRecord>builder()
                        .query("SELECT id FROM " + RealBigQuery.tablePath(VIEW))
                        .parentProject(RealBigQuery.project())
                        .deserializer(BigQueryRowDeserializer.genericRecord(READER_SCHEMA));

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<GenericRecord> records =
                env.fromSource(
                                customizer.apply(builder).build(),
                                WatermarkStrategy.noWatermarks(),
                                "bigquery")
                        .executeAndCollect()) {
            records.forEachRemaining(row -> ids.add((Long) row.get("id")));
        }
        return ids;
    }

    private static List<Long> expected() {
        return IntStream.range(THRESHOLD, ROWS)
                .mapToObj(Long::valueOf)
                .collect(Collectors.toList());
    }
}
