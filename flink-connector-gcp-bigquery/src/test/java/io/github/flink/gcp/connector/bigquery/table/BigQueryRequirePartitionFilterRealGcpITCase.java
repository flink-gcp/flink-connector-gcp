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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code sink.table-create.require-partition-filter} against real BigQuery (#2169): a table each
 * write method creates refuses a query without a partition filter, while the sink's own writes into
 * it succeed. A CDC table refuses even a filtered query, which is why CDC provisioning refuses the
 * requirement (#2175). The Storage Read API enforces the requirement as well, so the Table source
 * reads such a table only through a restriction on its partitioning column or pseudocolumn, and an
 * ingestion-time-partitioned table keeps the requirement too (#2183).
 *
 * <p>Real GCP because the emulator is not evidence for it: whether the service keeps the
 * requirement on the created table, and whether each write method's appends, loads and commits go
 * through it, is the service's answer. The copy and the terminal query that finish an oversized
 * {@code FILE_LOADS} commit are covered by {@code BigQueryFileLoadsTempTableLayoutRealGcpITCase}.
 *
 * <p>Requires application-default credentials plus {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET} and
 * {@code BQ_IT_GCS_BUCKET}; skipped when they are absent.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 900, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryRequirePartitionFilterRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String DEFAULT_STREAM = "partition_filter_it_ds_" + RUN_ID;
    private static final String EXACTLY_ONCE = "partition_filter_it_eo_" + RUN_ID;
    private static final String APPEND = "partition_filter_it_append_" + RUN_ID;
    private static final String CDC = "partition_filter_it_cdc_" + RUN_ID;
    private static final String READ = "partition_filter_it_read_" + RUN_ID;
    private static final String INGESTION_READ = "partition_filter_it_ingestion_read_" + RUN_ID;
    private static final String INGESTION = "partition_filter_it_ingestion_" + RUN_ID;
    private static final String STAGING_ROOT = "flink-partition-filter-it/" + RUN_ID;

    private static final String COLUMNS =
            "id BIGINT NOT NULL, name STRING, event_ts TIMESTAMP_LTZ(6)";

    /** A filter on the partitioning column that every row the cases write satisfies. */
    private static final String RECENT_ROWS =
            " WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)";

    /** The creation options the sink-created cases share: day partitions on the timestamp. */
    private static final String[] CREATION =
            new String[] {
                "sink.table-create.time-partitioning.type",
                "day",
                "sink.table-create.time-partitioning.field",
                "event_ts",
                "sink.table-create.require-partition-filter",
                "true"
            };

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () ->
                        RealBigQuery.deleteTables(
                                DEFAULT_STREAM,
                                EXACTLY_ONCE,
                                APPEND,
                                truncating(WriteDisposition.WRITE_TRUNCATE),
                                truncating(WriteDisposition.WRITE_TRUNCATE_DATA),
                                CDC,
                                READ,
                                INGESTION_READ,
                                INGESTION),
                () -> RealGcs.deletePrefix(STAGING_ROOT));
    }

    private static TableEnvironment streaming() {
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        // A refused write must fail the job rather than restart it until the timeout.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        return TableEnvironment.create(
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(configuration)
                        .build());
    }

    private static TableEnvironment batch() {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        return TableEnvironment.create(
                EnvironmentSettings.newInstance()
                        .inBatchMode()
                        .withConfiguration(configuration)
                        .build());
    }

    /** The table a truncating disposition's case creates. */
    private static String truncating(WriteDisposition disposition) {
        return "partition_filter_it_" + disposition.name().toLowerCase(Locale.ROOT) + "_" + RUN_ID;
    }

    private static String stagingPath(String table) {
        return RealGcs.uri(STAGING_ROOT + "/" + table);
    }

    /**
     * Writes two rows into a table the sink creates with {@link #CREATION} and the given options.
     */
    private static void write(TableEnvironment tEnv, String table, String... options)
            throws Exception {
        tEnv.executeSql(
                "CREATE TABLE t ("
                        + COLUMNS
                        + ") "
                        + TableDdl.withOptions(
                                RealBigQuery.project(),
                                RealBigQuery.dataset(),
                                table,
                                TableDdl.concat(CREATION, options)));

        // The current time, so that the rows land in a partition no expiration has reached.
        tEnv.executeSql(
                        "INSERT INTO t VALUES (1, 'a', CURRENT_TIMESTAMP), (2, 'b', CURRENT_TIMESTAMP)")
                .await();
    }

    /**
     * Asserts that the table refuses a query without a partition filter and holds the given number
     * of rows when one is given.
     */
    private static void assertRequiresAPartitionFilter(String table, long rows) throws Exception {
        assertRequiresAPartitionFilter(table, RECENT_ROWS, rows);
    }

    /**
     * Asserts that the table refuses a query without a partition filter and holds the given number
     * of rows under the given filter.
     */
    private static void assertRequiresAPartitionFilter(String table, String filter, long rows)
            throws Exception {
        // The description overload: .as() after assertThatThrownBy is never reached when nothing
        // is thrown, which is the failure this assertion exists to report.
        assertThatThrownBy(
                        () ->
                                RealBigQuery.queryLongs(
                                        "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(table)),
                        "%s refuses a query without a partition filter",
                        table)
                .hasMessageContaining("without a filter");
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(table) + filter))
                .as("%s%s", table, filter)
                .containsExactly(rows);
    }

    /**
     * Reads the table through the Table source, under the given {@code WHERE} condition, if any,
     * and source options.
     */
    private static List<Row> read(
            String table, String columns, @Nullable String where, String... options)
            throws Exception {
        TableEnvironment tEnv = batch();
        tEnv.executeSql(
                "CREATE TABLE src ("
                        + columns
                        + ") "
                        + TableDdl.withOptions(
                                RealBigQuery.project(), RealBigQuery.dataset(), table, options));
        return CollectionUtil.iteratorToList(
                tEnv.executeSql("SELECT * FROM src" + (where == null ? "" : " WHERE " + where))
                        .collect());
    }

    /**
     * A {@code scan.row-restriction} value, its quotes doubled because it sits in a quoted DDL
     * option.
     */
    private static String[] rowRestriction(String restriction) {
        return new String[] {"scan.row-restriction", restriction.replace("'", "''")};
    }

    @Test
    void aTableTheDefaultStreamCreatesRequiresAPartitionFilter() throws Exception {
        write(streaming(), DEFAULT_STREAM);

        assertRequiresAPartitionFilter(DEFAULT_STREAM, 2);
    }

    @Test
    void aTableTheExactlyOnceMethodCreatesRequiresAPartitionFilter() throws Exception {
        write(
                streaming(),
                EXACTLY_ONCE,
                "sink.parallelism",
                "1",
                "sink.write-method",
                "storage-api-exactly-once");

        assertRequiresAPartitionFilter(EXACTLY_ONCE, 2);
    }

    @Test
    void aTableAnAppendingLoadCreatesRequiresAPartitionFilter() throws Exception {
        write(
                streaming(),
                APPEND,
                "sink.write-method",
                "file-loads",
                "sink.file-loads.staging-path",
                stagingPath(APPEND),
                "sink.file-loads.min-checkpoint-interval",
                "1 s");

        assertRequiresAPartitionFilter(APPEND, 2);
    }

    /**
     * The truncating dispositions run in batch, since the connector refuses them in streaming. A
     * load that replaces the table's data, and its schema too under {@code write-truncate}, keeps
     * the requirement.
     */
    @ParameterizedTest
    @EnumSource(
            value = WriteDisposition.class,
            names = {"WRITE_TRUNCATE", "WRITE_TRUNCATE_DATA"})
    void aTableATruncatingLoadCreatesRequiresAPartitionFilter(WriteDisposition disposition)
            throws Exception {
        String table = truncating(disposition);
        write(
                batch(),
                table,
                "sink.write-method",
                "file-loads",
                "sink.file-loads.staging-path",
                stagingPath(table),
                "sink.file-loads.write-disposition",
                disposition.toString());

        assertRequiresAPartitionFilter(table, 2);
    }

    /**
     * The requirement on a table the sink creates with ingestion-time partitioning. Rows the
     * Storage Write API wrote recently may still carry a NULL {@code _PARTITIONTIME}, measured on
     * 2026-11-17 right after the write, so the filter that counts every row, in a query and in the
     * Table source's row restriction alike, names that too; it satisfies the requirement. A read
     * restricted on {@code _PARTITIONDATE} alone returned none of the two rows right after the
     * write, which is why this case does not pin it: the count depends on when BigQuery moves them.
     */
    @Test
    void aTableTheDefaultStreamCreatesWithIngestionTimePartitioningRequiresAPartitionFilter()
            throws Exception {
        TableEnvironment tEnv = streaming();
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, name STRING) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(),
                                RealBigQuery.dataset(),
                                INGESTION,
                                "sink.table-create.time-partitioning.type",
                                "day",
                                "sink.table-create.require-partition-filter",
                                "true"));
        tEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await();

        String recent = "'" + LocalDate.now(ZoneOffset.UTC).minusDays(2) + "'";
        assertRequiresAPartitionFilter(
                INGESTION,
                " WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)"
                        + " OR _PARTITIONTIME IS NULL",
                2);
        assertThat(
                        read(
                                INGESTION,
                                "id BIGINT NOT NULL, name STRING",
                                null,
                                rowRestriction(
                                        "_PARTITIONTIME >= TIMESTAMP "
                                                + recent
                                                + " OR _PARTITIONTIME IS NULL")))
                .as("%s read through the Table source", INGESTION)
                .hasSize(2);
    }

    /**
     * The Storage Read API refuses a read session on a table that requires a partition filter
     * unless its row restriction constrains the partitioning column, so the Table source cannot
     * start without a restriction on that column, nor with one on another column alone or joined to
     * it with {@code OR}, and reads the table with one the source pushes down.
     */
    @Test
    void theTableSourceReadsSuchATableOnlyThroughAFilterOnThePartitioningColumn() throws Exception {
        String path = RealBigQuery.tablePath(READ);
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + path
                        + " (id INT64 NOT NULL, name STRING, event_ts TIMESTAMP)"
                        + " PARTITION BY DATE(event_ts)"
                        + " OPTIONS (require_partition_filter = TRUE)");
        RealBigQuery.queryRows(
                "INSERT INTO "
                        + path
                        + " VALUES (1, 'a', CURRENT_TIMESTAMP()), (2, 'b', CURRENT_TIMESTAMP())");
        String recent =
                "event_ts >= TO_TIMESTAMP_LTZ("
                        + Instant.now().minus(Duration.ofDays(2)).toEpochMilli()
                        + ", 3)";

        assertThat(read(READ, COLUMNS, recent)).as("%s WHERE %s", READ, recent).hasSize(2);
        // The other column's condition goes through scan.row-restriction, which reaches the
        // service whether or not the source would translate it.
        assertRefusedRead(READ, null, rowRestriction("id = 1"));
        assertRefusedRead(READ, null);
        assertRefusedRead(READ, recent + " OR id = 1");
    }

    private static void assertRefusedRead(String table, @Nullable String where, String... options) {
        assertThatThrownBy(
                        () -> read(table, COLUMNS, where, options),
                        "the Table source refuses to read %s WHERE %s with %s",
                        table,
                        where,
                        Arrays.toString(options))
                .hasStackTraceContaining(
                        "without a filter over column(s) 'event_ts' that can be used for"
                                + " partition elimination");
    }

    /**
     * An ingestion-time-partitioned table takes its filter on {@code _PARTITIONTIME} or {@code
     * _PARTITIONDATE}, which the Table source does not expose as columns, so it is read through
     * {@code scan.row-restriction}.
     */
    @Test
    void anIngestionTimeTableIsReadThroughTheRowRestrictionOption() throws Exception {
        String path = RealBigQuery.tablePath(INGESTION_READ);
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + path
                        + " (id INT64 NOT NULL, name STRING)"
                        + " PARTITION BY _PARTITIONDATE"
                        + " OPTIONS (require_partition_filter = TRUE)");
        RealBigQuery.queryRows("INSERT INTO " + path + " (id, name) VALUES (1, 'a'), (2, 'b')");
        String columns = "id BIGINT NOT NULL, name STRING";

        assertThatThrownBy(
                        () -> read(INGESTION_READ, columns, null),
                        "the Table source refuses to read %s without a restriction",
                        INGESTION_READ)
                .hasStackTraceContaining("without a filter over column(s)")
                .hasStackTraceContaining("'_PARTITIONTIME'");
        String restriction =
                "_PARTITIONDATE >= DATE '" + LocalDate.now(ZoneOffset.UTC).minusDays(2) + "'";
        assertThat(read(INGESTION_READ, columns, null, rowRestriction(restriction)))
                .as("%s read with %s", INGESTION_READ, restriction)
                .hasSize(2);
    }

    /**
     * Why the sink refuses the requirement on a table its CDC provisioning creates (ADR-0205): a
     * CDC table that carries it refuses a query, filtered or not, while upserts wait to be applied,
     * because BigQuery merges them at query time without pruning partitions. The table is created
     * here, outside the sink, as a user who accepts that would, and the sink writes it under {@code
     * create-never}. If this fails because the filtered query is answered, either the service now
     * prunes during that merge, and ADR-0205 and the rejection change with it, or a background
     * apply ran within seconds of the write, which ADR-0205's measurements never saw.
     */
    @Test
    void aCdcTableThatRequiresAPartitionFilterRefusesEvenAFilteredQuery() throws Exception {
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + RealBigQuery.tablePath(CDC)
                        + " (id INT64 NOT NULL, name STRING, event_ts TIMESTAMP,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " PARTITION BY DATE(event_ts)"
                        + " OPTIONS (require_partition_filter = TRUE)");
        // The filter prunes this table while it has nothing to merge, so a refusal of the same
        // filter below is the merge's doing, not the filter's.
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM "
                                        + RealBigQuery.tablePath(CDC)
                                        + RECENT_ROWS))
                .containsExactly(0L);
        Instant now = Instant.now();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> changes =
                env.fromData(
                        Arrays.asList(
                                Row.ofKind(RowKind.UPDATE_AFTER, 1L, "a", now, "1"),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2L, "b", now, "1"),
                                Row.ofKind(RowKind.UPDATE_AFTER, 1L, "a2", now, "2")),
                        Types.ROW_NAMED(
                                new String[] {"id", "name", "event_ts", "sequence"},
                                Types.LONG,
                                Types.STRING,
                                Types.INSTANT,
                                Types.STRING));
        tEnv.createTemporaryView(
                "changes",
                tEnv.fromChangelogStream(
                        changes,
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("name", DataTypes.STRING())
                                .column("event_ts", DataTypes.TIMESTAMP_LTZ(6))
                                .column("sequence", DataTypes.STRING())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        tEnv.executeSql(
                "CREATE TABLE current_rows ("
                        + COLUMNS
                        + ", sequence STRING METADATA FROM 'change-sequence-number', "
                        + "PRIMARY KEY (id) NOT ENFORCED) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(),
                                RealBigQuery.dataset(),
                                CDC,
                                "sink.cdc.enabled",
                                "true",
                                "sink.create-disposition",
                                "create-never",
                                "sink.location",
                                RealBigQuery.datasetLocation()));

        // The writes themselves go through.
        tEnv.executeSql(
                        "INSERT INTO current_rows"
                                + " SELECT id, name, event_ts, sequence FROM changes")
                .await();

        for (String filter : new String[] {"", RECENT_ROWS}) {
            assertThatThrownBy(
                            () ->
                                    RealBigQuery.queryLongs(
                                            "SELECT COUNT(*) FROM "
                                                    + RealBigQuery.tablePath(CDC)
                                                    + filter),
                            "%s refuses the query '%s'",
                            CDC,
                            filter)
                    .hasMessageContaining("requires partition filter")
                    .hasMessageContaining("unapplied upsert data");
        }
    }
}
