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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.AbstractSpannerRealGcpITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamTestSourceFactory;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.DefaultSpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What real Cloud Spanner does with a partitioned read — the three things {@code docs/adr/0085}
 * could not settle against the emulator.
 *
 * <p>The emulator planned exactly two partitions for every table it was asked about, one of them
 * empty, and ignored both partition hints, so split planning had no coverage at all. Its
 * partitionability check is its own and runs <em>stricter</em> than the service's, so which query
 * shapes Spanner will plan was unmeasured. And it accepts {@code dataBoostEnabled} while doing
 * nothing with it, so nothing showed the flag reaching anything.
 *
 * <p>What this class can and cannot claim about split planning: its table is a few thousand small
 * rows, which is far below the size at which Spanner splits a table, so a small partition count
 * here is a measurement of this scale and not evidence about a large one. That is worth having
 * anyway — it is what a reader of the docs page needs in order to know the hints are hints — and it
 * is a better answer than seeding gigabytes to make a number look impressive.
 *
 * <p>Issue #573 also keeps the Table API's named-schema path here so both dialects exercise writes,
 * bounded index scans, and synchronous and asynchronous lookups on the same billed instance this
 * class already owns.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "SPANNER_IT_PROJECT", matches = ".+")
class SpannerSourceRealGcpITCase extends AbstractSpannerRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerSourceRealGcpITCase.class);

    private static final int ROWS = 5_000;

    private static final int CHANGE_STREAM_ROWS = 5_000;
    private static final String ALL_CHANGES = "all_changes";
    private static final String EXPLICIT_CHANGES = "explicit_changes";
    private static final Duration CHANGE_STREAM_WAIT = Duration.ofMinutes(2);

    /** Spanner's commit limits are per transaction, so the seed goes in several. */
    private static final int SEED_BATCH = 500;

    private static SpannerDatabase database;
    private static Map<Dialect, SpannerDatabase> namedSchemaDatabases;
    private static Map<Dialect, SpannerDatabase> changeStreamDatabases;
    private static Map<Dialect, Instant> beforeChangeStreamCreation;

    @TempDir private static Path savepointDirectory;

    @BeforeAll
    static void createAndSeedDatabase() throws Exception {
        database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (id)");
        List<Mutation> rows = new ArrayList<>(SEED_BATCH);
        for (long id = 0; id < ROWS; id++) {
            rows.add(
                    Mutation.newInsertOrUpdateBuilder("singers")
                            .set("id")
                            .to(id)
                            .set("name")
                            .to("singer-" + id)
                            .build());
            if (rows.size() == SEED_BATCH) {
                client(database).write(rows);
                rows.clear();
            }
        }
        if (!rows.isEmpty()) {
            client(database).write(rows);
        }
        namedSchemaDatabases = new EnumMap<>(Dialect.class);
        changeStreamDatabases = new EnumMap<>(Dialect.class);
        beforeChangeStreamCreation = new EnumMap<>(Dialect.class);
        for (Dialect dialect : Dialect.values()) {
            namedSchemaDatabases.put(dialect, createNamedSchemaDatabase(dialect));
            beforeChangeStreamCreation.put(dialect, Instant.now().minusSeconds(5));
            changeStreamDatabases.put(dialect, createChangeStreamDatabase(dialect));
        }
    }

    @Test
    void measuresHowManyPartitionsTheServicePlans() {
        Statement query = Statement.of("SELECT id FROM singers");
        BatchReadOnlyTransaction txn = transaction();

        int byDefault = txn.partitionQuery(PartitionOptions.getDefaultInstance(), query).size();
        int withMaxPartitions =
                txn.partitionQuery(
                                PartitionOptions.newBuilder().setMaxPartitions(16).build(), query)
                        .size();
        int withPartitionSize =
                txn.partitionQuery(
                                PartitionOptions.newBuilder().setPartitionSizeBytes(1024).build(),
                                query)
                        .size();
        int byRead =
                txn.partitionRead(
                                PartitionOptions.getDefaultInstance(),
                                "singers",
                                KeySet.all(),
                                Collections.singletonList("id"),
                                Options.dataBoostEnabled(false))
                        .size();

        LOG.info(
                "Cloud Spanner partition planning over {} rows:"
                        + "\n  partitionQuery, default hints      : {}"
                        + "\n  partitionQuery, maxPartitions=16   : {}"
                        + "\n  partitionQuery, sizeBytes=1024     : {}"
                        + "\n  partitionRead,  default hints      : {}",
                ROWS,
                byDefault,
                withMaxPartitions,
                withPartitionSize,
                byRead);

        // The service always plans at least one partition for a partitionable read; a count of
        // zero would mean the enumerator has nothing to assign and the job silently reads nothing.
        assertThat(byDefault).isPositive();
        assertThat(byRead).isPositive();
    }

    @Test
    void everyRowAppearsInExactlyOnePartition() {
        BatchReadOnlyTransaction txn = transaction();
        List<Partition> partitions =
                txn.partitionQuery(
                        PartitionOptions.getDefaultInstance(),
                        Statement.of("SELECT id FROM singers"));

        Set<Long> seen = new HashSet<>();
        int total = 0;
        for (Partition partition : partitions) {
            for (long id : idsIn(txn, partition)) {
                seen.add(id);
                total++;
            }
        }

        // Complete and disjoint, which is what makes one partition per split a correct plan and
        // what an at-least-once bounded read is otherwise free to violate unnoticed.
        assertThat(total).isEqualTo(ROWS);
        assertThat(seen).hasSize(ROWS);
    }

    @Test
    void measuresWhichQueryShapesTheServiceWillPlan() {
        BatchReadOnlyTransaction txn = transaction();
        Map<String, String> verdicts = new LinkedHashMap<>();
        for (String sql :
                new String[] {
                    "SELECT id FROM singers",
                    "SELECT id FROM singers WHERE id > 10",
                    "SELECT COUNT(*) AS c FROM singers",
                    "SELECT id FROM singers ORDER BY id",
                    "SELECT id FROM singers LIMIT 10"
                }) {
            verdicts.put(sql, plan(txn, sql));
        }

        LOG.info(
                "Cloud Spanner root-partitionability, one shape per line:\n  {}",
                verdicts.entrySet().stream()
                        .map(entry -> entry.getKey() + "\n    -> " + entry.getValue())
                        .collect(Collectors.joining("\n  ")));

        // The controls: a plain scan and a predicate are root-partitionable, so a refusal anywhere
        // above is about the shape rather than about the table, the transaction or the account.
        assertThat(verdicts.get("SELECT id FROM singers")).startsWith("planned");
        assertThat(verdicts.get("SELECT id FROM singers WHERE id > 10")).startsWith("planned");

        // Measured 2026-08-10: the service refuses the same three shapes the emulator does, so
        // the emulator's conservatism — real in principle, since its check is its own — did not
        // manifest on any shape tried here. What differs is the message, and that difference is
        // the whole reason the connector surfaces it unwrapped: Spanner names the condition and
        // links the documentation for it, where the emulator says only that it could not tell.
        // Asserted rather than merely logged so that a reworded refusal makes someone revisit the
        // docs sentence that quotes this.
        for (String sql :
                new String[] {
                    "SELECT COUNT(*) AS c FROM singers",
                    "SELECT id FROM singers ORDER BY id",
                    "SELECT id FROM singers LIMIT 10"
                }) {
            assertThat(verdicts.get(sql))
                    .startsWith("refused, INVALID_ARGUMENT")
                    .contains("root partitionable");
        }
    }

    @Test
    void dataBoostServesAPartitionedRead() {
        BatchReadOnlyTransaction txn = transaction();
        List<Partition> partitions =
                txn.partitionQuery(
                        PartitionOptions.getDefaultInstance(),
                        Statement.of("SELECT id FROM singers"),
                        Options.dataBoostEnabled(true));

        int total = 0;
        for (Partition partition : partitions) {
            total += idsIn(txn, partition).size();
        }

        LOG.info("Data Boost planned {} partitions and returned {} rows", partitions.size(), total);
        // The first evidence anywhere in this repository that the flag does something rather than
        // being accepted and ignored: the emulator takes it and changes nothing, so a boosted read
        // returning the whole table is the measurement. It also exercises the
        // spanner.databases.useDataBoost permission, which a reader role does not carry.
        assertThat(total).isEqualTo(ROWS);
    }

    @Test
    void readsEveryRowThroughTheProductionClient() throws Exception {
        // No emulatorEndpoint(...), so the source builds its client over application-default
        // credentials — the one path every emulator test in this module skips.
        assertThat(run(builder -> builder)).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void readsEveryRowWithDataBoostThroughTheSource() throws Exception {
        assertThat(run(builder -> builder.dataBoostEnabled(true)))
                .containsExactlyInAnyOrderElementsOf(allIds());
    }

    @ParameterizedTest
    @MethodSource("namedSchemaCases")
    void tableApiUsesANamedSchemaForWritesScansAndLookups(Dialect dialect, boolean quoted)
            throws Exception {
        SpannerDatabase namedSchemaDatabase = namedSchemaDatabases.get(dialect);
        TableEnvironment sink =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        sink.executeSql(
                namedSchemaTableDdl("named_sink", namedSchemaDatabase, dialect, false, quoted));
        sink.executeSql("INSERT INTO named_sink VALUES (1, 'Ada'), (2, 'Grace')").await();

        TableEnvironment source =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        source.getConfig().set("parallelism.default", "1");
        source.executeSql(
                namedSchemaTableDdl("named_source", namedSchemaDatabase, dialect, false, quoted));
        assertThat(tableRows(source, "SELECT id FROM named_source WHERE name = 'Grace'"))
                .containsExactly(Row.of(2L));

        source.executeSql(
                "CREATE TABLE named_facts (id BIGINT, event_time AS PROCTIME()) WITH ("
                        + "'connector'='datagen', 'number-of-rows'='2', "
                        + "'fields.id.kind'='sequence', 'fields.id.start'='1', "
                        + "'fields.id.end'='2')");
        assertThat(lookupNames(source, "named_source")).containsExactly("Ada", "Grace");

        source.executeSql(
                namedSchemaTableDdl(
                        "named_async_source", namedSchemaDatabase, dialect, true, quoted));
        assertThat(lookupNames(source, "named_async_source")).containsExactly("Ada", "Grace");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void changeStreamMetadataMatchesTheServiceAndReportsExplicitColumns(Dialect dialect)
            throws Exception {
        SpannerDatabase changeStreamDatabase = changeStreamDatabases.get(dialect);
        try (LogCapture capture =
                        LogCapture.of(
                                DefaultSpannerChangeStreamCoordinatorClientFactory.class,
                                LogCapture.Level.INFO);
                SpannerChangeStreamCoordinatorClient all =
                        new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                        changeStreamDatabase, ALL_CHANGES, null)
                                .create();
                SpannerChangeStreamCoordinatorClient explicit =
                        new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                        changeStreamDatabase, EXPLICIT_CHANGES, null)
                                .create()) {
            assertThat(all.initialize()).isEqualTo(Duration.ofDays(1));
            assertThat(explicit.initialize()).isEqualTo(Duration.ofDays(7));

            String messages = String.join("\n", capture.getMessages());
            assertThat(messages)
                    .contains("scope=ALL")
                    .contains("retention=PT24H")
                    .contains("valueCaptureType=OLD_AND_NEW_VALUES")
                    .contains("excludeTtlDeletes=true")
                    .contains("excludeInserts=true")
                    .contains("allowTransactionExclusion=true")
                    .contains("watches an explicit column list")
                    .contains("changes")
                    .contains("Columns added later are not watched automatically");
        }
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void changeStreamRecoversFromCheckpointAndSavepoint(Dialect dialect) throws Exception {
        SpannerDatabase changeStreamDatabase = changeStreamDatabases.get(dialect);
        String runId = "recovery-" + dialect + "-" + UUID.randomUUID();
        SpannerChangeStreamRealGcpSupport.reset(runId);
        JobClient first =
                SpannerChangeStreamRealGcpSupport.start(
                        changeStreamSource(changeStreamDatabase, StartPosition.latest(), null),
                        runId,
                        null,
                        true);
        boolean stopped = false;
        try {
            awaitChangeStream(
                    "the initial Spanner query to start",
                    first,
                    () ->
                            SpannerChangeStreamRealGcpSupport.counter(
                                            runId, "changeStreamQueriesStarted")
                                    > 0,
                    runId);
            writeChangeRows(changeStreamDatabase, 0, CHANGE_STREAM_ROWS / 2);
            awaitChangeStream(
                    "the first half of the mutations",
                    first,
                    () ->
                            SpannerChangeStreamRealGcpSupport.uniqueIds(runId)
                                    >= CHANGE_STREAM_ROWS / 2,
                    runId);
            awaitChangeStream(
                    "a completed checkpoint",
                    first,
                    () -> SpannerChangeStreamRealGcpSupport.completedCheckpoint(runId) >= 0,
                    runId);

            long queriesBeforeFailure =
                    SpannerChangeStreamRealGcpSupport.counter(runId, "changeStreamQueriesStarted");
            SpannerChangeStreamRealGcpSupport.armFailure(runId);
            writeChangeRows(
                    changeStreamDatabase, CHANGE_STREAM_ROWS / 2, CHANGE_STREAM_ROWS / 2 + 1);
            awaitChangeStream(
                    "the deliberate post-checkpoint failure",
                    first,
                    () -> SpannerChangeStreamRealGcpSupport.failed(runId),
                    runId);
            awaitChangeStream(
                    "a query to reopen after restart",
                    first,
                    () ->
                            SpannerChangeStreamRealGcpSupport.counter(
                                            runId, "changeStreamQueriesStarted")
                                    > queriesBeforeFailure,
                    runId);

            writeChangeRows(changeStreamDatabase, CHANGE_STREAM_ROWS / 2 + 1, CHANGE_STREAM_ROWS);
            awaitChangeStream(
                    "all change-stream mutations after restart",
                    first,
                    () -> SpannerChangeStreamRealGcpSupport.uniqueIds(runId) >= CHANGE_STREAM_ROWS,
                    runId);
            awaitChangeStream(
                    "the service heartbeat watermark",
                    first,
                    () -> SpannerChangeStreamRealGcpSupport.watermarkAdvanced(runId),
                    runId);

            assertChangeStreamMetrics(runId);
            assertThat(SpannerChangeStreamRealGcpSupport.timestampMismatches(runId)).isZero();
            LOG.info(
                    "Cloud Spanner Change Streams {} checkpoint evidence: records={}, unique={},"
                            + " duplicates={}, metrics={}",
                    dialect,
                    SpannerChangeStreamRealGcpSupport.realRecordCount(runId),
                    SpannerChangeStreamRealGcpSupport.uniqueIds(runId),
                    SpannerChangeStreamRealGcpSupport.duplicateCount(runId),
                    SpannerChangeStreamRealGcpSupport.metricSummary(runId));

            String savepoint =
                    first.stopWithSavepoint(
                                    false,
                                    savepointDirectory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(CHANGE_STREAM_WAIT.toSeconds(), TimeUnit.SECONDS);
            stopped = true;

            long stoppedGap = CHANGE_STREAM_ROWS;
            long afterRestore = CHANGE_STREAM_ROWS + 1L;
            writeChangeRows(changeStreamDatabase, stoppedGap, stoppedGap + 1);
            JobClient restored =
                    SpannerChangeStreamRealGcpSupport.start(
                            changeStreamSource(changeStreamDatabase, StartPosition.latest(), null),
                            runId,
                            savepoint,
                            false);
            try {
                writeChangeRows(changeStreamDatabase, afterRestore, afterRestore + 1);
                awaitChangeStream(
                        "the mutation written while stopped and the mutation after restore",
                        restored,
                        () -> {
                            Set<Long> ids = SpannerChangeStreamRealGcpSupport.ids(runId);
                            return ids.contains(stoppedGap) && ids.contains(afterRestore);
                        },
                        runId);
                assertThat(SpannerChangeStreamRealGcpSupport.timestampMismatches(runId)).isZero();
            } finally {
                cancelQuietly(restored);
            }
        } finally {
            if (!stopped) {
                cancelQuietly(first);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void startBeforeChangeStreamCreationGetsConnectorGuidance(Dialect dialect) throws Exception {
        SpannerDatabase changeStreamDatabase = changeStreamDatabases.get(dialect);
        String runId = "precreation-" + dialect + "-" + UUID.randomUUID();
        SpannerChangeStreamRealGcpSupport.reset(runId);
        JobClient job =
                SpannerChangeStreamRealGcpSupport.start(
                        changeStreamSource(
                                changeStreamDatabase,
                                StartPosition.at(beforeChangeStreamCreation.get(dialect)),
                                null),
                        runId,
                        null,
                        false);

        Throwable failure = executionFailure(job);
        assertThat(failureMessages(failure))
                .contains("initial start timestamp")
                .contains("change stream was created")
                .contains("StartPosition.latest()")
                .contains("StartPosition.at(...)");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void expiredRestoreFailsByDefaultAndCanUseAnExplicitFallback(Dialect dialect) throws Exception {
        SpannerDatabase changeStreamDatabase = changeStreamDatabases.get(dialect);
        Instant stale = Instant.now().minus(Duration.ofDays(2));
        String staleRun = "stale-savepoint-" + dialect + "-" + UUID.randomUUID();
        SpannerChangeStreamRealGcpSupport.reset(staleRun);
        JobClient scripted =
                SpannerChangeStreamRealGcpSupport.start(
                        SpannerChangeStreamTestSourceFactory.staleSource(stale, 2),
                        staleRun,
                        null,
                        false);
        String staleSavepoint;
        boolean stopped = false;
        try {
            awaitChangeStream(
                    "the scripted stale partitions",
                    scripted,
                    () -> SpannerChangeStreamRealGcpSupport.allRecords(staleRun) >= 2,
                    staleRun);
            staleSavepoint =
                    scripted.stopWithSavepoint(
                                    false,
                                    savepointDirectory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(CHANGE_STREAM_WAIT.toSeconds(), TimeUnit.SECONDS);
            stopped = true;
        } finally {
            if (!stopped) {
                cancelQuietly(scripted);
            }
        }

        String failedRun = "expired-default-" + dialect + "-" + UUID.randomUUID();
        SpannerChangeStreamRealGcpSupport.reset(failedRun);
        JobClient failedRestore =
                SpannerChangeStreamRealGcpSupport.start(
                        changeStreamSource(changeStreamDatabase, StartPosition.latest(), null),
                        failedRun,
                        staleSavepoint,
                        false);
        assertThat(failureMessages(executionFailure(failedRestore)))
                .contains("older than the computed earliest position")
                .contains("unavailable range")
                .contains("No restore fallback was configured");
        assertThat(
                        SpannerChangeStreamRealGcpSupport.counter(
                                failedRun, "changeStreamQueriesStarted"))
                .isZero();

        String fallbackRun = "expired-fallback-" + dialect + "-" + UUID.randomUUID();
        SpannerChangeStreamRealGcpSupport.reset(fallbackRun);
        JobClient fallbackRestore =
                SpannerChangeStreamRealGcpSupport.start(
                        changeStreamSource(
                                changeStreamDatabase,
                                StartPosition.latest(),
                                StartPosition.latest()),
                        fallbackRun,
                        staleSavepoint,
                        false);
        try {
            awaitChangeStream(
                    "the fallback query",
                    fallbackRestore,
                    () ->
                            SpannerChangeStreamRealGcpSupport.counter(
                                            fallbackRun, "changeStreamQueriesStarted")
                                    > 0,
                    fallbackRun);
            long id = 20_000L + dialect.ordinal();
            writeChangeRows(changeStreamDatabase, id, id + 1);
            awaitChangeStream(
                    "the mutation after the explicit fallback",
                    fallbackRestore,
                    () -> SpannerChangeStreamRealGcpSupport.ids(fallbackRun).contains(id),
                    fallbackRun);
            assertThat(SpannerChangeStreamRealGcpSupport.allRecords(fallbackRun))
                    .isEqualTo(SpannerChangeStreamRealGcpSupport.realRecordCount(fallbackRun));
        } finally {
            cancelQuietly(fallbackRestore);
        }
    }

    private static Stream<Arguments> namedSchemaCases() {
        return Stream.of(Dialect.values())
                .flatMap(
                        dialect ->
                                Stream.of(
                                        Arguments.of(dialect, false), Arguments.of(dialect, true)));
    }

    // ---------------------------------------------------------------- helpers

    /** Plans the query and reports what the service said, for the table this test logs. */
    private static String plan(BatchReadOnlyTransaction txn, String sql) {
        try {
            int partitions =
                    txn.partitionQuery(PartitionOptions.getDefaultInstance(), Statement.of(sql))
                            .size();
            return "planned, " + partitions + " partition(s)";
        } catch (SpannerException e) {
            // The message, not only the code: the emulator refuses these shapes too, and what
            // distinguishes the two refusals — and tells a user which constraint they met — is the
            // wording.
            return "refused, " + e.getErrorCode() + ": " + e.getMessage();
        }
    }

    private static BatchReadOnlyTransaction transaction() {
        BatchClient batch =
                spanner()
                        .getBatchClient(
                                DatabaseId.of(
                                        database.getProject(),
                                        database.getInstance(),
                                        database.getDatabase()));
        return batch.batchReadOnlyTransaction(TimestampBound.strong());
    }

    private static List<Long> idsIn(BatchReadOnlyTransaction txn, Partition partition) {
        List<Long> ids = new ArrayList<>();
        try (ResultSet resultSet = txn.execute(partition)) {
            while (resultSet.next()) {
                ids.add(resultSet.getLong("id"));
            }
        }
        return ids;
    }

    private static List<Long> allIds() {
        return LongStream.range(0, ROWS).boxed().collect(Collectors.toList());
    }

    private static List<Long> run(UnaryOperator<SpannerSourceBuilder<Long>> configure)
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
                                                        .readOperation(
                                                                SpannerReadOperation.query(
                                                                        Statement.of(
                                                                                "SELECT id FROM"
                                                                                        + " singers")))
                                                        .deserializer(new IdDeserializer()))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "spanner")
                        .executeAndCollect()) {
            collected.forEachRemaining(ids::add);
        }
        return ids;
    }

    private static SpannerDatabase createNamedSchemaDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE SCHEMA analytics",
                    "CREATE TABLE analytics.records ("
                            + "id bigint NOT NULL PRIMARY KEY, name varchar(64))",
                    "CREATE INDEX records_by_name ON analytics.records (name)",
                    "CREATE SCHEMA \"QuotedAnalytics\"",
                    "CREATE TABLE \"QuotedAnalytics\".\"QuotedRecords\" ("
                            + "id bigint NOT NULL PRIMARY KEY, name varchar(64))",
                    "CREATE INDEX \"QuotedRecordsByName\" ON"
                            + " \"QuotedAnalytics\".\"QuotedRecords\" (name)");
        }
        return createDatabase(
                dialect,
                "CREATE SCHEMA analytics",
                "CREATE TABLE analytics.records ("
                        + "id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                "CREATE INDEX analytics.records_by_name ON analytics.records (name)",
                "CREATE SCHEMA `QuotedAnalytics`",
                "CREATE TABLE `QuotedAnalytics`.`QuotedRecords` ("
                        + "id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                "CREATE INDEX `QuotedAnalytics`.`QuotedRecordsByName` ON"
                        + " `QuotedAnalytics`.`QuotedRecords` (name)");
    }

    private static SpannerDatabase createChangeStreamDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE changes (id bigint NOT NULL PRIMARY KEY, value varchar)",
                    "CREATE CHANGE STREAM " + ALL_CHANGES + " FOR ALL WITH (retention_period='1d')",
                    "CREATE CHANGE STREAM "
                            + EXPLICIT_CHANGES
                            + " FOR changes(value) WITH (exclude_ttl_deletes=true,"
                            + " exclude_insert=true, allow_txn_exclusion=true)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE changes (id INT64 NOT NULL, value STRING(MAX)) PRIMARY KEY (id)",
                "CREATE CHANGE STREAM " + ALL_CHANGES + " FOR ALL OPTIONS (retention_period='1d')",
                "CREATE CHANGE STREAM "
                        + EXPLICIT_CHANGES
                        + " FOR changes(value) OPTIONS (exclude_ttl_deletes=true,"
                        + " exclude_insert=true, allow_txn_exclusion=true)");
    }

    private static SpannerChangeStreamSource<String> changeStreamSource(
            SpannerDatabase changeStreamDatabase,
            StartPosition start,
            @Nullable StartPosition resumeFallback) {
        SpannerChangeStreamSourceBuilder<String> builder =
                SpannerChangeStreamSource.<String>builder()
                        .database(changeStreamDatabase)
                        .changeStreamName(ALL_CHANGES)
                        .deserializer(SpannerChangeStreamRealGcpSupport.realDeserializer())
                        .startPosition(start)
                        .heartbeatInterval(Duration.ofSeconds(1))
                        .maxConcurrentQueriesPerSubtask(2);
        if (resumeFallback != null) {
            builder.resumeFallback(resumeFallback);
        }
        return builder.build();
    }

    private static void writeChangeRows(
            SpannerDatabase target, long fromInclusive, long toExclusive) {
        List<Mutation> mutations = new ArrayList<>(SEED_BATCH);
        for (long id = fromInclusive; id < toExclusive; id++) {
            mutations.add(
                    Mutation.newInsertOrUpdateBuilder("changes")
                            .set("id")
                            .to(id)
                            .set("value")
                            .to("value-" + id)
                            .build());
            if (mutations.size() == SEED_BATCH) {
                client(target).write(mutations);
                mutations.clear();
            }
        }
        if (!mutations.isEmpty()) {
            client(target).write(mutations);
        }
    }

    private static void awaitChangeStream(
            String description, JobClient job, BooleanSupplier condition, String runId)
            throws InterruptedException {
        await(
                description,
                CHANGE_STREAM_WAIT,
                () -> {
                    SpannerChangeStreamRealGcpSupport.sampleActiveQueries(runId);
                    if (condition.getAsBoolean()) {
                        return true;
                    }
                    return runningOrThrow(job, description);
                },
                () ->
                        "job="
                                + SpannerChangeStreamRealGcpSupport.jobStatus(job)
                                + "; records="
                                + SpannerChangeStreamRealGcpSupport.realRecordCount(runId)
                                + "; unique="
                                + SpannerChangeStreamRealGcpSupport.uniqueIds(runId)
                                + "; checkpoints="
                                + SpannerChangeStreamRealGcpSupport.completedCheckpoint(runId)
                                + "; metrics="
                                + SpannerChangeStreamRealGcpSupport.metricSummary(runId));
    }

    private static boolean runningOrThrow(JobClient job, String description) {
        JobStatus status = job.getJobStatus().join();
        if (!status.isGloballyTerminalState()) {
            return false;
        }
        try {
            job.getJobExecutionResult().join();
        } catch (CompletionException e) {
            throw new AssertionError(
                    "Job terminated with " + status + " while awaiting " + description + ".",
                    e.getCause());
        }
        throw new AssertionError(
                "Job terminated with " + status + " while awaiting " + description + ".");
    }

    private static void assertChangeStreamMetrics(String runId) {
        long discovered =
                SpannerChangeStreamRealGcpSupport.counter(
                        runId, "changeStreamPartitionsDiscovered");
        Map<String, Long> queries =
                SpannerChangeStreamRealGcpSupport.counterBySubtask(
                        runId, "changeStreamQueriesStarted");
        Map<String, Long> peaks = SpannerChangeStreamRealGcpSupport.peakActiveQueries(runId);
        assertThat(discovered).isPositive();
        assertThat(queries.values()).anyMatch(value -> value > 0);
        assertThat(peaks).isNotEmpty();
        assertThat(peaks.values()).allMatch(value -> value >= 0 && value <= 2);

        long activeSubtasks = queries.values().stream().filter(value -> value > 0).count();
        if (discovered >= 2) {
            assertThat(activeSubtasks).isGreaterThanOrEqualTo(2);
        } else {
            LOG.info(
                    "Cloud Spanner produced only {} discovered partition(s) in run {};"
                            + " cross-subtask partition distribution is best-effort evidence.",
                    discovered,
                    runId);
        }
        if (discovered >= 4) {
            assertThat(peaks.values()).anyMatch(value -> value == 2);
        } else {
            LOG.info(
                    "Cloud Spanner produced {} discovered partition(s) in run {};"
                            + " concurrent-query occupancy above one is best-effort evidence.",
                    discovered,
                    runId);
        }
    }

    private static Throwable executionFailure(JobClient job) throws Exception {
        try {
            job.getJobExecutionResult().get(CHANGE_STREAM_WAIT.toSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        throw new AssertionError("Expected the Change Streams job to fail.");
    }

    private static String failureMessages(Throwable failure) {
        return causeChain(failure).stream()
                .map(cause -> cause.getClass().getSimpleName() + ": " + cause.getMessage())
                .collect(Collectors.joining("\n"));
    }

    private static List<Throwable> causeChain(Throwable failure) {
        List<Throwable> causes = new ArrayList<>();
        Throwable current = failure;
        while (current != null && !causes.contains(current)) {
            causes.add(current);
            current = current.getCause();
        }
        return causes;
    }

    private static void cancelQuietly(JobClient job) {
        try {
            job.cancel().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Failed to cancel gated Change Streams job", e);
        }
    }

    private static String namedSchemaTableDdl(
            String tableName,
            SpannerDatabase namedSchemaDatabase,
            Dialect dialect,
            boolean async,
            boolean quoted) {
        String schema = namedIdentifier(dialect, quoted, "analytics", "QuotedAnalytics");
        String table = namedIdentifier(dialect, quoted, "records", "QuotedRecords");
        String index = namedIdentifier(dialect, quoted, "records_by_name", "QuotedRecordsByName");
        return "CREATE TABLE "
                + tableName
                + " (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ("
                + "'connector'='spanner', "
                + "'project'='"
                + namedSchemaDatabase.getProject()
                + "', "
                + "'instance'='"
                + namedSchemaDatabase.getInstance()
                + "', "
                + "'database'='"
                + namedSchemaDatabase.getDatabase()
                + "', "
                + "'schema'='"
                + schema
                + "', 'table'='"
                + table
                + "', "
                + "'scan.index'='"
                + index
                + "', 'dialect'='"
                + dialect.name()
                + "', 'lookup.async'='"
                + async
                + "')";
    }

    private static String namedIdentifier(
            Dialect dialect, boolean quoted, String unquoted, String quotedName) {
        if (!quoted) {
            return unquoted;
        }
        char quote = dialect == Dialect.POSTGRESQL ? '"' : '`';
        return quote + quotedName + quote;
    }

    private static List<Row> tableRows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> collected = table.executeSql(sql).collect()) {
            collected.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static List<String> lookupNames(TableEnvironment table, String source)
            throws Exception {
        return tableRows(
                        table,
                        "SELECT f.id, s.name FROM named_facts AS f LEFT JOIN "
                                + source
                                + " FOR SYSTEM_TIME AS OF f.event_time AS s "
                                + "ON f.id = s.id ORDER BY f.id")
                .stream()
                .map(row -> (String) row.getField(1))
                .collect(Collectors.toList());
    }

    /** Reads the one column these tests select. */
    private static final class IdDeserializer implements SpannerStructDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        @Nullable
        public Long deserialize(Struct row) {
            return row.getLong("id");
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
