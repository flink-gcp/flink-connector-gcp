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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What does {@code CreateWriteStream} answer for a table that is not there?
 *
 * <p>The buffered path recovers a missing table through {@link
 * AppendErrorClassifier#isMissingTable} — {@code NOT_FOUND} or the {@code PERMISSION_DENIED} the
 * service masks a missing table behind. That verdict was measured on the <em>default-stream</em>
 * path, where the writer's first RPC is an append; {@code CreateWriteStream} is a different RPC,
 * and applying the same verdict to it was an inference. This class makes it an observation, which
 * only the real service can give: the goccy emulator answers {@code UNKNOWN} here (an
 * emulator-support gap — #326, reported upstream as goccy/bigquery-emulator#504 and pinned by
 * {@link BigQueryEmulatorMissingTableDeviationITCase}), so no emulator test can reach the question.
 *
 * <p>Four cases, in the order the writer meets them: the raw response for a missing table, the
 * propagation window right after the table is created, the same window measured at the writer's
 * <em>append</em> against the {@code FlushRows} the window was observed at, and a whole job that
 * has to create its destination before it can write. All but the last go through {@link
 * WriteClientBufferedStreamService} directly rather than through a job, because a job's recovery
 * swallows exactly the response being measured.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryBufferedStreamMissingTableITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigQueryBufferedStreamMissingTableITCase.class);

    private static final String RUN_ID = TestNames.runId();

    /** Never created, by design: the whole point of the first case. */
    private static final String ABSENT_TABLE = "buffered_missing_absent_" + RUN_ID;

    /** The positive control of the first case: a table that <em>is</em> there. */
    private static final String CONTROL_TABLE = "buffered_missing_control_" + RUN_ID;

    private static final String PROPAGATION_TABLE = "buffered_missing_propagation_" + RUN_ID;
    private static final String AUTO_CREATED_TABLE = "buffered_missing_autocreate_" + RUN_ID;

    /**
     * Trials of the append case, one table each — twenty is what a single run takes, and the answer
     * ADR-0030 records is seven of them.
     *
     * <p>At the flush rate that answer measured (11 of 140 trials, 8%) one run of twenty shows no
     * flush denial about 20% of the time, and such a run says nothing about appends however clean
     * it looks — which is why both counts are logged and neither absence is read as a result.
     * Twenty still beats a handful: zero denied appends in twenty puts the 95% upper bound on their
     * per-trial rate at 14%, where three trials would put it near 60%.
     */
    private static final int APPEND_TRIALS = 20;

    private static final long RECORD_COUNT = 20;

    @AfterAll
    static void cleanUp() {
        // ABSENT_TABLE too: if a run ever does create it, the next one must not find it there.
        RealBigQuery.deleteTables(
                ABSENT_TABLE, CONTROL_TABLE, PROPAGATION_TABLE, AUTO_CREATED_TABLE);
        RealBigQuery.deleteTables(
                IntStream.rangeClosed(1, APPEND_TRIALS)
                        .mapToObj(BigQueryBufferedStreamMissingTableITCase::appendTable)
                        .toArray(String[]::new));
    }

    private static String appendTable(int trial) {
        return "buffered_missing_append_" + RUN_ID + "_" + trial;
    }

    @Test
    void createWriteStreamOnAMissingTableAnswersAMaskedPermissionDenied() throws Exception {
        TableDestination destination = RealBigQuery.destination(ABSENT_TABLE);

        Throwable failure;
        try (BufferedStreamService service = service()) {
            // The premise, without which this case passes just as well on credentials that lost
            // bigquery.tables.* — where the masked code would be a plain denial and would prove
            // nothing about existence-masking. A stream opening on a table that *is* there
            // establishes the permission is held; only then does the absent table mean anything.
            RealBigQuery.createTable(
                    CONTROL_TABLE, new NameColumnSerializer().getTableSchema(null));
            assertThat(service.createBufferedStream(RealBigQuery.destination(CONTROL_TABLE)))
                    .isNotBlank();

            failure = catchThrowable(() -> service.createBufferedStream(destination));
        }

        // Logged, not asserted on: the message is the service's prose and nothing pins it, but it
        // is what a reader of the record needs, so a run has to print it.
        LOG.info("CreateWriteStream on a missing table answered: {}", String.valueOf(failure));
        assertThat(failure).isNotNull();
        assertThat(AppendErrorClassifier.hasCode(failure, Status.Code.PERMISSION_DENIED)).isTrue();
        // The verdict the writer routes on, asserted separately from the code that produces it:
        // widening isMissingTable is what made auto-creation reachable on this path at all.
        assertThat(AppendErrorClassifier.isMissingTable(failure)).isTrue();
    }

    @Test
    void aStreamOpensOnAJustCreatedTableWithinTheRecoveryBudget() throws Exception {
        RealBigQuery.createTable(
                PROPAGATION_TABLE, new NameColumnSerializer().getTableSchema(null));
        TableDestination destination = RealBigQuery.destination(PROPAGATION_TABLE);
        RetrySchedule schedule = BufferedStreamOptions.builder().build().toRecoverySchedule();

        String stream = null;
        try (BufferedStreamService service = service()) {
            for (int attempt = 1; attempt <= schedule.maxAttempts(); attempt++) {
                String[] opened = new String[1];
                Throwable failure =
                        catchThrowable(() -> opened[0] = service.createBufferedStream(destination));
                if (failure == null) {
                    stream = opened[0];
                    LOG.info("A stream opened on the just-created table at attempt {}", attempt);
                    break;
                }
                // The record this case exists for: whether the window masks the same way the
                // missing table did, and under which permission. A run that never fails here is
                // itself a result — the window closed before the first attempt.
                LOG.info(
                        "Attempt {} on the just-created table answered: {}",
                        attempt,
                        String.valueOf(failure));
                // Transient is allowed through without comment: CreateWriteStream can answer
                // UNAVAILABLE on any run, the writer's own loop retries exactly those, and failing
                // here would accuse the service of a behaviour change it did not make.
                assertThat(
                                AppendErrorClassifier.isMissingTable(failure)
                                        || AppendErrorClassifier.isTransient(failure))
                        .as("the propagation window must stay a missing-table verdict")
                        .isTrue();
                if (attempt < schedule.maxAttempts()) {
                    Thread.sleep(schedule.backoffMs(attempt));
                }
            }
        }

        // Discriminating: a window that never closed inside the budget the writer gives it would
        // leave this null, which is the failure mode the writer's own loop would hit.
        assertThat(stream).isNotNull();
    }

    /**
     * Does the propagation window reach an <em>append</em>, the way it reaches the commit?
     *
     * <p>The writer takes a missing-table verdict when it opens a stream and nowhere on its append
     * side, so a masked {@code PERMISSION_DENIED} arriving at an append fails the job. Whether one
     * can arrive is a measurement and not a deduction: the append does not ride the connection
     * {@code CreateWriteStream} just succeeded on, because {@code
     * StreamWriter.newBuilder(streamName, client)} copies the client's settings and opens a
     * connection of its own, so an append reaches the table as cold as the committer's separate
     * client does.
     *
     * <p>{@link #APPEND_TRIALS} trials, a fresh table each, driving the writer's own sequence and
     * pausing before no RPC that has not already failed — the window is caught by being fast, and
     * it is not provokable on demand. Where a retry does wait, it waits the writer's own backoff,
     * so "cleared within the budget" means the budget a job actually has.
     *
     * <p>The {@code FlushRows} at the end is a <strong>positive control</strong> rather than
     * coverage. It is the RPC the window was actually observed at, so without it a run reporting no
     * append failure cannot be told apart from a run in which the window never opened at all, and
     * "no append failures" would read as an answer while being no evidence. The two long-lived
     * services are the shape a job has and not a saving: one subtask holds a single client across
     * every stream it opens, and the committer holds a second one that has appended nothing.
     */
    @Test
    // Above the class-level 600, because a caught window costs wall-clock and losing the
    // measurement to a timeout is the one failure this case cannot afford. Twenty trials measured
    // 46-58 s across seven runs, the window cleared by the second or third attempt throughout, so
    // thirty times the observed cost. It is not a proof against every run: twenty trials each
    // waiting out a full ~55 s budget on all three arms would exceed it — but by then every denial
    // is already in the log, and a window that never closes ends the run at the first trial's
    // assertion rather than at this timeout.
    @Timeout(1800)
    void anAppendOnAJustCreatedTableIsMeasuredAgainstTheFlushThatSawTheWindow() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        TableSchema schema = serializer.getTableSchema(null);
        ProtoRows row =
                ProtoRows.newBuilder().addSerializedRows(serializer.serialize("row0")).build();
        RetrySchedule schedule = BufferedStreamOptions.builder().build().toRecoverySchedule();
        // Wrapped exactly as both storage sinks wrap it, so no reading of this measurement can rest
        // on the probe having created its tables by some route the writer does not take — and so a
        // creation the per-table quota rejects retries here as it would in a job.
        TableAdmin tableAdmin = new RetryingTableAdmin(new BigQueryTableAdmin(), schedule);

        int streamHits = 0;
        int appendHits = 0;
        int flushHits = 0;

        try (BufferedStreamService writerService = service();
                BufferedStreamService commitService = service()) {
            for (int trial = 1; trial <= APPEND_TRIALS; trial++) {
                TableDestination destination = RealBigQuery.destination(appendTable(trial));

                // The writer's own first RPC, against a table that is not there. Asserted rather
                // than assumed: a trial whose table somehow existed would measure nothing.
                Throwable absent =
                        catchThrowable(() -> writerService.createBufferedStream(destination));
                assertThat(absent)
                        .as("trial %d: a stream must not open on a table that is not there", trial)
                        .isNotNull();
                assertRecognised(absent, trial, "CreateWriteStream on the absent table");

                tableAdmin.create(destination, schema, TableCreateOptions.defaults());

                String stream = null;
                for (int attempt = 1; attempt <= schedule.maxAttempts(); attempt++) {
                    String[] opened = new String[1];
                    Throwable failure =
                            catchThrowable(
                                    () ->
                                            opened[0] =
                                                    writerService.createBufferedStream(
                                                            destination));
                    if (failure == null) {
                        stream = opened[0];
                        break;
                    }
                    streamHits += AppendErrorClassifier.isExistenceMasked(failure) ? 1 : 0;
                    assertRecognised(failure, trial, "CreateWriteStream after the creation");
                    Thread.sleep(schedule.backoffMs(attempt));
                }
                assertThat(stream)
                        .as("trial %d: the stream must open within the recovery budget", trial)
                        .isNotNull();

                // A fresh appender per attempt, which is both simpler and truer than reusing one: a
                // StreamWriter poisons itself on a connection-level failure, and every attempt then
                // reaches the table over a connection of its own.
                Throwable appendFailure = null;
                for (int attempt = 1; attempt <= schedule.maxAttempts(); attempt++) {
                    try (OffsetRowAppender appender =
                            writerService.openAppender(
                                    stream, serializer.getDescriptor(destination))) {
                        appendFailure = appendAt(appender, row);
                    }
                    if (appendFailure == null
                            || AppendErrorClassifier.isOffsetAlreadyExists(appendFailure)) {
                        // As resendAtSameOffset reads it: the row is at offset 0 either way.
                        appendFailure = null;
                        break;
                    }
                    appendHits += AppendErrorClassifier.isExistenceMasked(appendFailure) ? 1 : 0;
                    assertRecognised(appendFailure, trial, "the first append on the created table");
                    Thread.sleep(schedule.backoffMs(attempt));
                }
                assertThat(appendFailure)
                        .as("trial %d: the append must land within the recovery budget", trial)
                        .isNull();

                // Offset 0 is what prepareCommit would name for one appended row (nextOffset - 1).
                String flushed = stream;
                Throwable flushFailure = null;
                for (int attempt = 1; attempt <= schedule.maxAttempts(); attempt++) {
                    flushFailure = catchThrowable(() -> commitService.flushRows(flushed, 0));
                    if (flushFailure == null) {
                        break;
                    }
                    flushHits += AppendErrorClassifier.isExistenceMasked(flushFailure) ? 1 : 0;
                    assertRecognised(flushFailure, trial, "the first flush on the created table");
                    Thread.sleep(schedule.backoffMs(attempt));
                }
                assertThat(flushFailure)
                        .as("trial %d: the flush must land within the recovery budget", trial)
                        .isNull();
            }
        }

        // The measurement itself. Zero appends beside a non-zero flush count is the interpretable
        // negative — the window opened and the append path did not see it — and zero of both says
        // only that this run never opened the window, which is not an answer about appends.
        LOG.info(
                "The masked PERMISSION_DENIED over {} trial(s) after creating the table:"
                        + " CreateWriteStream {}, the first append {}, the first flush {}",
                APPEND_TRIALS,
                streamHits,
                appendHits,
                flushHits);

        // The one count that must stay zero, asserted after the log so a failure keeps the whole
        // tally. Logging it alone would put the observation that overturns ADR-0030's decision in a
        // weekly run's output and nowhere else, which is the same as not measuring it: the writer's
        // four append-side sites are transient-only *because* this is zero, so the run that finds
        // otherwise has to stop.
        assertThat(appendHits)
                .as(
                        "an append met the masked PERMISSION_DENIED, which docs/adr/0030 records as"
                                + " never observed in 140 trials — the four append-side recovery"
                                + " sites need re-deciding, and this observation earns a new issue"
                                + " naming that ADR")
                .isZero();
    }

    /**
     * Appends one row at offset 0 and returns what the append answered, {@code null} on success.
     *
     * <p>Reads a response-embedded error as a failure the way {@code
     * BigQueryBufferedStreamWriter#responseToThrowable} does: the Storage Write API reports some
     * failures in the response instead of by failing the future, and a probe that only caught the
     * future would report a clean run for one of them.
     */
    @Nullable
    private static Throwable appendAt(OffsetRowAppender appender, ProtoRows rows) {
        try {
            AppendRowsResponse response = appender.append(rows, 0).get(60, TimeUnit.SECONDS);
            if (!response.hasError()) {
                return null;
            }
            Throwable storageError = AppendErrorClassifier.toStorageException(response.getError());
            return storageError != null
                    ? storageError
                    : StatusProto.toStatusRuntimeException(response.getError());
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            // Returned rather than thrown so assertRecognised names it: a 60 s append is a real
            // finding, and it must not be mistaken for one of the two verdicts below.
            return e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting an append", e);
        }
    }

    /**
     * Logs what an RPC answered — the record a reader of ADR-0030 needs, and nothing pins the
     * service's prose so a run has to print it — and holds it to one of the two verdicts the
     * writer's own loop recognises. Anything else means the service changed shape, which is the one
     * thing a standing measurement must not pass over in silence. Transient is allowed through
     * without comment: any of these RPCs can answer {@code UNAVAILABLE} on any run.
     */
    private static void assertRecognised(Throwable failure, int trial, String what) {
        LOG.info("Trial {}: {} answered: {}", trial, what, String.valueOf(failure));
        assertThat(
                        AppendErrorClassifier.isMissingTable(failure)
                                || AppendErrorClassifier.isTransient(failure))
                .as(
                        "trial %d: %s answered neither a missing-table nor a transient verdict,"
                                + " so the service no longer answers what this case measures",
                        trial, what)
                .isTrue();
    }

    @Test
    void aJobWritesIntoATableItHasToCreateFirst() throws Exception {
        Configuration configuration = new Configuration();
        // A bounded restart budget rather than "none": the regression this guards against — the
        // propagation window reaching the committer — is a race the job would survive by
        // restarting, so a fail-fast job here would be a test that fails on timing rather than on
        // behaviour. What pins the committer's allowance deterministically is
        // BufferedStreamCommitterTest; what this case covers is that auto-creation works at all on
        // this write path. Bounded so a permanent failure still ends the run rather than looping on
        // a job that is paying for real BigQuery.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2_000);
        // Two subtasks, so both race to create the same missing table — the concurrent case the
        // TableAdmin's 409-is-success contract exists for. Not more: at ten, BigQuery answers
        // "Exceeded rate limits: too many table update operations for this table".
        env.setParallelism(2);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) index -> "row" + index,
                        RECORD_COUNT,
                        Types.STRING);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "rows")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(RealBigQuery.destination(AUTO_CREATED_TABLE))
                                .serializer(new NameColumnSerializer())
                                // No createDisposition(...) call: CREATE_IF_NEEDED is the default,
                                // and this is the path a user takes without knowing the knob.
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .build());

        env.execute("buffered-stream-auto-creation-it");

        // The table exists only because the sink made it — tableDefinition asserts that itself.
        assertThat(RealBigQuery.tableDefinition(AUTO_CREATED_TABLE).getSchema()).isNotNull();
        String path = RealBigQuery.tablePath(AUTO_CREATED_TABLE);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + path))
                .containsExactly(RECORD_COUNT);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(DISTINCT name) FROM " + path))
                .containsExactly(RECORD_COUNT);
    }

    private static BufferedStreamService service() throws Exception {
        return new WriteClientBufferedStreamService(null, BufferedStreamOptions.builder().build());
    }
}
