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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.grpc.Status;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * emulator-support gap, #326), so no emulator test can reach the question.
 *
 * <p>Three cases, in the order the writer meets them: the raw response for a missing table, the
 * propagation window right after the table is created, and a whole job that has to create its
 * destination before it can write. The first two go through {@link
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

    private static final long RECORD_COUNT = 20;

    @AfterAll
    static void cleanUp() {
        // ABSENT_TABLE too: if a run ever does create it, the next one must not find it there.
        RealBigQuery.deleteTables(
                ABSENT_TABLE, CONTROL_TABLE, PROPAGATION_TABLE, AUTO_CREATED_TABLE);
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
                                .destination(RealBigQuery.destination(AUTO_CREATED_TABLE))
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
