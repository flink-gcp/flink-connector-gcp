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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cross-checks {@code sink.write-method} brings, refused <em>in the planner</em> rather than
 * through {@code FactoryMocks} — which is where a SQL user meets them, and where Flink's own
 * wrapping of the exception happens.
 *
 * <p>No BigQuery of any kind is contacted: rejection cases throw while the job graph is being
 * built, and acceptance cases stop at {@code explainSql}, before a client is opened. The endpoint
 * below is therefore a string that only has to parse. That is why this is not an ITCase — an
 * emulator container would be infrastructure no test uses.
 *
 * <p>Refusals are all any local test can carry for the two write methods this issue adds, and the
 * emulator is why. FILE_LOADS stages to Cloud Storage, which nothing here stands in for — the last
 * case is that fact reaching a SQL user. Exactly-once was tried against the emulator and dropped,
 * measured 2026-08-06 on goccy 0.8.1: its {@code CreateWriteStream} answers {@code UNKNOWN} for a
 * missing table, so {@code create-if-needed} cannot auto-create (issue 326), and with the table
 * pre-created its {@code AppendRows} acknowledges an append of two rows at offset 2 when offset 0
 * was requested — the emulator assigns its own offsets, so {@code BigQueryBufferedStreamWriter}'s
 * consistency check fails on the first append. The same gap {@code
 * BigQueryBufferedStreamSmokeITCase} documents from the other side. Both round trips are therefore
 * gated: {@code BigQueryTableExactlyOnceITCase} and {@code BigQueryTableFileLoadsITCase}.
 */
class BigQueryTableWriteMethodsPlanTest {

    private static final String PROJECT = "my-project";
    private static final String DATASET = "my_dataset";

    private static String withOptions(String table, String... keysAndValues) {
        return TableDdl.withOptions(PROJECT, DATASET, table, keysAndValues);
    }

    private static TableEnvironment tableEnvironmentWith(String ddl) {
        return tableEnvironmentWith(new Configuration(), ddl);
    }

    private static TableEnvironment tableEnvironmentWith(Configuration configuration, String ddl) {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());
        tEnv.executeSql(ddl);
        return tEnv;
    }

    private static TableEnvironment batchTableEnvironmentWith(
            Configuration configuration, String ddl) {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inBatchMode()
                                .withConfiguration(configuration)
                                .build());
        tEnv.executeSql(ddl);
        return tEnv;
    }

    private static Configuration checkpointingEvery(Duration interval) {
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, interval);
        return configuration;
    }

    @Test
    void refusesATuningKeyOfAnotherWriteMethodAtPlanTime() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "wrong_family",
                                        "sink.write-method",
                                        "storage-api-exactly-once",
                                        "sink.default-stream.flush-interval",
                                        "5 s"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .isInstanceOf(ValidationException.class)
                // A phrase only this connector's message carries: FactoryUtil attaches a dump of
                // the whole WITH clause to anything the factory throws, so the option key alone
                // would match with the check deleted.
                .hasStackTraceContaining("but this table's write method is")
                .hasStackTraceContaining("sink.default-stream.flush-interval");
    }

    @Test
    void acceptsSchemaEvolutionUnderExactlyOnceAtPlanTime() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        checkpointingEvery(Duration.ofSeconds(1)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "schema_update",
                                        "sink.write-method",
                                        "storage-api-exactly-once",
                                        "sink.schema-update.allow-new-fields",
                                        "true"));

        assertThatCode(() -> tEnv.explainSql("INSERT INTO events VALUES ('alice')"))
                .doesNotThrowAnyException();
    }

    @Test
    void refusesAnEmulatorEndpointUnderFileLoadsAtPlanTime() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix",
                                        "emulator-endpoint",
                                        "localhost:9060"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .isInstanceOf(ValidationException.class)
                // Again the opening clause: the builder says "which the BigQuery emulator does not
                // provide", one word away from the tail of this connector's message.
                .hasStackTraceContaining("point at a BigQuery emulator")
                .hasStackTraceContaining("emulator-endpoint");
    }

    // The two below are the sinks' own graph-construction rules rather than the factory's, and
    // they are here for a reason the factory tests cannot cover: whether a TableEnvironment's
    // execution.checkpointing.* reaches
    // committables.getExecutionEnvironment().getCheckpointConfig()
    // is planner plumbing — the planner builds that StreamExecutionEnvironment itself. Until these
    // existed the only thing pinning it was a gated ITCase, which costs real BigQuery to run.

    @Test
    void refusesExactlyOnceInStreamingWithoutCheckpointing() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "exactly_once_no_checkpointing",
                                        "sink.write-method",
                                        "storage-api-exactly-once"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .hasStackTraceContaining("requires checkpointing");
    }

    @Test
    void refusesFileLoadsInStreamingWithoutCheckpointing() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_no_checkpointing",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                // The factory's interval rule is silent here, and has to be: an absent
                // interval cannot be compared with a floor, and checkpointing being off is
                // this message's own subject.
                .hasStackTraceContaining("requires checkpointing");
    }

    // The FILE_LOADS streaming rules below are the factory's, restated in DDL keys because
    // BigQueryFileLoadsSink's own messages name WriteDisposition.WRITE_APPEND and
    // FileLoadsOptions.minCheckpointInterval(...) — vocabulary a SQL user cannot act on. Those
    // sink rules stay where they are as the DataStream backstop, and BigQueryFileLoadsSinkTopology
    // Test is what exercises them: both layers read the same values, so on every SQL path where
    // the factory sees the final one it decides first.

    @Test
    void refusesACheckpointIntervalBelowTheFileLoadsFloorByKeyName() {
        // The most likely FILE_LOADS failure in SQL: the connector's floor is two minutes and a
        // SQL Client checkpoint interval is usually shorter.
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        checkpointingEvery(Duration.ofSeconds(30)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_fast_checkpoints",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .isInstanceOf(ValidationException.class)
                // A clause only the factory's message carries: the sink says "is below the
                // FILE_LOADS minimum" and both messages carry the quota sentence, so neither
                // would tell the two apart.
                .hasStackTraceContaining("is shorter than the smallest checkpoint interval")
                // The value, not just the clause. This is the only local assertion that the
                // interval a TableEnvironment was configured with reaches the *factory* — the
                // planner builds the StreamExecutionEnvironment whose configuration the session
                // TableConfig falls back to, and nothing else here pins that.
                .hasStackTraceContaining("'execution.checkpointing.interval' (30000 ms)")
                .hasStackTraceContaining("sink.file-loads.min-checkpoint-interval");
    }

    @Test
    void refusesANonAppendWriteDispositionInStreamingByKeyName() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        checkpointingEvery(Duration.ofMinutes(5)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_truncate",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix",
                                        "sink.file-loads.write-disposition",
                                        "write-truncate"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .isInstanceOf(ValidationException.class)
                // The sink's message says "supports WriteDisposition.WRITE_APPEND only"; this
                // clause is the factory's alone.
                .hasStackTraceContaining("cannot be used in streaming execution")
                // The DDL spelling the user typed, which the sink's message never carries: it
                // names the Java constant WRITE_TRUNCATE beside WRITE_APPEND deliberately.
                .hasStackTraceContaining(
                        "Option 'sink.file-loads.write-disposition' = 'write-truncate'");
    }

    @Test
    void refusesWriteTruncateDataInStreamingByKeyName() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        checkpointingEvery(Duration.ofMinutes(5)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_truncate_data",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix",
                                        "sink.file-loads.write-disposition",
                                        "write-truncate-data"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be used in streaming execution")
                .hasStackTraceContaining(
                        "Option 'sink.file-loads.write-disposition' = 'write-truncate-data'");
    }

    @Test
    void acceptsBothStreamingOnlyViolationsInBatchExecution() {
        // Both rules are streaming's alone — a batch job loads once at end of input, so truncating
        // is a meaningful write and no checkpoint issues a load job. The table therefore violates
        // *both*, and the 30 s interval matters as much as the disposition: a session carrying
        // execution.checkpointing.interval is routine from SQL Client and harmless in batch, so
        // without it here the mode gate would be pinned for the disposition rule only and moving
        // the interval rule above the gate would keep the whole suite green.
        TableEnvironment tEnv =
                batchTableEnvironmentWith(
                        checkpointingEvery(Duration.ofSeconds(30)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_batch_truncate",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix",
                                        "sink.file-loads.write-disposition",
                                        "write-truncate"));

        assertThatCode(() -> tEnv.explainSql("INSERT INTO events VALUES ('alice')"))
                .doesNotThrowAnyException();
    }

    @Test
    void acceptsWriteTruncateDataInBatchExecution() {
        TableEnvironment tEnv =
                batchTableEnvironmentWith(
                        new Configuration(),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_batch_truncate_data",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix",
                                        "sink.file-loads.write-disposition",
                                        "write-truncate-data"));

        assertThatCode(() -> tEnv.explainSql("INSERT INTO events VALUES ('alice')"))
                .doesNotThrowAnyException();
    }

    @Test
    void acceptsAnIntervalThatExactlyMeetsAFloorTheDdlLowered() {
        // Two things at once. That the factory compares against the option rather than against a
        // copy of FileLoadsOptions.DEFAULT_MIN_CHECKPOINT_INTERVAL made in the table layer — this
        // is the same 30 s the case above refuses. And that the comparison is strict: an interval
        // equal to the floor is a floor that is met, so a `<` widened to `<=` fails here.
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        checkpointingEvery(Duration.ofSeconds(30)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_lowered_floor",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix",
                                        "sink.file-loads.min-checkpoint-interval",
                                        "30 s"));

        assertThatCode(() -> tEnv.explainSql("INSERT INTO events VALUES ('alice')"))
                .doesNotThrowAnyException();
    }

    @Test
    void anAutomaticExecutionModeIsRefusedByFlinkBeforeAnyFactory() {
        // Why the factory's mode guard needs no AUTOMATIC branch, measured 2026-08-09 on Flink
        // 2.2.1 (one run): the Table API refuses that mode itself, in DefaultPlannerFactory, when
        // the TableEnvironment is created — before any DDL and long before a connector factory.
        // BigQueryFileLoadsSink keeps its own AUTOMATIC refusal for the DataStream path, which is
        // where the mode can actually arrive. Should a later Flink accept it here, this fails and
        // the guard's silent branch becomes reachable and has to be argued.
        //
        // A framework assertion in a connector test, deliberately: the fact it pins is a premise
        // of this connector's code, and nothing in the connector can observe it. The string is
        // byte-identical in flink-table-planner 1.20.4, 2.2.1 and 2.3.0 — the whole range
        // verify-flink covers — so it is a premise, not a version-fragile probe.
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);

        assertThatThrownBy(
                        () ->
                                TableEnvironment.create(
                                        EnvironmentSettings.newInstance()
                                                .withConfiguration(configuration)
                                                .build()))
                .isInstanceOf(TableException.class)
                .hasStackTraceContaining(
                        "Unsupported mode 'AUTOMATIC' for 'execution.runtime-mode'");
    }

    @Test
    void acceptsAFileLoadsTableWhoseCheckpointIntervalClearsTheFloor() {
        // The success side, so the refusals above cannot pass by refusing everything.
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        checkpointingEvery(Duration.ofMinutes(5)),
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "file_loads_ok",
                                        "sink.write-method",
                                        "file-loads",
                                        "sink.file-loads.staging-path",
                                        "gs://bucket/prefix"));

        // Builds the job graph and stops there: executing it would need real Cloud Storage.
        assertThatCode(() -> tEnv.explainSql("INSERT INTO events VALUES ('alice')"))
                .doesNotThrowAnyException();
    }
}
