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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
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
 * <p>No BigQuery of any kind is contacted: every case throws while the job graph is being built,
 * before a client is opened, so the endpoint below is a string that only has to parse. That is why
 * this is not an ITCase — an emulator container would be infrastructure no test uses.
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
    void refusesSchemaEvolutionUnderExactlyOnceAtPlanTime() {
        TableEnvironment tEnv =
                tableEnvironmentWith(
                        "CREATE TABLE events (name STRING) "
                                + withOptions(
                                        "schema_update",
                                        "sink.write-method",
                                        "storage-api-exactly-once",
                                        "sink.schema-update.allow-new-fields",
                                        "true"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')"))
                .isInstanceOf(ValidationException.class)
                // The opening clause, deliberately: the builder's own rejection — which this level
                // reaches, unlike a FactoryMocks test — says "pinned when the stream is created"
                // verbatim, so that phrase would not tell the two apart.
                .hasStackTraceContaining("ask the sink to evolve the table schema")
                .hasStackTraceContaining("sink.schema-update.allow-new-fields");
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

    // The four below are the sinks' own graph-construction rules rather than the factory's, and
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
                .hasStackTraceContaining("requires checkpointing");
    }

    @Test
    void refusesACheckpointIntervalBelowTheFileLoadsFloor() {
        // The most likely FILE_LOADS failure in SQL: the connector's floor is two minutes and a
        // SQL Client checkpoint interval is usually shorter. Proves the interval a
        // TableEnvironment was configured with is the one the sink reads.
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
                .hasStackTraceContaining("is below the")
                .hasStackTraceContaining("1,500 load jobs per table per day");
    }

    @Test
    void refusesANonAppendWriteDispositionInStreaming() {
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
                .hasStackTraceContaining("the write disposition is WRITE_TRUNCATE");
    }

    @Test
    void acceptsAFileLoadsTableWhoseCheckpointIntervalClearsTheFloor() {
        // The success side, so the three refusals above cannot pass by refusing everything.
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
