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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStreamView;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A <b>manual probe</b>, not a weekly E2E test: connector-driven schema evolution on the default
 * stream against <b>real</b> BigQuery, run end to end through the production {@code
 * StreamWriterRowAppenderFactory} and the production schema-wait schedule.
 *
 * <p><b>Measured 2026-07-31</b> (us-central1, one run): the REST {@code tables.update} succeeds
 * instantly and the first append with the new column is rejected fast ({@code
 * SchemaMismatchedException}), but from there the service did not accept the new column for <b>~1 h
 * 56 m</b> — one retry's append future hung ~35 minutes before failing (with {@code
 * MaximumRequestCallbackWaitTimeExceededException} underneath), the next hung ~79 minutes before
 * succeeding, and the ~35-minute "failed" append had been applied server-side anyway, landing the
 * row twice (at-least-once duplicates working as specified, but showing the hang is not a clean
 * rejection). The emulator's instant success for the same scenario ({@link
 * BigQuerySchemaEvolutionITCase}) is exactly the false design signal issue #16 warns about.
 *
 * <p>That wall time is why this class is <b>deliberately outside the weekly E2E suite</b>: it would
 * consume the whole runner budget. {@code scripts/e2e-gated-its.sh} derives the suite from the
 * {@code BQ_IT_PROJECT} annotation literal, so this class gates on {@code BQ_IT_SCHEMA_EVOLUTION}
 * (plus {@code BQ_IT_DATASET}) instead and the script never sees it — <b>do not "fix" the gating to
 * match the other real-GCP ITCases</b>. {@code BQ_IT_PROJECT} and {@code GOOGLE_CLOUD_PROJECT} must
 * still be set for the run to work, and {@code -Dtest.excluded.groups=} is what clears the
 * exclusion the build applies to the {@code gated} tag by default (issue #245) — without it this
 * class is not selected at all, whatever the environment holds:
 *
 * <pre>{@code
 * BQ_IT_SCHEMA_EVOLUTION=1 ./mvnw -pl flink-connector-gcp-bigquery test-compile \
 *   surefire:test@integration-tests -Dtest.excluded.groups= \
 *   -Dtest=BigQueryDefaultStreamSchemaEvolutionITCase
 * }</pre>
 *
 * <p>The evolution <em>mechanics</em> — fingerprint change detection, reconcile, continued writes
 * on one writer — stay pinned by the emulator ITCase and by {@link
 * BigQueryDefaultStreamWriterSchemaEvolutionTest} against fakes; this probe exists to measure the
 * real service's propagation behavior, and reruns are the trap for the hang recorded above — its
 * record and open hypotheses are in issue #174, closed as wait-and-see. A captured reproduction
 * gets a new issue referencing #174, with the run log attached.
 *
 * <p><b>Instrumentation for #174's hypotheses</b> (each maps to one of the issue's checkable
 * predictions; everything goes to the run's console, so a teed run log is the measurement):
 *
 * <ul>
 *   <li><b>H1 (watchdog evasion / SDK-internal retries)</b>: the SDK logs its reconnects, in-stream
 *       retries and error responses through {@code java.util.logging}, which nothing in this build
 *       bridges to slf4j — {@link #captureSdkLogs()} attaches a timestamped console handler at
 *       {@code FINE} to the {@code com.google.cloud.bigquery.storage.v1} logger so "Retrying
 *       default stream message …", "Messages blocked for retry …", "Connection is going to be
 *       reestablished …" and "Got error message: …" (FINE, full server responses) appear in the run
 *       log with wall-clock times.
 *   <li><b>H3 (backend propagation)</b>: a {@link SchemaViewPoller} thread logs, every {@link
 *       #POLL_INTERVAL_MS}, the column list as seen by the Storage Write API ({@code
 *       GetWriteStream} with view {@code FULL} on the default stream) and by REST {@code
 *       tables.get} — when each view first shows the new column is the propagation measurement.
 *   <li><b>H2 (pooled connection pins the stale schema)</b>: a {@link NonPooledCanary} thread,
 *       started at the evolution, periodically appends one {@code canary-<n>} row through a
 *       <b>fresh non-pooled</b> {@link StreamWriter} (same schema-to-descriptor path as the
 *       production writer, no connection pool). The gap between the canary's first success and the
 *       pooled production writer's is the discriminator. Canary rows are filtered out of the final
 *       assertion.
 * </ul>
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_SCHEMA_EVOLUTION", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(10_800)
class BigQueryDefaultStreamSchemaEvolutionITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigQueryDefaultStreamSchemaEvolutionITCase.class);

    private static final String TABLE = "default_stream_evolution_it_" + TestNames.runId();

    private static final long POLL_INTERVAL_MS = 30_000;
    private static final long CANARY_INITIAL_DELAY_MS = 60_000;
    private static final long CANARY_INTERVAL_MS = 180_000;
    private static final long CANARY_APPEND_TIMEOUT_S = 60;

    /**
     * Strong reference to the SDK's JUL logger: JUL holds loggers weakly, so without it the FINE
     * level and handler installed by {@link #captureSdkLogs()} could be garbage-collected away
     * mid-run.
     */
    private static final java.util.logging.Logger SDK_JUL_LOGGER =
            java.util.logging.Logger.getLogger("com.google.cloud.bigquery.storage.v1");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static TableFieldSchema nullableString(String name) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRING)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    private static final TableSchema V1 =
            TableSchema.newBuilder().addFields(nullableString("name")).build();
    private static final TableSchema V2 =
            TableSchema.newBuilder()
                    .addFields(nullableString("name"))
                    .addFields(nullableString("note"))
                    .build();

    @BeforeAll
    static void captureSdkLogs() {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
        handler.setFormatter(
                new Formatter() {
                    @Override
                    public String format(LogRecord record) {
                        String thrown = record.getThrown() == null ? "" : " " + record.getThrown();
                        return String.format(
                                "%s SDK-JUL %s %s: %s%s%n",
                                record.getInstant(),
                                record.getLevel(),
                                record.getLoggerName(),
                                formatMessage(record),
                                thrown);
                    }
                });
        SDK_JUL_LOGGER.setLevel(Level.FINE);
        // Without this, INFO+ records would also reach the JUL root handler and print twice.
        SDK_JUL_LOGGER.setUseParentHandlers(false);
        SDK_JUL_LOGGER.addHandler(handler);
    }

    @AfterAll
    static void dropTable() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void connectorWidensTheTableAndWritesThroughRealPropagation() throws Exception {
        // Fail loud, not with a bare NPE: this class's gate deliberately omits the BQ_IT_PROJECT
        // annotation (see the class javadoc), so nothing else checks the variable is set.
        assertThat(RealBigQuery.project())
                .as(
                        "BQ_IT_PROJECT (and GOOGLE_CLOUD_PROJECT) must be set alongside"
                                + " BQ_IT_SCHEMA_EVOLUTION")
                .isNotNull();
        RealBigQuery.createTable(TABLE, V1);
        TableDestination destination = RealBigQuery.destination(TABLE);
        SchemaViewPoller poller = new SchemaViewPoller(destination);
        poller.start();
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(destination)
                                .serializer(serializer)
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new StreamWriterRowAppenderFactory(sink.getOptions()),
                        new BigQueryTableAdmin(),
                        TestSinkWriterMetricGroup.create());
        NonPooledCanary canary = new NonPooledCanary(destination);
        try {
            LOG.info("PROBE steady-state append starting");
            writer.write("alice", CONTEXT);
            writer.flush(false);
            LOG.info("PROBE steady-state append done");

            serializer.evolveTo(V2);
            canary.start();

            LOG.info("PROBE evolved append starting (this is the measured window)");
            writer.write("bob:hello", CONTEXT);
            writer.flush(false);
            LOG.info("PROBE evolved append accepted by the pooled production writer");
        } finally {
            canary.stopAndJoin();
            poller.stopAndJoin();
            writer.close();
        }

        // The sink widened the table itself, and the evolved column's value is queryable — the
        // half the emulator cannot show. DISTINCT because the propagation wait retries appends,
        // and a hung-but-applied append lands its row twice (measured; at-least-once permits it).
        // Canary rows are the H2 instrumentation's, not the sink's, so they are filtered out.
        assertThat(tableFieldNames()).containsExactly("name", "note");
        List<String> rows = new ArrayList<>();
        for (FieldValueList row :
                RealBigQuery.queryRows(
                        "SELECT DISTINCT name, note FROM "
                                + RealBigQuery.tablePath(TABLE)
                                + " WHERE name NOT LIKE 'canary%' ORDER BY name")) {
            rows.add(
                    row.get(0).getStringValue()
                            + "|"
                            + (row.get(1).isNull() ? "" : row.get(1).getStringValue()));
        }
        assertThat(rows).containsExactly("alice|", "bob|hello");
    }

    private static List<String> tableFieldNames() {
        List<String> fieldNames = new ArrayList<>();
        for (Field field : RealBigQuery.tableFields(TABLE)) {
            fieldNames.add(field.getName());
        }
        return fieldNames;
    }

    /**
     * H3 instrumentation: logs the Storage Write API's schema view ({@code GetWriteStream}, view
     * {@code FULL}, a fresh <em>client and channel</em> per tick — H2's suspected mechanism is
     * metadata pinned to an established connection, so a poller reusing one long-lived channel
     * could not call its own view fresh) and REST {@code tables.get}'s view, every {@link
     * #POLL_INTERVAL_MS}, logging a line per tick so liveness is visible.
     *
     * <p>The default stream's {@code GetWriteStream} resource name is tried in both spellings on
     * the first tick ({@code .../streams/_default} per the resource-name pattern, {@code
     * .../_default} as {@code AppendRows} spells it) and the accepted one is kept; the emulator
     * only registers the former (goccy/bigquery-emulator#342), so the real service's answer is
     * itself a small measurement.
     */
    private static final class SchemaViewPoller extends Thread {
        private final TableDestination destination;
        private volatile boolean stopped;

        SchemaViewPoller(TableDestination destination) {
            super("schema-view-poller");
            this.destination = destination;
            setDaemon(true);
        }

        @Override
        public void run() {
            String lastStorage = null;
            String lastRest = null;
            String streamName = null;
            try {
                while (!stopped) {
                    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
                        if (streamName == null) {
                            streamName = resolveStreamName(client);
                        }
                        String storage = storageView(client, streamName);
                        String rest = restView();
                        if (!storage.equals(lastStorage) || !rest.equals(lastRest)) {
                            LOG.info(
                                    "POLLER view changed: GetWriteStream(FULL)={} tables.get={}",
                                    storage,
                                    rest);
                            lastStorage = storage;
                            lastRest = rest;
                        } else {
                            LOG.info("POLLER unchanged: storage={} rest={}", storage, rest);
                        }
                    } catch (RuntimeException | java.io.IOException e) {
                        LOG.warn("POLLER tick failed: {}", e.toString());
                    }
                    Thread.sleep(POLL_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private String resolveStreamName(BigQueryWriteClient client) {
            String tablePath = destination.toTablePath();
            for (String candidate :
                    new String[] {tablePath + "/streams/_default", tablePath + "/_default"}) {
                try {
                    client.getWriteStream(
                            GetWriteStreamRequest.newBuilder().setName(candidate).build());
                    LOG.info("POLLER GetWriteStream accepts the name form {}", candidate);
                    return candidate;
                } catch (RuntimeException e) {
                    LOG.info("POLLER GetWriteStream rejects {}: {}", candidate, e.toString());
                }
            }
            throw new IllegalStateException("no GetWriteStream name form accepted");
        }

        private String storageView(BigQueryWriteClient client, String streamName) {
            WriteStream stream =
                    client.getWriteStream(
                            GetWriteStreamRequest.newBuilder()
                                    .setName(streamName)
                                    .setView(WriteStreamView.FULL)
                                    .build());
            List<String> names = new ArrayList<>();
            for (TableFieldSchema field : stream.getTableSchema().getFieldsList()) {
                names.add(field.getName());
            }
            return names.toString();
        }

        private String restView() {
            return tableFieldNames().toString();
        }

        void stopAndJoin() throws InterruptedException {
            stopped = true;
            interrupt();
            join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    /**
     * H2 instrumentation: every {@link #CANARY_INTERVAL_MS} (after an initial delay that lets the
     * connector's {@code tables.update} land), append one {@code canary-<n>} row through a fresh
     * {@link StreamWriter} with <b>no connection pool</b>, built over the same schema-to-descriptor
     * path the production writer uses (an {@link EvolvingSerializer} pinned at V2). Logs every
     * outcome and stops at the first success — the timestamp to compare against the pooled writer's
     * acceptance. Each attempt's writer is closed afterwards; a close on a dead connection can
     * block up to the SDK's 3-minute done-callback wait, which only stretches the cadence, not the
     * measurement.
     */
    private static final class NonPooledCanary extends Thread {
        private final TableDestination destination;
        private volatile boolean stopped;

        NonPooledCanary(TableDestination destination) {
            super("non-pooled-canary");
            this.destination = destination;
            setDaemon(true);
        }

        @Override
        public void run() {
            EvolvingSerializer v2Serializer = new EvolvingSerializer(V2);
            try {
                Thread.sleep(CANARY_INITIAL_DELAY_MS);
                for (int attempt = 1; !stopped; attempt++) {
                    if (appendOnce(v2Serializer, attempt)) {
                        return;
                    }
                    Thread.sleep(CANARY_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean appendOnce(EvolvingSerializer v2Serializer, int attempt) {
            Instant start = Instant.now();
            StreamWriter streamWriter = null;
            try {
                streamWriter =
                        StreamWriter.newBuilder(destination.toTablePath() + "/_default")
                                .setWriterSchema(
                                        ProtoSchemaConverter.convert(
                                                v2Serializer.getDescriptor(destination)))
                                .build();
                ApiFuture<AppendRowsResponse> future =
                        streamWriter.append(
                                ProtoRows.newBuilder()
                                        .addSerializedRows(
                                                v2Serializer.serialize(
                                                        "canary-" + attempt + ":canary"))
                                        .build());
                AppendRowsResponse response = future.get(CANARY_APPEND_TIMEOUT_S, TimeUnit.SECONDS);
                if (response.hasError()) {
                    LOG.info(
                            "CANARY attempt {} rejected in-response after {}: {}",
                            attempt,
                            Duration.between(start, Instant.now()),
                            response.getError());
                    return false;
                }
                LOG.info(
                        "CANARY attempt {} SUCCEEDED after {} — a fresh non-pooled writer's V2"
                                + " append was accepted",
                        attempt,
                        Duration.between(start, Instant.now()));
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return true;
            } catch (Exception e) {
                LOG.info(
                        "CANARY attempt {} failed after {}: {}",
                        attempt,
                        Duration.between(start, Instant.now()),
                        e.toString());
                return false;
            } finally {
                closeQuietly(streamWriter, attempt);
            }
        }

        private static void closeQuietly(StreamWriter streamWriter, int attempt) {
            if (streamWriter == null) {
                return;
            }
            Instant closeStart = Instant.now();
            try {
                streamWriter.close();
            } catch (RuntimeException e) {
                LOG.info("CANARY attempt {} close failed: {}", attempt, e.toString());
            }
            Duration took = Duration.between(closeStart, Instant.now());
            if (took.getSeconds() > 5) {
                LOG.info("CANARY attempt {} close took {}", attempt, took);
            }
        }

        void stopAndJoin() throws InterruptedException {
            stopped = true;
            interrupt();
            join(TimeUnit.SECONDS.toMillis(30));
        }
    }
}
