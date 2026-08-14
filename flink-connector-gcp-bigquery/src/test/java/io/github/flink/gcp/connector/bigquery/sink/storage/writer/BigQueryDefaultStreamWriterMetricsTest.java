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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.UnroutableRecord;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdminException;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the metrics {@link BigQueryDefaultStreamWriter} registers.
 *
 * <p>Every assertion goes through the name a metric registered under, so renaming one — or failing
 * to register it — fails here. The fakes are this class's own: the writer's behavioural tests keep
 * theirs private, and the cases below need pieces of all three of them (scripted append failures, a
 * recording table admin, a dropping failure handler).
 */
class BigQueryDefaultStreamWriterMetricsTest {

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;
    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final TableSchema LIVE =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private static final TableSchema SERIALIZER_SCHEMA =
            LIVE.toBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("email")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
    private final RecordingTableAdmin admin = new RecordingTableAdmin(LIVE);
    private final DroppingHandler handler = new DroppingHandler();

    // ------------------------------------------------------------------
    // Sends
    // ------------------------------------------------------------------

    @Test
    void countsEveryRowHandedToTheClientWithItsPayloadBytes() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.write("bbb", CONTEXT);
        writer.flush(false);

        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numBytesSend")).isEqualTo(5);
        assertThat(counter("numRecordsSendErrors")).isZero();
        // A clean run registers no error class at all — not even UNCLASSIFIED, which is what a
        // failure counted before the success branch would produce.
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    @Test
    void countsARetriedBatchOnlyOnceAndNamesTheStatusItFailedWith() throws Exception {
        // The batch is handed over once and re-appended once; numRecordsSend must report the rows
        // and appendRetries the attempt. A mutant counting the send in retryBatches dies here.
        factory.scriptedResults.add(failedWith(Status.Code.UNAVAILABLE));
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numBytesSend")).isEqualTo(2);
        assertThat(counter("appendRetries")).isEqualTo(1);
        assertThat(errors("UNAVAILABLE")).isEqualTo(1);
    }

    @Test
    void skipsRecordsTheSerializerReturnsNullFor() throws Exception {
        // Each record resolves to its own table, so a skip that opened a stream — or created a
        // table — for the destination it would have gone to shows up as a second open destination.
        BigQueryDefaultStreamWriter<String> writer =
                writer(new SkippingSerializer(), null, (e, context) -> destination(e));

        writer.write("skip-me", CONTEXT);
        writer.write("aa", CONTEXT);
        writer.flush(false);

        // Skipped, not failed: appended nowhere, and never offered to the handler.
        assertThat(factory.allAppendedRows()).containsExactly("aa");
        assertThat(handler.rows).isEmpty();
        assertThat(this.<Integer>gauge("openDestinations")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isZero();
        assertThat(counter("recordsSkipped")).isEqualTo(1);
    }

    @Test
    void countsNothingAsSentWhenTheClientRejectsTheAppendSynchronously() throws Exception {
        factory.throwOnAppend = true;
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(RuntimeException.class);
        assertThat(counter("numRecordsSend")).isZero();
        assertThat(counter("numBytesSend")).isZero();
    }

    // ------------------------------------------------------------------
    // Send errors
    // ------------------------------------------------------------------

    @Test
    void countsARecordTheSerializerRejectedAsASendError() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(new FailingSerializer(), null);

        writer.write("aa", CONTEXT);

        assertThat(handler.rows).hasSize(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void countsAnOversizedRowAsASendError() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer(new OversizedSerializer(), null);

        writer.write("aa", CONTEXT);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void countsTheRowsTheServiceRejectedByIndexAsSendErrors() throws Exception {
        factory.scriptedResults.add(rowLevelError(Map.of(0, "bad row")));
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        // Both rows reached the client, so both were counted as sent; only the survivor was
        // re-appended.
        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        // The re-append that carried the survivors succeeded, and a success is not an error class.
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    @Test
    void routesAnExplicitResolutionFailureWithoutCreatingDestinationState() throws Exception {
        CountingSerializer serializer = new CountingSerializer();
        UnroutableRecord unroutable =
                UnroutableRecord.of(ByteString.copyFromUtf8("original"), "unknown tenant");
        BigQueryDefaultStreamWriter<String> writer =
                writer(serializer, null, (element, context) -> unroutable);

        writer.write("record", CONTEXT);

        assertThat(handler.rows).containsExactly(unroutable);
        assertThat(serializer.invocations).isZero();
        assertThat(factory.created).isEmpty();
        assertThat(this.<Integer>gauge("openDestinations")).isZero();
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", "unresolved", "sendErrors")).isFalse();
    }

    @Test
    void treatsANullResolutionAsFatalBeforeTheFailurePolicy() {
        CountingSerializer serializer = new CountingSerializer();
        BigQueryDefaultStreamWriter<String> writer =
                writer(serializer, null, (element, context) -> null);

        assertThatThrownBy(() -> writer.write("record", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessage("The destination resolver returned null for a record.");
        assertThat(handler.rows).isEmpty();
        assertThat(serializer.invocations).isZero();
        assertThat(factory.created).isEmpty();
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    @Test
    void explicitResolutionFailureFailsTheJobUnderTheDefaultPolicy() {
        UnroutableRecord unroutable =
                UnroutableRecord.of(ByteString.copyFromUtf8("original"), "unknown tenant");
        BigQuerySinkConfig<String> config =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .destinationResolver((element, context) -> unroutable)
                                        .serializer(new CountingSerializer())
                                        .build())
                        .getConfig();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config,
                        factory,
                        admin,
                        metrics,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        assertThatThrownBy(() -> writer.write("record", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("destination unresolved")
                .hasMessageContaining("unknown tenant");
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(factory.created).isEmpty();
    }

    @Test
    void keepsUnexpectedResolverExceptionsFatal() {
        CountingSerializer serializer = new CountingSerializer();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        serializer,
                        null,
                        (element, context) -> {
                            throw new IllegalStateException("resolver bug");
                        });

        assertThatThrownBy(() -> writer.write("record", CONTEXT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("resolver bug");
        assertThat(handler.rows).isEmpty();
        assertThat(serializer.invocations).isZero();
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    // ------------------------------------------------------------------
    // Repairs
    // ------------------------------------------------------------------

    @Test
    void countsTheTablesTheSinkCreates() throws Exception {
        factory.scriptedResults.add(failedWith(Status.Code.NOT_FOUND));
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(admin.creates).containsExactly(DESTINATION);
        assertThat(counter("tablesCreated")).isEqualTo(1);
        assertThat(counter("schemaReconciliations")).isZero();
        assertThat(errors("NOT_FOUND")).isEqualTo(1);
    }

    @Test
    void countsOneCdcCreationRequestBeforeTheAppenderOpens() throws Exception {
        admin.liveSchema = null;
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(new ValidEmptyProtoSerializer())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(List.of("name"))
                                                .build())
                                .build();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        sink.getConfig(),
                        factory,
                        admin,
                        metrics,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        writer.write("aa", CONTEXT);

        assertThat(admin.creates).containsExactly(DESTINATION);
        assertThat(counter("tablesCreated")).isEqualTo(1);
    }

    @Test
    void countsACdcCreationRequestWhenLaterProvisioningFails() throws Exception {
        admin.liveSchema = null;
        admin.cdcFailureAfterCreate =
                new TableAdminException("ALTER denied", new IOException("denied"), true);
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(new ValidEmptyProtoSerializer())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(List.of("name"))
                                                .build())
                                .build();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        sink.getConfig(),
                        factory,
                        admin,
                        metrics,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        assertThatThrownBy(() -> writer.write("aa", CONTEXT))
                .isInstanceOf(TableAdminException.class)
                .hasMessage("ALTER denied");

        assertThat(admin.creates).containsExactly(DESTINATION);
        assertThat(counter("tablesCreated")).isEqualTo(1);
    }

    @Test
    void doesNotCountCdcVerificationOfAnExistingTableAsCreation() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(new ValidEmptyProtoSerializer())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(List.of("name"))
                                                .build())
                                .build();
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        sink.getConfig(),
                        factory,
                        admin,
                        metrics,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        writer.write("aa", CONTEXT);

        assertThat(admin.creates).isEmpty();
        assertThat(counter("tablesCreated")).isZero();
    }

    @Test
    void countsARetriedCreationOnceRatherThanPerAttempt() throws Exception {
        // The counter answers "how many tables did this subtask ask for", not "how many REST calls
        // did it take" — so the two attempts a lost creation race costs must not read as two
        // tables. The writer counts after the admin returns, which is what makes it hold however
        // many times the admin had to ask.
        //
        // The admin is wrapped here as the sink wraps it in production: the retry is the
        // decorator's, so an unwrapped fake would make this case pass for the wrong reason.
        factory.scriptedResults.add(failedWith(Status.Code.NOT_FOUND));
        admin.creationFailures.add(BigQueryDefaultStreamWriterTest.rateLimited(DESTINATION));
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        new RetryingTableAdmin(
                                admin, BigQueryDefaultStreamWriterTest.fastSchedule(3)));

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(admin.creates).containsExactly(DESTINATION, DESTINATION);
        assertThat(counter("tablesCreated")).isEqualTo(1);
    }

    @Test
    void countsTheMaskedPermissionDeniedTheServiceAnswersForAMissingTable() throws Exception {
        // The route PERMISSION_DENIED now takes: the service masks a missing table behind it, so
        // the failure repairs rather than being terminal — and is counted where every other
        // repaired failure is, on the task thread, once per failed append. The two tests that used
        // to cover this code moved to INVALID_ARGUMENT when it stopped being terminal, which would
        // have left the new route with no metric coverage at all.
        factory.scriptedResults.add(failedWith(Status.Code.PERMISSION_DENIED));
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(admin.creates).containsExactly(DESTINATION);
        assertThat(counter("tablesCreated")).isEqualTo(1);
        assertThat(errors("PERMISSION_DENIED")).isEqualTo(1);
    }

    @Test
    void countsTheSchemaUpdatesTheSinkApplies() throws Exception {
        factory.scriptedResults.add(schemaMismatch());
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        new StringSerializer(),
                        SchemaUpdateOptions.builder().allowNewFields().build());

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(admin.updates).hasSize(1);
        assertThat(counter("schemaReconciliations")).isEqualTo(1);
        // A reconciliation is not a creation: the table was there.
        assertThat(counter("tablesCreated")).isZero();
    }

    @Test
    void countsATableRecreatedDuringReconciliationAsACreation() throws Exception {
        // Degenerate reconciliation: the table has meanwhile disappeared, so the sink creates it
        // instead of updating a schema. That is a creation and must not also count as a
        // reconciliation — the two counters answer different questions.
        RecordingTableAdmin vanished = new RecordingTableAdmin(null);
        factory.scriptedResults.add(schemaMismatch());
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(
                                new StringSerializer(),
                                SchemaUpdateOptions.builder().allowNewFields().build(),
                                (element, context) -> DESTINATION),
                        factory,
                        vanished,
                        metrics,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(vanished.creates).containsExactly(DESTINATION);
        assertThat(vanished.updates).isEmpty();
        assertThat(counter("tablesCreated")).isEqualTo(1);
        assertThat(counter("schemaReconciliations")).isZero();
    }

    @Test
    void countsATerminalFailureUnderItsStatusExactlyOnce() throws Exception {
        // The completion callback owns a terminal failure — it never reaches handleFailedAppend —
        // so checkAsyncError counts it, and only the first time: it is called on every write and
        // flush while the task is torn down.
        // INVALID_ARGUMENT rather than PERMISSION_DENIED, which used to be the example here: the
        // service masks a missing table behind PERMISSION_DENIED, so under the default
        // CREATE_IF_NEEDED that code now routes to table creation instead of being terminal (see
        // AppendErrorClassifier#isMissingTable). An INVALID_ARGUMENT naming no rows still is.
        factory.scriptedResults.add(failedWith(Status.Code.INVALID_ARGUMENT));
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);

        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Gauges
    // ------------------------------------------------------------------

    @Test
    void gaugesReportTheInFlightBatchesAndOpenDestinations() throws Exception {
        // A one-byte batching cap sends each record's batch as the next record arrives, so the
        // append that stays unacknowledged is issued without a flush and the whole test runs on
        // one thread — as the writer itself does.
        SettableApiFuture<AppendRowsResponse> unacknowledged = SettableApiFuture.create();
        factory.scriptedResults.add(unacknowledged);
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(new StringSerializer(), null, (e, context) -> destination(e)),
                        factory,
                        admin,
                        metrics,
                        1,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        assertThat(this.<Integer>gauge("openDestinations")).isZero();
        assertThat(this.<Integer>gauge("inFlightBatches")).isZero();

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT);
        assertThat(this.<Integer>gauge("openDestinations")).isEqualTo(2);
        assertThat(this.<Integer>gauge("inFlightBatches")).isZero();

        // Same destination as the first record, so its buffered batch is appended and stays in
        // flight until the scripted future completes.
        writer.write("a", CONTEXT);
        assertThat(this.<Integer>gauge("inFlightBatches")).isEqualTo(1);

        unacknowledged.set(AppendRowsResponse.getDefaultInstance());
        assertThat(this.<Integer>gauge("inFlightBatches")).isZero();
    }

    @Test
    void gaugesAreClearedWhenTheWriterIsClosed() throws Exception {
        // A writer torn down mid-flight must not keep reporting appends nobody will wait for: the
        // reporter can still sample the gauge between close() and the metric group's own close.
        SettableApiFuture<AppendRowsResponse> unacknowledged = SettableApiFuture.create();
        factory.scriptedResults.add(unacknowledged);
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(new StringSerializer(), null, (e, context) -> destination(e)),
                        factory,
                        admin,
                        metrics,
                        1,
                        BigQueryDefaultStreamWriterTest.fastSchedule(3),
                        BigQueryDefaultStreamWriterTest.fastSchedule(3));

        writer.write("a", CONTEXT);
        writer.write("a", CONTEXT);
        assertThat(this.<Integer>gauge("inFlightBatches")).isEqualTo(1);
        assertThat(this.<Integer>gauge("openDestinations")).isEqualTo(1);

        writer.close();

        assertThat(this.<Integer>gauge("inFlightBatches")).isZero();
        assertThat(this.<Integer>gauge("openDestinations")).isZero();
    }

    // ------------------------------------------------------------------
    // Per-destination counters
    // ------------------------------------------------------------------

    @Test
    void registersNoPerDestinationCountersByDefault() throws Exception {
        BigQueryDefaultStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", "p.d.t", "recordsSend")).isFalse();
        assertThat(metrics.hasMetric("destination", "p.d.t", "sendErrors")).isFalse();
    }

    @Test
    void countsPerTableWhenPerDestinationMetricsAreOn() throws Exception {
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(new StringSerializer(), null, (element, context) -> DESTINATION),
                        factory,
                        admin,
                        metrics,
                        DefaultStreamOptions.builder().perDestinationMetrics(true).build());

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(metrics.counterValue("destination", "p.d.t", "recordsSend")).isEqualTo(1);
        assertThat(metrics.counterValue("destination", "p.d.t", "sendErrors")).isZero();
    }

    @Test
    void countsPerTableSendErrorsWhenPerDestinationMetricsAreOn() throws Exception {
        BigQueryDefaultStreamWriter<String> writer =
                new BigQueryDefaultStreamWriter<>(
                        config(new FailingSerializer(), null, (element, context) -> DESTINATION),
                        factory,
                        admin,
                        metrics,
                        DefaultStreamOptions.builder().perDestinationMetrics(true).build());

        writer.write("aa", CONTEXT);

        assertThat(metrics.counterValue("destination", "p.d.t", "sendErrors")).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private long counter(String... identifier) {
        return metrics.counterValue(identifier);
    }

    private long errors(String errorClass) {
        return counter("errorClass", errorClass, "errors");
    }

    private <T> T gauge(String name) {
        return metrics.gaugeValue(name);
    }

    private static TableDestination destination(String element) {
        return TableDestination.of("p", "d", element);
    }

    private BigQueryDefaultStreamWriter<String> writer() {
        return writer(new StringSerializer(), null);
    }

    /** A writer over a given admin — for the one case that needs the production wrap around it. */
    private BigQueryDefaultStreamWriter<String> writer(TableAdmin tableAdmin) {
        return new BigQueryDefaultStreamWriter<>(
                config(new StringSerializer(), null, (element, context) -> DESTINATION),
                factory,
                tableAdmin,
                metrics,
                BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                BigQueryDefaultStreamWriterTest.fastSchedule(3),
                BigQueryDefaultStreamWriterTest.fastSchedule(3));
    }

    private BigQueryDefaultStreamWriter<String> writer(
            BigQueryProtoSerializer<String> serializer, SchemaUpdateOptions schemaUpdateOptions) {
        return writer(serializer, schemaUpdateOptions, (element, context) -> DESTINATION);
    }

    private BigQueryDefaultStreamWriter<String> writer(
            BigQueryProtoSerializer<String> serializer,
            SchemaUpdateOptions schemaUpdateOptions,
            io.github.flink.gcp.connector.bigquery.sink.DestinationResolver<? super String>
                    resolver) {
        return new BigQueryDefaultStreamWriter<>(
                config(serializer, schemaUpdateOptions, resolver),
                factory,
                admin,
                metrics,
                BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                BigQueryDefaultStreamWriterTest.fastSchedule(3),
                BigQueryDefaultStreamWriterTest.fastSchedule(3));
    }

    private BigQuerySinkConfig<String> config(
            BigQueryProtoSerializer<String> serializer,
            SchemaUpdateOptions schemaUpdateOptions,
            io.github.flink.gcp.connector.bigquery.sink.DestinationResolver<? super String>
                    resolver) {
        var builder =
                BigQuerySink.<String>builder()
                        .destinationResolver(resolver)
                        .serializer(serializer)
                        .failureHandler(handler);
        if (schemaUpdateOptions != null) {
            builder.schemaUpdateOptions(schemaUpdateOptions);
        }
        return ((BigQueryDefaultStreamSink<String>) builder.build()).getConfig();
    }

    private static ApiFuture<AppendRowsResponse> failedWith(Status.Code code) {
        return ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.fromCode(code)));
    }

    private static ApiFuture<AppendRowsResponse> rowLevelError(Map<Integer, String> rowErrors) {
        return ApiFutures.immediateFailedFuture(
                new Exceptions.AppendSerializtionError(
                        Status.Code.INVALID_ARGUMENT.value(), "bad rows", "stream", rowErrors));
    }

    private static ApiFuture<AppendRowsResponse> schemaMismatch() {
        return ApiFutures.immediateFailedFuture(
                Exceptions.toStorageException(
                        com.google.rpc.Status.newBuilder()
                                .setCode(Status.Code.INVALID_ARGUMENT.value())
                                .setMessage("synthesized schema mismatch")
                                .addDetails(
                                        Any.pack(
                                                StorageError.newBuilder()
                                                        .setCode(
                                                                StorageError.StorageErrorCode
                                                                        .SCHEMA_MISMATCH_EXTRA_FIELDS)
                                                        .setEntity("t")
                                                        .setErrorMessage("extra fields")
                                                        .build()))
                                .build(),
                        null));
    }

    /** Serializer writing the record's UTF-8 bytes, with a schema the live table does not cover. */
    private static class StringSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SERIALIZER_SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(String element) throws IOException {
            return ByteString.copyFromUtf8(element);
        }
    }

    /** Serializer emitting bytes valid for the Empty descriptor, for CDC augmentation tests. */
    private static final class ValidEmptyProtoSerializer extends StringSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) {
            return Empty.getDefaultInstance().toByteString();
        }
    }

    /** Serializer rejecting every record. */
    private static final class FailingSerializer extends StringSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) throws IOException {
            throw new IOException("cannot serialize " + element);
        }
    }

    /** Serializer recording whether destination resolution allowed serialization to begin. */
    private static final class CountingSerializer extends StringSerializer {
        private static final long serialVersionUID = 1L;

        private int invocations;

        @Override
        public ByteString serialize(String element) throws IOException {
            invocations++;
            return super.serialize(element);
        }
    }

    /** Serializer skipping the record {@code skip-me} and writing every other one. */
    private static final class SkippingSerializer extends StringSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) throws IOException {
            return element.equals("skip-me") ? null : super.serialize(element);
        }
    }

    /** Serializer emitting one oversized row. */
    private static final class OversizedSerializer extends StringSerializer {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFrom(new byte[BigQueryDefaultStreamWriter.MAX_ROW_BYTES + 1]);
        }
    }

    /** Handler recording every routed row and dropping it. */
    private static final class DroppingHandler implements FailureHandler<BigQueryFailure> {
        private static final long serialVersionUID = 1L;

        private final transient List<BigQueryFailure> rows = new ArrayList<>();

        @Override
        public void handle(BigQueryFailure row) {
            rows.add(row);
        }
    }

    /** Admin recording creations and schema updates, answering with a mutable live schema. */
    private static final class RecordingTableAdmin implements TableAdmin {

        private final List<TableDestination> creates = new ArrayList<>();
        private final List<TableSchema> updates = new ArrayList<>();

        /** What each creation attempt throws, one entry per attempt; exhausted means success. */
        private final Deque<IOException> creationFailures = new ArrayDeque<>();

        private IOException cdcFailureAfterCreate;

        private TableSchema liveSchema;

        RecordingTableAdmin(TableSchema liveSchema) {
            this.liveSchema = liveSchema;
        }

        @Override
        public void create(
                TableDestination destination, TableSchema schema, TableCreateOptions options)
                throws IOException {
            creates.add(destination);
            IOException failure = creationFailures.poll();
            if (failure != null) {
                throw failure;
            }
            liveSchema = schema;
        }

        @Override
        public boolean ensureCdcTable(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptionsProvider optionsProvider,
                CdcTableOptions cdcOptions,
                CreateDisposition createDisposition,
                CdcTableReconciliationPolicy reconciliationPolicy)
                throws IOException {
            if (liveSchema != null) {
                return false;
            }
            create(destination, schema, optionsProvider.optionsFor(destination));
            if (cdcFailureAfterCreate != null) {
                throw cdcFailureAfterCreate;
            }
            return true;
        }

        @Override
        public TableSchemaSnapshot getSchema(TableDestination destination) {
            return liveSchema == null ? null : TableSchemaSnapshot.of(liveSchema, null);
        }

        @Override
        public boolean updateSchema(
                TableDestination destination, TableSchemaSnapshot base, TableSchema proposed) {
            updates.add(proposed);
            liveSchema = proposed;
            return true;
        }
    }

    /** Appender factory with a global append-result script (empty script means success). */
    private static final class ScriptedAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final transient List<FakeAppender> created = new ArrayList<>();
        private final transient Deque<ApiFuture<AppendRowsResponse>> scriptedResults =
                new ArrayDeque<>();

        private transient boolean throwOnAppend;

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            FakeAppender appender = new FakeAppender();
            created.add(appender);
            return appender;
        }

        private List<String> allAppendedRows() {
            List<String> rows = new ArrayList<>();
            for (FakeAppender appender : created) {
                for (ProtoRows batch : appender.appends) {
                    batch.getSerializedRowsList().forEach(b -> rows.add(b.toStringUtf8()));
                }
            }
            return rows;
        }

        private final class FakeAppender implements RowAppender {
            private final List<ProtoRows> appends = new ArrayList<>();

            @Override
            public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
                if (throwOnAppend) {
                    throw new IllegalStateException("the client rejected the request");
                }
                appends.add(rows);
                if (scriptedResults.isEmpty()) {
                    return ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance());
                }
                return scriptedResults.poll();
            }

            @Override
            public void close() {}
        }
    }
}
