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

import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Any;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.FakeTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.CONTEXT;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.RecordingHandler;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.onlyCommittable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for schema evolution on application-created buffered streams. */
class BigQueryBufferedStreamWriterSchemaEvolutionTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");
    private static final TableDestination OTHER = TableDestination.of("p", "d", "other");

    private static final TableSchema V1 =
            TableSchema.newBuilder().addFields(nullableString("name")).build();
    private static final TableSchema V2 = V1.toBuilder().addFields(nullableString("note")).build();

    private static TableFieldSchema nullableString(String name) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRING)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    private static Throwable schemaMismatch() {
        return Exceptions.toStorageException(
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.INVALID_ARGUMENT.value())
                        .setMessage("synthesized schema mismatch")
                        .addDetails(
                                Any.pack(
                                        StorageError.newBuilder()
                                                .setCode(
                                                        StorageError.StorageErrorCode
                                                                .SCHEMA_MISMATCH_EXTRA_FIELDS)
                                                .setEntity("stream")
                                                .setErrorMessage("synthesized schema mismatch")
                                                .build()))
                        .build(),
                null);
    }

    private static BigQuerySinkConfig<String> config(
            EvolvingSerializer serializer, SchemaUpdateOptions schemaUpdateOptions) {
        return config(serializer, schemaUpdateOptions, (element, context) -> DESTINATION);
    }

    private static BigQuerySinkConfig<String> config(
            EvolvingSerializer serializer,
            SchemaUpdateOptions schemaUpdateOptions,
            DestinationResolver<String> destinationResolver) {
        return config(serializer, schemaUpdateOptions, destinationResolver, null);
    }

    private static BigQuerySinkConfig<String> config(
            EvolvingSerializer serializer,
            SchemaUpdateOptions schemaUpdateOptions,
            DestinationResolver<String> destinationResolver,
            @Nullable FailureHandler<BigQueryFailure> failureHandler) {
        var builder =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destinationResolver(destinationResolver)
                        .serializer(serializer)
                        .schemaUpdateOptions(schemaUpdateOptions)
                        .bufferedStreamOptions(options());
        if (failureHandler != null) {
            builder.failureHandler(failureHandler);
        }
        return ((BigQueryBufferedStreamSink<String>) builder.build()).getConfig();
    }

    private static BufferedStreamOptions options() {
        return BufferedStreamOptions.builder()
                .recoveryInitialBackoff(Duration.ofMillis(1))
                .recoveryMaxBackoff(Duration.ofMillis(1))
                .recoveryMaxAttempts(3)
                .build();
    }

    private static BigQueryBufferedStreamWriter<String> writer(
            BigQuerySinkConfig<String> config,
            FakeBufferedStreamService service,
            FakeTableAdmin tableAdmin,
            TestSinkWriterMetricGroup metrics,
            BufferedStreamWriterState... restoredStates) {
        return writer(
                config,
                service,
                tableAdmin,
                metrics,
                new RetrySchedule(1, 1, 3, 0),
                restoredStates);
    }

    private static BigQueryBufferedStreamWriter<String> writer(
            BigQuerySinkConfig<String> config,
            FakeBufferedStreamService service,
            FakeTableAdmin tableAdmin,
            TestSinkWriterMetricGroup metrics,
            RetrySchedule schemaWaitSchedule,
            BufferedStreamWriterState... restoredStates) {
        return new BigQueryBufferedStreamWriter<>(
                config,
                options(),
                service.asFactory(),
                tableAdmin,
                metrics,
                0,
                List.of(restoredStates),
                schemaWaitSchedule,
                System::nanoTime);
    }

    @Test
    void fingerprintChangeDrainsOldRowsAndReopensTheSameStream() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V2);
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.defaults()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create());

        writer.write("alice", CONTEXT);
        serializer.evolveTo(V2);
        writer.write("bob:hello", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).hasSize(1);
        String stream = service.createdStreams.get(0);
        assertThat(service.openedAppenders).containsExactly(stream, stream);
        assertThat(service.closedAppenders).containsExactly(stream);
        assertThat(service.openedDescriptors)
                .extracting(descriptor -> descriptor.getFields().size())
                .containsExactly(1, 2);
        assertThat(service.appends).extracting(call -> call.offset).containsExactly(0L, 1L);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(1);
        assertThat(writer.snapshotState(7))
                .containsExactly(new BufferedStreamWriterState(DESTINATION, stream, 2, 7));
    }

    @Test
    void enabledEvolutionUpdatesTheTableAndCountsTheReconciliation() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        service,
                        admin,
                        metrics);

        writer.write("alice", CONTEXT);
        writer.flush(false);
        serializer.evolveTo(V2);
        writer.write("bob:hello", CONTEXT);
        writer.flush(false);

        assertThat(admin.schemaUpdates).containsExactly(DESTINATION);
        assertThat(admin.tables.get(DESTINATION)).isEqualTo(V2);
        assertThat(metrics.counterValue("schemaReconciliations")).isEqualTo(1);
        assertThat(service.createdStreams).hasSize(1);
        assertThat(service.openedAppenders).hasSize(2).containsOnly(service.createdStreams.get(0));
    }

    @Test
    void reactiveMismatchReconcilesAndReappendsAtTheSameOffset() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(FakeBufferedStreamService.failure(schemaMismatch()));
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create());

        writer.write("alice:hello", CONTEXT);
        writer.flush(false);

        assertThat(admin.schemaUpdates).containsExactly(DESTINATION);
        assertThat(service.createdStreams).hasSize(1);
        String stream = service.createdStreams.get(0);
        assertThat(service.openedAppenders).containsExactly(stream, stream);
        assertThat(service.appends).extracting(call -> call.offset).containsExactly(0L, 0L);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isZero();
    }

    @Test
    void schemaWaitBudgetStartsWhenAMismatchFollowsATransientFailure() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(new StatusRuntimeException(Status.UNAVAILABLE)));
        service.appendResults.add(FakeBufferedStreamService.failure(schemaMismatch()));
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create(),
                        new RetrySchedule(1, 1, 1, 0));

        writer.write("alice:hello", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).extracting(call -> call.offset).containsExactly(0L, 0L, 0L);
        assertThat(admin.schemaUpdates).containsExactly(DESTINATION);
    }

    @Test
    void restoredStreamIsReconciledWithoutReplacement() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(FakeBufferedStreamService.failure(schemaMismatch()));
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        String restoredStream = DESTINATION.toTablePath() + "/streams/restored";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create(),
                        new BufferedStreamWriterState(DESTINATION, restoredStream, 5, 10));

        writer.write("alice:hello", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).isEmpty();
        assertThat(service.openedAppenders).containsExactly(restoredStream, restoredStream);
        assertThat(service.appends).extracting(call -> call.offset).containsExactly(5L, 5L);
        BufferedStreamCommittable committable = onlyCommittable(writer.prepareCommit());
        assertThat(committable.getStreamName()).isEqualTo(restoredStream);
        assertThat(committable.getFlushOffset()).isEqualTo(5);
    }

    @Test
    void restoredProbeGetsAFreshSchemaWaitBudget() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(new StatusRuntimeException(Status.UNAVAILABLE)));
        service.appendResults.add(FakeBufferedStreamService.failure(schemaMismatch()));
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        String restoredStream = DESTINATION.toTablePath() + "/streams/restored";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create(),
                        new RetrySchedule(1, 1, 1, 0),
                        new BufferedStreamWriterState(DESTINATION, restoredStream, 5, 10));

        writer.write("alice:hello", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).extracting(call -> call.offset).containsExactly(5L, 5L, 5L);
        assertThat(admin.schemaUpdates).containsExactly(DESTINATION);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(5);
    }

    @Test
    void proactiveChangeBeforeTheFirstRestoredAppendOpensTheService() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V2);
        String restoredStream = DESTINATION.toTablePath() + "/streams/restored";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.defaults()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create(),
                        new BufferedStreamWriterState(DESTINATION, restoredStream, 5, 10));

        serializer.evolveTo(V2);
        writer.write("alice:hello", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).isEmpty();
        assertThat(service.openedAppenders).containsExactly(restoredStream);
        assertThat(service.appends).extracting(call -> call.offset).containsExactly(5L);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(5);
    }

    @Test
    void oneDynamicDestinationRefreshDoesNotReopenAnother() throws Exception {
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V2);
        admin.tables.put(OTHER, V2);
        DestinationResolver<String> resolver =
                (element, context) -> element.startsWith("other") ? OTHER : DESTINATION;
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.defaults(), resolver),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create());

        writer.write("first", CONTEXT);
        writer.write("other-first", CONTEXT);
        writer.flush(false);
        String firstStream = service.createdStreams.get(0);
        String otherStream = service.createdStreams.get(1);

        serializer.evolveTo(V2);
        writer.write("first:note", CONTEXT);
        writer.flush(false);

        assertThat(service.openedAppenders).containsExactly(firstStream, otherStream, firstStream);
        assertThat(service.closedAppenders).containsExactly(firstStream);
    }

    @Test
    void aMismatchDuringARowLevelReplayReconcilesTheTableToo() throws Exception {
        // The replay path carries its own copy of the reconciliation, reached through a row-level
        // rejection here and through an abandoned restored stream elsewhere. An append rejected
        // atomically leaves the batches behind it to be re-appended at recomputed offsets, and the
        // schema may be what they then trip on.
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new Exceptions.AppendSerializtionError(
                                Status.Code.INVALID_ARGUMENT.value(),
                                "bad rows",
                                "stream",
                                Map.of(1, "bad row"))));
        service.appendResults.add(FakeBufferedStreamService.failure(schemaMismatch()));
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(
                                serializer,
                                SchemaUpdateOptions.builder().allowNewFields().build(),
                                (element, context) -> DESTINATION,
                                handler),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create());

        writer.write("alice:hello", CONTEXT);
        writer.write("bob:hello", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(admin.schemaUpdates).containsExactly(DESTINATION);
        assertThat(admin.tables.get(DESTINATION)).isEqualTo(V2);
        // Reconciling the table is only half of it: the appender has to be reopened, or the
        // replayed rows keep serializing against the descriptor that just failed and the retry
        // budget drains against a mismatch nothing else will clear.
        String stream = service.createdStreams.get(0);
        assertThat(service.openedAppenders).containsExactly(stream, stream);
        // Original append at 0, the survivor's replay at 0 (rejected for the schema), and the
        // reconciled retry at 0 again.
        assertThat(service.appends).extracting(call -> call.offset).containsExactly(0L, 0L, 0L);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isZero();
    }

    @Test
    void mismatchWithoutSchemaUpdatesRemainsTerminal() {
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(FakeBufferedStreamService.failure(schemaMismatch()));
        FakeTableAdmin admin = new FakeTableAdmin();
        admin.tables.put(DESTINATION, V1);
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.defaults()),
                        service,
                        admin,
                        TestSinkWriterMetricGroup.create());

        assertThatThrownBy(
                        () -> {
                            writer.write("alice:hello", CONTEXT);
                            writer.flush(false);
                        })
                .hasMessageContaining("enable schemaUpdateOptions");
        assertThat(admin.schemaUpdates).isEmpty();
    }
}
