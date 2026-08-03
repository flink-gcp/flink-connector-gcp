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
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.failure.FailedRow;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the schema evolution behavior of {@link BigQueryDefaultStreamWriter}. */
class BigQueryDefaultStreamWriterSchemaEvolutionTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final TableSchema V1 =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private static final TableSchema V2 =
            V1.toBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("email")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static ApiFuture<AppendRowsResponse> storageErrorFuture(
            StorageError.StorageErrorCode code) {
        return ApiFutures.immediateFailedFuture(
                Exceptions.toStorageException(
                        com.google.rpc.Status.newBuilder()
                                .setCode(Status.Code.INVALID_ARGUMENT.value())
                                .setMessage("synthesized " + code)
                                .addDetails(
                                        Any.pack(
                                                StorageError.newBuilder()
                                                        .setCode(code)
                                                        .setEntity("t")
                                                        .setErrorMessage("synthesized " + code)
                                                        .build()))
                                .build(),
                        null));
    }

    private static ApiFuture<AppendRowsResponse> schemaMismatch() {
        return storageErrorFuture(StorageError.StorageErrorCode.SCHEMA_MISMATCH_EXTRA_FIELDS);
    }

    /** Serializer with a mutable schema; the fingerprint is a bumped version counter. */
    private static class EvolvingSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private TableSchema schema;
        private int version;

        EvolvingSerializer(TableSchema schema) {
            this.schema = schema;
        }

        void evolveTo(TableSchema newSchema) {
            this.schema = newSchema;
            this.version++;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema;
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return version;
        }

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFromUtf8(element);
        }
    }

    /** Admin over a single mutable live schema, with scriptable update outcomes. */
    private static class RecordingTableAdmin implements TableAdmin {
        private TableSchema liveSchema;

        /** Consumed per update call; empty means the update succeeds. */
        private final Deque<Boolean> scriptedUpdateResults = new ArrayDeque<>();

        private final List<TableSchema> updates = new ArrayList<>();
        private final List<TableDestination> creates = new ArrayList<>();
        private int reads;

        RecordingTableAdmin(TableSchema liveSchema) {
            this.liveSchema = liveSchema;
        }

        @Override
        public void create(
                TableDestination destination, TableSchema schema, TableCreateOptions options) {
            creates.add(destination);
            liveSchema = schema;
        }

        @Override
        public TableSchemaSnapshot getSchema(TableDestination destination) {
            reads++;
            return liveSchema == null ? null : TableSchemaSnapshot.of(liveSchema, null);
        }

        @Override
        public boolean updateSchema(
                TableDestination destination, TableSchemaSnapshot base, TableSchema proposed) {
            updates.add(proposed);
            boolean applied = scriptedUpdateResults.isEmpty() || scriptedUpdateResults.poll();
            if (applied) {
                liveSchema = proposed;
            }
            return applied;
        }
    }

    /**
     * Appender factory recording the descriptor every created appender was built with, with a
     * global append-result script (consumed across all appenders in append order; empty script
     * means success).
     */
    private static class ScriptedAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final List<FakeAppender> created = new ArrayList<>();
        private final Deque<ApiFuture<AppendRowsResponse>> scriptedResults = new ArrayDeque<>();

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            FakeAppender appender = new FakeAppender(rowDescriptor);
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

        private class FakeAppender implements RowAppender {
            private final Descriptors.Descriptor descriptor;
            private final List<ProtoRows> appends = new ArrayList<>();
            private boolean closed;

            FakeAppender(Descriptors.Descriptor descriptor) {
                this.descriptor = descriptor;
            }

            @Override
            public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
                appends.add(rows);
                if (scriptedResults.isEmpty()) {
                    return ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance());
                }
                return scriptedResults.poll();
            }

            @Override
            public void close() {
                closed = true;
            }
        }
    }

    private static BigQuerySinkConfig<String> config(
            BigQueryProtoSerializer<? super String> serializer, SchemaUpdateOptions options) {
        return config((element, context) -> DESTINATION, serializer, options);
    }

    private static BigQuerySinkConfig<String> config(
            DestinationResolver<? super String> resolver,
            BigQueryProtoSerializer<? super String> serializer,
            SchemaUpdateOptions options) {
        return ((BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(resolver)
                                .serializer(serializer)
                                .schemaUpdateOptions(options)
                                .build())
                .getConfig();
    }

    private static BigQueryDefaultStreamWriter<String> writer(
            BigQuerySinkConfig<String> config,
            ScriptedAppenderFactory factory,
            RecordingTableAdmin admin) {
        return new BigQueryDefaultStreamWriter<>(
                config,
                factory,
                admin,
                TestSinkWriterMetricGroup.create(),
                BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                BigQueryDefaultStreamWriterTest.fastSchedule(3),
                BigQueryDefaultStreamWriterTest.fastSchedule(3));
    }

    // --- schema-mismatch append failures ---

    @Test
    void schemaMismatchWithUpdatesEnabledReconcilesAndReappends() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);
        writer.flush(false);

        // The live schema was read, unioned with the serializer schema and updated.
        assertThat(admin.reads).isEqualTo(1);
        assertThat(admin.updates).hasSize(1);
        assertThat(admin.updates.get(0).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("name", "email");
        // The batch was re-appended on a rebuilt appender without losing rows.
        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void schemaMismatchWithUpdatesDisabledIsTerminalWithHint() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new EvolvingSerializer(V2), SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("schemaUpdateOptions");
        assertThat(admin.updates).isEmpty();
    }

    @Test
    void lostUpdateRacesAreRetriedFromAFreshRead() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        admin.scriptedUpdateResults.add(false);
        admin.scriptedUpdateResults.add(false);
        admin.scriptedUpdateResults.add(true);
        EvolvingSerializer serializer = new EvolvingSerializer(V2);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(admin.reads).isEqualTo(3);
        assertThat(admin.updates).hasSize(3);
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void exhaustedUpdateRacesAreTerminal() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        for (int i = 0; i < BigQueryDefaultStreamWriter.SCHEMA_UPDATE_MAX_ATTEMPTS; i++) {
            admin.scriptedUpdateResults.add(false);
        }
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(
                                new EvolvingSerializer(V2),
                                SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("lost a concurrent-update race");
    }

    @Test
    void alreadyCoveringSchemaIsNotUpdated() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        // The live table already covers the serializer schema: nothing to update, the mismatch
        // resolves through re-appends alone (the backend cache catches up).
        RecordingTableAdmin admin = new RecordingTableAdmin(V2);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(
                                new EvolvingSerializer(V2),
                                SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(admin.reads).isEqualTo(1);
        assertThat(admin.updates).isEmpty();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void mismatchPersistingPastTheRetryBudgetIsTerminal() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch()); // initial append
        factory.scriptedResults.add(schemaMismatch()); // re-append attempt 1
        factory.scriptedResults.add(schemaMismatch()); // re-append attempt 2
        factory.scriptedResults.add(schemaMismatch()); // re-append attempt 3
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(
                                new EvolvingSerializer(V2),
                                SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("after reconciling the table schema")
                .hasMessageContaining("retry budget is exhausted");
        assertThat(admin.updates).hasSize(1);
    }

    @Test
    void missingTableDuringReconciliationIsCreatedInstead() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        RecordingTableAdmin admin = new RecordingTableAdmin(null);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(
                                new EvolvingSerializer(V2),
                                SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(admin.creates).containsExactly(DESTINATION);
        assertThat(admin.updates).isEmpty();
    }

    // --- server-pushed updated_schema ---

    @Test
    void updatedSchemaOnResponseRebuildsOnlyThatDestination() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder().setUpdatedSchema(V2).build()));
        RecordingTableAdmin admin = new RecordingTableAdmin(V2);
        TableDestination other = TableDestination.of("p", "d", "other");
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(
                                (element, context) ->
                                        element.startsWith("other") ? other : DESTINATION,
                                serializer,
                                SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);
        writer.write("other-a", CONTEXT);
        writer.flush(false); // the response of [aa] carries updated_schema
        serializer.evolveTo(V2);
        writer.write("bb", CONTEXT); // drains the refresh: rebuilds DESTINATION only
        writer.flush(false);

        // Three appenders: two initial ones plus the rebuilt DESTINATION appender.
        assertThat(factory.created).hasSize(3);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.created.get(1).closed).isFalse();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "other-a", "bb");
    }

    @Test
    void updatedSchemaWithUnchangedSerializerSchemaDoesNotRebuild() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder().setUpdatedSchema(V2).build()));
        RecordingTableAdmin admin = new RecordingTableAdmin(V2);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new EvolvingSerializer(V1), SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);
        writer.flush(false); // the response carries updated_schema
        writer.write("bb", CONTEXT);
        writer.flush(false);

        // The rebuilt writer would install the identical serializer schema: no rebuild.
        assertThat(factory.created).hasSize(1);
        assertThat(factory.created.get(0).closed).isFalse();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "bb");
    }

    @Test
    void updatedSchemaIsDrainedOnFlushAfterSerializerEvolution() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder().setUpdatedSchema(V2).build()));
        RecordingTableAdmin admin = new RecordingTableAdmin(V2);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(serializer, SchemaUpdateOptions.defaults()), factory, admin);

        writer.write("aa", CONTEXT);
        writer.flush(false);
        serializer.evolveTo(V2);
        writer.flush(false); // drains the refresh without any intervening write

        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.allAppendedRows()).containsExactly("aa");
    }

    // --- serializer fingerprint changes ---

    @Test
    void fingerprintChangeRebuildsBeforeAppendingWithoutAFailedAppend() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingTableAdmin admin = new RecordingTableAdmin(V2);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(serializer, SchemaUpdateOptions.defaults()), factory, admin);

        writer.write("aa", CONTEXT); // buffered under v1
        serializer.evolveTo(V2);
        writer.write("bb", CONTEXT); // pre-check rebuilds, carries [aa] over
        writer.flush(false);

        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.created.get(0).appends).isEmpty();
        assertThat(factory.created.get(1).appends).hasSize(1);
        assertThat(factory.allAppendedRows()).containsExactly("aa", "bb");
        // The rebuilt appender was built with the evolved two-field descriptor.
        assertThat(factory.created.get(0).descriptor.getFields()).hasSize(1);
        assertThat(factory.created.get(1).descriptor.getFields()).hasSize(2);
    }

    @Test
    void fingerprintChangeWithUpdatesEnabledReconcilesProactively() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(serializer, SchemaUpdateOptions.builder().allowNewFields().build()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);
        serializer.evolveTo(V2);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        // The table was updated before any append could fail on the new schema.
        assertThat(admin.updates).hasSize(1);
        assertThat(admin.updates.get(0).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("name", "email");
        assertThat(factory.allAppendedRows()).containsExactly("aa", "bb");
    }

    @Test
    void unchangedFingerprintDoesNotRebuild() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new EvolvingSerializer(V1), SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(factory.created).hasSize(1);
        assertThat(admin.reads).isZero();
    }

    @Test
    void missingTableDuringReconciliationFailsUnderCreateNever() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(schemaMismatch());
        RecordingTableAdmin admin = new RecordingTableAdmin(null);
        BigQuerySinkConfig<String> config =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .destination(DESTINATION)
                                        .serializer(new EvolvingSerializer(V2))
                                        .createDisposition(CreateDisposition.CREATE_NEVER)
                                        .schemaUpdateOptions(
                                                SchemaUpdateOptions.builder()
                                                        .allowNewFields()
                                                        .build())
                                        .build())
                        .getConfig();
        BigQueryDefaultStreamWriter<String> writer = writer(config, factory, admin);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(admin.creates).isEmpty();
    }

    // --- response-embedded storage errors ---

    @Test
    void invalidStreamStateResponseErrorIsRepairedByRebuilding() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        // The SDK has no typed exception for INVALID_STREAM_STATE; the writer must still detect
        // the packed storage error in the response and repair by rebuilding.
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder()
                                .setError(
                                        com.google.rpc.Status.newBuilder()
                                                .setCode(Status.Code.FAILED_PRECONDITION.value())
                                                .setMessage("invalid stream state")
                                                .addDetails(
                                                        Any.pack(
                                                                StorageError.newBuilder()
                                                                        .setCode(
                                                                                StorageError
                                                                                        .StorageErrorCode
                                                                                        .INVALID_STREAM_STATE)
                                                                        .build())))
                                .build()));
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new EvolvingSerializer(V1), SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(factory.created).hasSize(2);
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void mismatchResponseWithRowErrorsAndUpdatesDisabledRoutesRows() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder()
                                .setError(
                                        com.google.rpc.Status.newBuilder()
                                                .setCode(Status.Code.INVALID_ARGUMENT.value())
                                                .setMessage("schema mismatch")
                                                .addDetails(
                                                        Any.pack(
                                                                StorageError.newBuilder()
                                                                        .setCode(
                                                                                StorageError
                                                                                        .StorageErrorCode
                                                                                        .SCHEMA_MISMATCH_EXTRA_FIELDS)
                                                                        .build())))
                                .addRowErrors(
                                        RowError.newBuilder().setIndex(0).setMessage("extra field"))
                                .build()));
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        List<FailedRow> routed = new ArrayList<>();
        BigQuerySinkConfig<String> config =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .destination(DESTINATION)
                                        .serializer(new EvolvingSerializer(V2))
                                        .failedRowHandler((FailureHandler<FailedRow>) routed::add)
                                        .build())
                        .getConfig();
        BigQueryDefaultStreamWriter<String> writer = writer(config, factory, admin);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);

        // With schema updates disabled, the row-error details keep the configured per-row policy
        // in charge instead of failing the whole job on the mismatch.
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(routed).hasSize(1);
        assertThat(routed.get(0).getRowBytes().toStringUtf8()).isEqualTo("aa");
        assertThat(factory.allAppendedRows()).containsExactly("aa", "bb", "bb");
    }

    // --- stale stream writers ---

    @Test
    void staleStreamWriterFailuresAreRepairedByRebuilding() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(
                storageErrorFuture(StorageError.StorageErrorCode.STREAM_FINALIZED));
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new EvolvingSerializer(V1), SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void callbackWaitTimeoutIsRepairedByRebuilding() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        // The SDK's connection watchdog killed the connection: the first in-flight append fails
        // with the raw callback-wait timeout exception, which carries no gRPC status. The writer
        // must treat it as a client-side dead writer and rebuild, not fail the job.
        factory.scriptedResults.add(
                ApiFutures.immediateFailedFuture(
                        new Exceptions.MaximumRequestCallbackWaitTimeExceededException(
                                Duration.ofMinutes(6), "writer-id", Duration.ofMinutes(5))));
        RecordingTableAdmin admin = new RecordingTableAdmin(V1);
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(new EvolvingSerializer(V1), SchemaUpdateOptions.defaults()),
                        factory,
                        admin);

        writer.write("aa", CONTEXT);

        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }
}
