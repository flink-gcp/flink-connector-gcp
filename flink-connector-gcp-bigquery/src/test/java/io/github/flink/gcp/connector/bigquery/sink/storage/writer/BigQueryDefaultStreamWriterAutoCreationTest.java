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
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the table auto-creation and create-disposition behavior of {@link
 * BigQueryDefaultStreamWriter}.
 */
class BigQueryDefaultStreamWriterAutoCreationTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static ApiFuture<AppendRowsResponse> notFound() {
        return ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.NOT_FOUND));
    }

    /**
     * What the <em>real</em> service answers when a stream is opened against a table that is not
     * there: it masks existence behind {@code PERMISSION_DENIED} rather than saying {@code
     * NOT_FOUND}, which only the emulator does. Measured 2026-08-06; see {@code
     * AppendErrorClassifier#isMissingTable}.
     */
    private static ApiFuture<AppendRowsResponse> maskedAsPermissionDenied() {
        return ApiFutures.immediateFailedFuture(
                new StatusRuntimeException(
                        Status.PERMISSION_DENIED.withDescription(
                                "Permission 'TABLES_GET' denied on resource"
                                        + " 'projects/p/datasets/d/tables/t' (or it may not"
                                        + " exist).")));
    }

    /** Serializer writing the record string bytes with a fixed single-column schema. */
    private static class StringSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(String element) {
            return ByteString.copyFromUtf8(element);
        }
    }

    private static class RecordingTableAdmin
            extends BigQueryDefaultStreamWriterTest.NoopTableAdmin {
        private final List<TableDestination> destinations = new ArrayList<>();
        private final List<TableSchema> schemas = new ArrayList<>();
        private final List<TableCreateOptions> options = new ArrayList<>();

        @Override
        public void create(
                TableDestination destination, TableSchema schema, TableCreateOptions opts) {
            destinations.add(destination);
            schemas.add(schema);
            options.add(opts);
        }
    }

    /**
     * Appender factory with a global append-result script (consumed across all appenders in append
     * order; empty script means success) and optional appender-creation failures.
     */
    private static class ScriptedAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        private final List<FakeAppender> created = new ArrayList<>();
        private final Deque<ApiFuture<AppendRowsResponse>> scriptedResults = new ArrayDeque<>();
        private int failingCreations;

        @Override
        public RowAppender create(
                TableDestination destination,
                Descriptors.Descriptor rowDescriptor,
                String location) {
            if (failingCreations > 0) {
                failingCreations--;
                throw new StatusRuntimeException(Status.NOT_FOUND);
            }
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

        private class FakeAppender implements RowAppender {
            private final List<ProtoRows> appends = new ArrayList<>();
            private boolean closed;

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
            CreateDisposition disposition, TableCreateOptionsProvider optionsProvider) {
        BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .destination(DESTINATION)
                        .serializer(new StringSerializer())
                        .createDisposition(disposition);
        if (optionsProvider != null) {
            builder.tableCreateOptionsProvider(optionsProvider);
        }
        return ((BigQueryDefaultStreamSink<String>) builder.build()).getConfig();
    }

    private static BigQueryDefaultStreamWriter<String> writer(
            BigQuerySinkConfig<String> config,
            ScriptedAppenderFactory factory,
            TableAdmin creator,
            long maxAppendRequestBytes,
            int recoveryMaxAttempts) {
        return new BigQueryDefaultStreamWriter<>(
                config,
                factory,
                creator,
                TestSinkWriterMetricGroup.create(),
                maxAppendRequestBytes,
                BigQueryDefaultStreamWriterTest.fastSchedule(recoveryMaxAttempts),
                BigQueryDefaultStreamWriterTest.fastSchedule(recoveryMaxAttempts));
    }

    @Test
    void flushCreatesMissingTableRebuildsWriterAndRetriesAppend() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY)
                        .build();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_IF_NEEDED, destination -> options),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(creator.destinations).containsExactly(DESTINATION);
        assertThat(creator.schemas.get(0).getFields(0).getName()).isEqualTo("f");
        assertThat(creator.options).containsExactly(options);
        // The failed appender was replaced by a fresh one that got the re-append.
        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.created.get(1).appends).hasSize(1);
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void theMaskedPermissionDeniedTheServiceActuallyAnswersAlsoCreatesTheTable() throws Exception {
        // Without this the feature is emulator-only: real BigQuery never answers NOT_FOUND when a
        // write stream is opened against a missing table, so auto-creation would never fire.
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(maskedAsPermissionDenied());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_IF_NEEDED, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(creator.destinations).containsExactly(DESTINATION);
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa");
    }

    @Test
    void theSameMaskingRightAfterCreationIsWaitedOutRatherThanFailing() throws Exception {
        // Measured against the service: the propagation window after this writer creates the table
        // masks the same way, naming TABLES_UPDATE_DATA rather than TABLES_GET. Narrowing the
        // post-creation retry clause to NOT_FOUND therefore creates the table and then fails on the
        // very next append — which is what a real run did before this case existed.
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(maskedAsPermissionDenied()); // the table is not there
        factory.scriptedResults.add(maskedAsPermissionDenied()); // ... and has not propagated yet
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_IF_NEEDED, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);

        // Created once — the guard holds across the retry — and the row landed on the third append.
        assertThat(creator.destinations).containsExactly(DESTINATION);
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa", "aa");
    }

    @Test
    void aMaskedPermissionDeniedUnderCreateNeverStillFails() throws Exception {
        // The disposition is what authorises creation; the wider verdict must not slip past it.
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(maskedAsPermissionDenied());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_NEVER, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(creator.destinations).isEmpty();
    }

    @Test
    void createNeverFailsFastOnMissingTable() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_NEVER, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(creator.destinations).isEmpty();
        assertThat(factory.created).hasSize(1);
    }

    @Test
    void asyncNotFoundIsRecoveredOnNextWriteAndBufferedRowsSurvive() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(CreateDisposition.CREATE_IF_NEEDED, null), factory, creator, 1, 3);

        writer.write("aa", CONTEXT); // buffered
        writer.write("bb", CONTEXT); // appends [aa], which fails asynchronously with NOT_FOUND
        writer.write("cc", CONTEXT); // sweeps the failure: creates the table, re-appends [aa]
        writer.flush(false);

        assertThat(creator.destinations).containsExactly(DESTINATION);
        assertThat(factory.created.get(0).closed).isTrue();
        // [aa] failed once, then all rows (including [bb] buffered at rebuild time) landed.
        assertThat(factory.allAppendedRows()).containsExactly("aa", "aa", "bb", "cc");
    }

    @Test
    void appenderCreationNotFoundTriggersTableCreation() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.failingCreations = 1;
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_IF_NEEDED, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(creator.destinations).containsExactly(DESTINATION);
        assertThat(factory.created).hasSize(1);
        assertThat(factory.allAppendedRows()).containsExactly("aa");
    }

    @Test
    void appenderCreationNotFoundFailsFastUnderCreateNever() {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.failingCreations = 1;
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_NEVER, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        assertThatThrownBy(() -> writer.write("aa", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(creator.destinations).isEmpty();
    }

    @Test
    void recoveryGivesUpAfterMaxAttempts() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound()); // initial append
        factory.scriptedResults.add(notFound()); // recovery attempt 1
        factory.scriptedResults.add(notFound()); // recovery attempt 2
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_IF_NEEDED, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        2);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2 attempt(s)");
        assertThat(creator.destinations).containsExactly(DESTINATION);
    }

    @Test
    void nonNotFoundFailureDuringRecoveryIsFatal() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound());
        factory.scriptedResults.add(ApiFutures.immediateFailedFuture(new RuntimeException("boom")));
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(
                        config(CreateDisposition.CREATE_IF_NEEDED, null),
                        factory,
                        creator,
                        BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                        3);

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("boom");
    }

    @Test
    void flushRecoversAllFailedBatchesOfADestinationWithOneRebuild() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound());
        factory.scriptedResults.add(notFound());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(CreateDisposition.CREATE_IF_NEEDED, null), factory, creator, 1, 3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT); // appends [aa], which fails with NOT_FOUND
        writer.flush(false); // appends [bb] (fails too); recovery re-appends both batches

        assertThat(creator.destinations).containsExactly(DESTINATION);
        // One original appender plus exactly one rebuilt appender shared by both batches.
        assertThat(factory.created).hasSize(2);
        assertThat(factory.created.get(0).closed).isTrue();
        assertThat(factory.created.get(1).appends).hasSize(2);
        assertThat(factory.allAppendedRows()).containsExactlyInAnyOrder("aa", "bb", "aa", "bb");
    }

    @Test
    void tableIsNotRecreatedOnSubsequentWrites() throws Exception {
        ScriptedAppenderFactory factory = new ScriptedAppenderFactory();
        factory.scriptedResults.add(notFound());
        RecordingTableAdmin creator = new RecordingTableAdmin();
        BigQueryDefaultStreamWriter<String> writer =
                writer(config(CreateDisposition.CREATE_IF_NEEDED, null), factory, creator, 1, 3);

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT); // appends [aa] -> NOT_FOUND -> recovered on next write
        writer.write("cc", CONTEXT);
        writer.flush(false);
        writer.write("dd", CONTEXT);
        writer.flush(false);

        assertThat(creator.destinations).containsExactly(DESTINATION);
        assertThat(factory.allAppendedRows()).contains("aa", "bb", "cc", "dd");
    }
}
