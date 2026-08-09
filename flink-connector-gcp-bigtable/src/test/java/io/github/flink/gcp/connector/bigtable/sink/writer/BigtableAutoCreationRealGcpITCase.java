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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What real Cloud Bigtable answers the auto-creation repair with. The emulator can drive the
 * missing-<em>table</em> leg but not the missing-<em>family</em> one (it answers {@code INTERNAL}
 * where the service says {@code NOT_FOUND}), and neither the service's missing-table status nor the
 * repair's end-to-end behaviour against real metadata propagation is evidence an emulator can give
 * — so this class owns three verdicts: the missing-table status the classifier keys on, the
 * write-path repair of a family missing from an existing table, and that an existing family's
 * garbage-collection rule survives a repair untouched.
 *
 * <p>Each {@code finally} closes the writer plainly, which is an assertion rather than cleanup
 * (#238): every case here provokes rejections the batcher accumulates, and a close re-reporting
 * them fails the test.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableAutoCreationRealGcpITCase extends AbstractBigtableRealGcpITCase {

    @Test
    void createIfNeededCreatesTheMissingTableAndWritesEndToEnd() throws Exception {
        TableDestination table = tableDestination("auto-created");
        SinkWriter<String> writer =
                writer(
                        table,
                        CreateDisposition.CREATE_IF_NEEDED,
                        TableCreateOptions.builder()
                                .columnFamily(FAMILY, GcRule.maxVersions(1))
                                .build(),
                        new RecordingHandler(),
                        toFamily(FAMILY));

        try {
            writer.write("row-1", TestContexts.NO_OP);
            writer.write("row-2", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly("row-1", "row-2");
            assertThat(
                            describeTable("auto-created").getColumnFamilies().stream()
                                    .filter(family -> family.getId().equals(FAMILY))
                                    .findFirst()
                                    .orElseThrow()
                                    .getGCRule()
                                    .toProto())
                    .isEqualTo(GCRules.GCRULES.maxVersions(1).toProto());
        } finally {
            writer.close();
        }
    }

    @Test
    void createNeverFailsFastOnAMissingTableWithTheDispositionHint() throws Exception {
        // This is the assertion that pins the service's missing-table status as NOT_FOUND — the
        // status the classifier's repairable class keys on. BigQuery masks a missing table as
        // PERMISSION_DENIED, so the code alone is not something to assume across services.
        TableDestination table = tableDestination("never-created");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(table, CreateDisposition.CREATE_NEVER, null, handler, toFamily(FAMILY));

        try {
            writer.write("row-1", TestContexts.NO_OP);

            assertThatThrownBy(() -> writer.flush(false))
                    .hasMessageContaining("createDisposition is CREATE_NEVER")
                    .hasStackTraceContaining("NOT_FOUND");
            assertThat(handler.handled).isEmpty();
        } finally {
            writer.close();
        }
    }

    @Test
    void aDeclaredFamilyMissingFromAnExistingTableIsRepairedThroughTheWritePath() throws Exception {
        // The leg the emulator cannot carry: a missing family answers NOT_FOUND ("Requested
        // column family not found") on the write path, and the repair must add only the missing
        // family — the existing one keeps the rule it was created with, here the created default
        // of no rule, although the options declare a different one for it on purpose.
        TableDestination table = createTable("auto-amended");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        table,
                        CreateDisposition.CREATE_IF_NEEDED,
                        TableCreateOptions.builder()
                                .columnFamily(FAMILY, GcRule.maxVersions(9))
                                .columnFamily("added", GcRule.maxAge(Duration.ofHours(24)))
                                .build(),
                        handler,
                        toFamily("added"));

        try {
            writer.write("row-1", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly("row-1");
            assertThat(describeTable("auto-amended").getColumnFamilies())
                    .extracting(ColumnFamily::getId)
                    .containsExactlyInAnyOrder(FAMILY, "added");
            // Creation-only per family: the live rule wins, the declared maxVersions(9) is
            // neither compared nor applied.
            assertThat(
                            describeTable("auto-amended").getColumnFamilies().stream()
                                    .filter(family -> family.getId().equals(FAMILY))
                                    .findFirst()
                                    .orElseThrow()
                                    .getGCRule()
                                    .toProto())
                    .isEqualTo(com.google.bigtable.admin.v2.GcRule.getDefaultInstance());
            assertThat(handler.handled).isEmpty();
        } finally {
            writer.close();
        }
    }

    /** A serializer writing every record's payload into the given family. */
    private static BigtableSerializationSchema<String> toFamily(String family) {
        return (element, context) ->
                RowMutationEntry.create(element).setCell(family, "payload", 1_000L, "v");
    }

    /** Builds a writer over the production batcher factory and admin, against the real service. */
    @SuppressWarnings("unchecked")
    private static SinkWriter<String> writer(
            TableDestination table,
            CreateDisposition disposition,
            TableCreateOptions createOptions,
            FailureHandler<FailedElement> handler,
            BigtableSerializationSchema<String> serializer)
            throws Exception {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();
        BigtableSinkBuilder<String> builder =
                BigtableSink.<String>builder()
                        .table(table)
                        .serializer(serializer)
                        .writerOptions(options)
                        .failedMutationHandler(handler)
                        .createDisposition(disposition);
        if (createOptions != null) {
            builder.tableCreateOptions(createOptions);
        }
        Sink<String> sink = builder.build();
        MutationBatcherFactory factory = new DefaultMutationBatcherFactory(null, options, null);
        return ((BigtableMutateRowsSink<String>) sink)
                .createWriter(
                        factory,
                        new BigtableTableAdmin(),
                        new FakeMailboxExecutor(),
                        TestSinkWriterMetricGroup.create());
    }

    /** A handler that drops every mutation, recording what it saw. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedElement> handled = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            handled.add(element);
        }
    }
}
