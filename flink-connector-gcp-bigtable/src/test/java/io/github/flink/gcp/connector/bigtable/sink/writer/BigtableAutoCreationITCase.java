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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end auto-creation against the emulator: the sink meets a table that does not exist,
 * creates it through the production {@code BigtableTableAdmin}, re-applies the failed batch, and
 * the rows land. The emulator can carry this because it answers a missing table with the same
 * request-level {@code NOT_FOUND} fan-out the repair is built on (measured 2026-08-08; the
 * deviation suite pins it) — unlike the missing-<em>family</em> case, whose deviating {@code
 * INTERNAL} leaves that leg of the repair to the gated real-GCP suite.
 */
class BigtableAutoCreationITCase extends AbstractBigtableEmulatorITCase {

    @Test
    void createIfNeededCreatesTheMissingTableAndWritesEndToEnd() throws Exception {
        TableDestination table = TableDestination.of(PROJECT, INSTANCE, "auto-created");
        TableCreateOptions createOptions =
                TableCreateOptions.builder()
                        .columnFamily(FAMILY, GcRule.maxVersions(1))
                        .columnFamily("plain")
                        .build();
        SinkWriter<String> writer = writer(table, createOptions);

        try {
            writer.write("row-1", TestContexts.NO_OP);
            writer.write("row-2", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly("row-1", "row-2");
            assertThat(describeTable("auto-created").getColumnFamilies())
                    .extracting(ColumnFamily::getId)
                    .containsExactlyInAnyOrder(FAMILY, "plain");
            assertThat(
                            describeTable("auto-created").getColumnFamilies().stream()
                                    .filter(family -> family.getId().equals(FAMILY))
                                    .findFirst()
                                    .orElseThrow()
                                    .getGCRule()
                                    .toProto())
                    .isEqualTo(GCRules.GCRULES.maxVersions(1).toProto());
        } finally {
            // A plain close is an assertion (#238): the incident left NOT_FOUND failures in the
            // batcher's accumulated stats, and a close re-reporting them would throw here.
            writer.close();
        }
    }

    @Test
    void createIfNeededCreatesEveryTableOneIncidentLeftMissing() throws Exception {
        // One repair covers every table parked at the time, and ensures each of them — the case a
        // single "have I created yet" flag would get wrong by creating the first and spending the
        // budget on the second (#232). One creation schema serves both, which is the other half
        // this states: a resolver names tables, not schemas.
        TableDestination even = TableDestination.of(PROJECT, INSTANCE, "auto-created-even");
        TableDestination odd = TableDestination.of(PROJECT, INSTANCE, "auto-created-odd");
        TableCreateOptions createOptions =
                TableCreateOptions.builder().columnFamily(FAMILY, GcRule.maxVersions(1)).build();
        SinkWriter<String> writer =
                writer(
                        (element, context) ->
                                Integer.parseInt(element.substring("row-".length())) % 2 == 0
                                        ? even
                                        : odd,
                        createOptions);

        try {
            writer.write("row-1", TestContexts.NO_OP);
            writer.write("row-2", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(readRows(even))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly("row-2");
            assertThat(readRows(odd))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly("row-1");
            assertThat(describeTable("auto-created-even").getColumnFamilies())
                    .extracting(ColumnFamily::getId)
                    .containsExactly(FAMILY);
            assertThat(describeTable("auto-created-odd").getColumnFamilies())
                    .extracting(ColumnFamily::getId)
                    .containsExactly(FAMILY);
        } finally {
            writer.close();
        }
    }

    /** Builds a CREATE_IF_NEEDED writer through the production factory and admin. */
    private static SinkWriter<String> writer(
            TableDestination table, TableCreateOptions createOptions) throws Exception {
        return writer((element, context) -> table, createOptions);
    }

    @SuppressWarnings("unchecked")
    private static SinkWriter<String> writer(
            DestinationResolver<String> resolver, TableCreateOptions createOptions)
            throws Exception {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();
        Sink<String> sink =
                BigtableSink.<String>builder()
                        .destinationResolver(resolver)
                        .serializer(
                                (element, context) ->
                                        RowMutationEntry.create(element)
                                                .setCell(FAMILY, "payload", 1_000L, "v"))
                        .writerOptions(options)
                        .createDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .tableCreateOptions(createOptions)
                        .emulatorEndpoint(emulatorEndpoint())
                        .build();
        MutationBatcherFactory factory =
                new DefaultMutationBatcherFactory(
                        null, options, EmulatorEndpoint.parse(emulatorEndpoint()));
        return ((BigtableMutateRowsSink<String>) sink)
                .createWriter(
                        factory,
                        new BigtableTableAdmin(EmulatorEndpoint.parse(emulatorEndpoint())),
                        new FakeMailboxExecutor(),
                        TestSinkWriterMetricGroup.create());
    }
}
