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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.ReadModifyWriteRowRequest.Rule;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the single-row request writer against the Bigtable emulator, through the
 * production {@link DefaultSingleRowClientFactory} in its emulator-endpoint mode — so the client
 * construction and the per-RPC settings that ship are what carry the requests here.
 *
 * <p>The emulator implements both RPCs; what it answers a malformed request with is not evidence
 * about the service, so nothing here asserts a service-side rejection — the one refusal asserted is
 * the client library's own, which runs before anything is sent. The results are discarded on this
 * surface by design (ADR-0148): what a request did is read back through the harness's client.
 */
class SingleRowRequestWriterEmulatorITCase extends AbstractBigtableEmulatorITCase {

    private static final String COUNTERS = "counters";
    private static final ByteString QUALIFIER = ByteString.copyFromUtf8("q");

    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();

    @Test
    void appliesTheMatchingAndTheNonMatchingBranchOfAConditionalMutation() throws Exception {
        TableDestination table = createTable("conditional");
        seedRows(table, "present");
        SinkWriter<String> writer =
                writer(
                        (element, context) -> table,
                        (element, context) ->
                                new CheckAndMutateRowRequest(
                                        ByteString.copyFromUtf8(element),
                                        Filters.FILTERS.family().exactMatch(FAMILY),
                                        Mutation.create().setCell(FAMILY, "matched", 1_000L, "yes"),
                                        Mutation.create()
                                                .setCell(FAMILY, "matched", 1_000L, "no")));

        try {
            writer.write("present", TestContexts.NO_OP);
            writer.write("absent", TestContexts.NO_OP);
            // Nothing is asserted before the flush: the RPCs are in flight until it drains them.
            writer.flush(false);

            Map<String, String> matched =
                    readRows(table).stream()
                            .collect(
                                    Collectors.toMap(
                                            row -> row.getKey().toStringUtf8(),
                                            row -> value(row, FAMILY, "matched")));
            assertThat(matched)
                    .containsEntry("present", "yes")
                    .containsEntry("absent", "no")
                    .hasSize(2);
            assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_ACCEPTED))
                    .isEqualTo(2);
            assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED))
                    .isEqualTo(2);
            assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isZero();
            assertThat(metricGroup.<Integer>gaugeValue(BigtableMetricNames.IN_FLIGHT_REQUESTS))
                    .isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void appliesAppendsAndIncrementsInOrderOnOneRow() throws Exception {
        TableDestination table = createTable("read-modify-write", FAMILY, COUNTERS);
        SinkWriter<String> writer =
                writer(
                        (element, context) -> table,
                        (element, context) ->
                                new ReadModifyWriteRowRequest(
                                        ByteString.copyFromUtf8("row"),
                                        Arrays.asList(
                                                Rule.append(
                                                        FAMILY,
                                                        QUALIFIER,
                                                        ByteString.copyFromUtf8(element)),
                                                Rule.increment(
                                                        COUNTERS, QUALIFIER, element.length()))));

        try {
            writer.write("ab", TestContexts.NO_OP);
            writer.flush(false);
            // Two records on one row, each flushed: the second request reads what the first
            // wrote, which is the read-modify-write the RPC is named for.
            writer.write("cde", TestContexts.NO_OP);
            writer.flush(false);

            List<Row> rows = readRows(table);
            assertThat(rows).hasSize(1);
            assertThat(value(rows.get(0), FAMILY, "q")).isEqualTo("abcde");
            ByteString counter = rows.get(0).getCells(COUNTERS, "q").get(0).getValue();
            assertThat(counter.size()).isEqualTo(8);
            assertThat(ByteBuffer.wrap(counter.toByteArray()).getLong()).isEqualTo(5L);
            assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED))
                    .isEqualTo(2);
        } finally {
            writer.close();
        }
    }

    @Test
    void routesEachRecordToTheTableItsResolverNamesOverOneClient() throws Exception {
        TableDestination even = createTable("routed-even");
        TableDestination odd = createTable("routed-odd");
        SinkWriter<Integer> writer =
                writer(
                        (element, context) -> element % 2 == 0 ? even : odd,
                        (element, context) ->
                                element == 7
                                        ? null
                                        : new CheckAndMutateRowRequest(
                                                ByteString.copyFromUtf8("row-" + element),
                                                null,
                                                null,
                                                Mutation.create()
                                                        .setCell(FAMILY, "q", 1_000L, "v")));

        try {
            for (int i = 0; i < 10; i++) {
                writer.write(i, TestContexts.NO_OP);
            }
            writer.flush(false);

            // Both tables live in one instance, so the production factory built one client and
            // leased it to both; the rows landing where the resolver said is what says the table
            // id was taken from the destination at start time and not from the request.
            assertThat(rowKeys(even)).containsExactly("row-0", "row-2", "row-4", "row-6", "row-8");
            assertThat(rowKeys(odd)).containsExactly("row-1", "row-3", "row-5", "row-9");
            assertThat(metricGroup.counterValue(BigtableMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
            assertThat(metricGroup.<Integer>gaugeValue(BigtableMetricNames.ACTIVE_CLIENTS))
                    .isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void aRequestTheClientRefusesBeforeSendingReachesTheHandlerAsARowLevelFailure()
            throws Exception {
        // The client's own validation runs synchronously inside the async call: a conditional
        // mutation whose branches are both empty is refused with an IllegalStateException before
        // any RPC starts. This pins that shape of the real client — a fake cannot — so the
        // writer's row-level route for it stays anchored to what the SDK does.
        TableDestination table = createTable("refused");
        List<FailedRequest> failed = new ArrayList<>();
        SinkWriter<String> writer =
                writer(
                        (element, context) -> table,
                        (element, context) ->
                                new CheckAndMutateRowRequest(
                                        ByteString.copyFromUtf8(element),
                                        null,
                                        Mutation.create(),
                                        Mutation.create()),
                        failed::add);

        try {
            writer.write("empty", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(failed).hasSize(1);
            FailedRequest failure = failed.get(0);
            assertThat(failure.getRowKey().toStringUtf8()).isEqualTo("empty");
            assertThat(failure.getOperation()).isEqualTo(RowOperation.CHECK_AND_MUTATE_ROW);
            assertThat(failure.getCause())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("then")
                    .hasMessageContaining("otherwise");
            assertThat(readRows(table)).isEmpty();
            assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_ACCEPTED)).isZero();
            assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    private <T> SinkWriter<T> writer(
            DestinationResolver<T> resolver, RowRequestSerializer<T> serializer) {
        return writer(resolver, serializer, FailureHandler.failJob());
    }

    private <T> SinkWriter<T> writer(
            DestinationResolver<T> resolver,
            RowRequestSerializer<T> serializer,
            FailureHandler<? super FailedRequest> handler) {
        BigtableRequestOptions options = BigtableRequestOptions.builder().build();
        return new SingleRowRequestWriter<>(
                new SingleRowRequestConfig<>(
                        resolver, serializer, null, options, handler, null, null),
                new DefaultSingleRowClientFactory(
                        null,
                        options,
                        EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"),
                        null),
                mailbox,
                metricGroup);
    }

    private static String value(Row row, String family, String qualifier) {
        return row.getCells(family, qualifier).get(0).getValue().toStringUtf8();
    }

    private static List<String> rowKeys(TableDestination table) {
        return readRows(table).stream()
                .map(row -> row.getKey().toStringUtf8())
                .collect(Collectors.toList());
    }
}
