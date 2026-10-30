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

package io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.OrderingMode;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link BigtableWriter} under {@link OrderingMode#PER_KEY}.
 *
 * <p>A record is {@code "row:value"}: the writer orders by the part before the colon, and the
 * requests are read back as the same strings, so an assertion names both which row a request
 * carried and which of its writes. Timed out as a class, as {@code BigtableWriterTest} is: a
 * buffered entry that no turn ever readies is a hang, not a failure.
 */
@Timeout(30)
class BigtableWriterRowOrderTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final TableDestination OTHER_TABLE = TableDestination.of("p", "i", "events");
    private static final TableDestination SECOND_INSTANCE =
            TableDestination.of("p", "second", "orders");

    private static final long STALL_WARN_AFTER_NANOS = java.time.Duration.ofHours(1).toNanos();

    private final FakeMutationBatcherFactory factory = new FakeMutationBatcherFactory();
    private final FakeMutationBatcher batcher = factory.batcherFor(TABLE);
    private final FakeTableAdmin admin = new FakeTableAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();
    private final RecordingHandler handler = new RecordingHandler();

    /** Where the resolver sends each record; {@link #TABLE} unless a test says otherwise. */
    private final Map<String, TableDestination> destinations = new HashMap<>();

    @Test
    void sendsARowsLaterEntryOnlyAfterItsEarlierEntryIsAnswered() throws Exception {
        BigtableWriter<String> writer = writer(perKey().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(writer.getBufferedEntries()).isEqualTo(1);
        assertThat(writer.getBufferedBytes()).isEqualTo(size("r1:b"));
        assertThat(metricGroup.<Integer>gaugeValue("bufferedEntries")).isEqualTo(1);
        writer.flush(false);

        // r2 was not held behind r1, and r1:b went in a request of its own after r1:a's.
        assertThat(sent()).containsExactly(List.of("r1:a", "r2:a"), List.of("r1:b"));
        assertThat(writer.getBufferedEntries()).isZero();
        assertThat(writer.getBufferedBytes()).isZero();
        assertThat(writer.getInFlightEntries()).isZero();
        // A held entry is a record sent once, when it is handed over.
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(3);
    }

    @Test
    void withoutTheModeARowsEntriesShareARequest() throws Exception {
        // The control for the test above: the default sends both writes of r1 in one request,
        // where MutateRows may apply them in either order.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.builder().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(sent()).containsExactly(List.of("r1:a", "r1:b", "r2:a"));
        assertThat(writer.getBufferedEntries()).isZero();
    }

    @Test
    void anEntryParkedForItsSoloVerdictKeepsItsRowsTurn() throws Exception {
        // Two entries in flight at most, so admitting r1:b waits, and the wait's mail parks r1:a:
        // the request carrying r2 is refused whole (#239). Were the park to end r1's turn, r1:b
        // would be admitted at once and sent before r1:a's solo re-submission.
        batcher.rejectedRowKeys.add("r2");
        BigtableWriter<String> writer = writer(perKey().maxInFlightEntries(2).build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r2:x", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        // The wait ended at r1:a's mail, with r2:x's still queued.
        assertThat(writer.getParkedEntries()).isEqualTo(1);
        assertThat(writer.getBufferedEntries()).isEqualTo(1);
        writer.flush(false);

        assertThat(sent())
                .containsExactly(
                        List.of("r1:a", "r2:x"), List.of("r1:a"), List.of("r2:x"), List.of("r1:b"));
        assertThat(handler.rowKeys()).containsExactly("r2");
        assertThat(writer.getBufferedEntries()).isZero();
    }

    @Test
    void anEntryParkedForTheRepairKeepsItsRowsTurn() throws Exception {
        // The same shape for a missing table: r1:a parks for the repair, and r1:b waits until the
        // repair's re-application of r1:a is answered rather than racing it into the new table.
        batcher.tableMissing = true;
        admin.onEnsure = () -> batcher.tableMissing = false;
        BigtableWriter<String> writer =
                writer(
                        perKey().maxInFlightEntries(1).build(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        TableCreateOptions.builder().columnFamily("cf").build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        assertThat(writer.getParkedEntries()).isEqualTo(1);
        assertThat(writer.getBufferedEntries()).isEqualTo(1);
        writer.flush(false);

        assertThat(sent()).containsExactly(List.of("r1:a"), List.of("r1:a"), List.of("r1:b"));
        assertThat(admin.ensured).containsExactly(TABLE);
        assertThat(writer.getBufferedEntries()).isZero();
    }

    @Test
    void aRoutedEntryEndsItsRowsTurn() throws Exception {
        // Every request carrying r1 is refused, so both of its entries are confirmed alone and
        // routed, in order; the second is sent only once the first is routed.
        batcher.rejectedRowKeys.add("r1");
        BigtableWriter<String> writer = writer(perKey().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(sent())
                .containsExactly(
                        List.of("r1:a"), List.of("r1:a"), List.of("r1:b"), List.of("r1:b"));
        assertThat(handler.records()).containsExactly("r1:a", "r1:b");
        assertThat(writer.getBufferedEntries()).isZero();
        assertThat(writer.getParkedEntries()).isZero();
    }

    @Test
    void bufferedEntriesCountAgainstTheEntryCap() throws Exception {
        // With r1:a in flight and r1:b buffered the writer is at a cap of two, so r2:a waits for
        // r1:a's answer. Were buffered entries not counted, r2:a would be admitted at once and
        // share r1:a's request.
        BigtableWriter<String> writer = writer(perKey().maxInFlightEntries(2).build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(sent()).containsExactly(List.of("r1:a"));
        // The wait readied r1:b and submitted it before admitting r2:a.
        assertThat(batcher.entries)
                .extracting(BigtableWriterRowOrderTest::record)
                .containsExactly("r1:a", "r1:b", "r2:a");
        writer.flush(false);
        assertThat(sent()).containsExactly(List.of("r1:a"), List.of("r1:b", "r2:a"));
    }

    @Test
    void bufferedEntriesCountAgainstTheByteCap() throws Exception {
        // The byte half of the test above: the entry cap is out of reach, and r1:a plus r1:b
        // reach the byte cap.
        BigtableWriter<String> writer =
                writer(perKey().maxInFlightBytes(size("r1:a") + size("r1:b")).build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(sent()).containsExactly(List.of("r1:a"));
        writer.flush(false);
        assertThat(sent()).containsExactly(List.of("r1:a"), List.of("r1:b", "r2:a"));
    }

    @Test
    void theAdmissionWaitConfirmsAParkedEntryWhenOnlyBufferedEntriesHoldTheCap() throws Exception {
        // r1's second write is as large as the byte cap on its own. Once the wait's mail parks
        // r1:a, nothing is in flight and r1:b alone holds the cap; no mail can arrive, so the wait
        // must run the isolation pass itself, or it spins until the test times out.
        batcher.rejectedRowKeys.add("r1");
        String large = "r1:" + "b".repeat(400);
        BigtableWriter<String> writer = writer(perKey().maxInFlightBytes(size(large)).build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write(large, TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);

        // The pass routed r1:a and so readied the large write, which the wait then sent; its own
        // refusal parked it in turn, after which nothing held the cap and r2:a was admitted.
        assertThat(handler.records()).containsExactly("r1:a");
        assertThat(writer.getBufferedEntries()).isZero();
        assertThat(batcher.entries)
                .extracting(BigtableWriterRowOrderTest::record)
                .containsExactly("r1:a", "r1:a", large, "r2:a");
        writer.flush(false);
        assertThat(handler.records()).containsExactly("r1:a", large);
    }

    @Test
    void theAdmissionWaitRunsTheRepairWhenOnlyBufferedEntriesHoldTheCap() throws Exception {
        // The repair's counterpart: r1:a parks for the repair inside the wait, and r1:b alone
        // holds the byte cap.
        batcher.tableMissing = true;
        admin.onEnsure = () -> batcher.tableMissing = false;
        String large = "r1:" + "b".repeat(400);
        BigtableWriter<String> writer =
                writer(
                        perKey().maxInFlightBytes(size(large)).build(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        TableCreateOptions.builder().columnFamily("cf").build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write(large, TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(admin.ensured).containsExactly(TABLE);
        assertThat(sent()).startsWith(List.of("r1:a"), List.of("r1:a"), List.of(large));
        assertThat(writer.getBufferedEntries()).isZero();
    }

    @Test
    void theSameRowKeyInAnotherTableIsAnotherRow() throws Exception {
        destinations.put("r1:b", OTHER_TABLE);
        BigtableWriter<String> writer = writer(perKey().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        assertThat(writer.getBufferedEntries()).isZero();
        assertThat(factory.batcherFor(OTHER_TABLE).entries)
                .extracting(BigtableWriterRowOrderTest::record)
                .containsExactly("r1:b");
    }

    @Test
    void anEntryGoesToItsTablesLiveBatcherWhenTheWaitEvictsAnInstance() throws Exception {
        // One instance at a time and a cap of two. Admitting r2:x waits for r1:a's answer and
        // readies r1:b, and opening r2:x's instance then evicts r1's. Taking r2:x's state before
        // the wait would let r1:b's submission evict r2:x's instance in turn and hand r2:x to its
        // closed batcher, which no flush sends, so the flush below would never return.
        destinations.put("r2:x", SECOND_INSTANCE);
        BigtableWriter<String> writer =
                writer(perKey().maxInFlightEntries(2).maxActiveInstances(1).build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r2:x", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(sent()).containsExactly(List.of("r1:a"), List.of("r1:b"));
        assertThat(factory.batcherFor(SECOND_INSTANCE).entries)
                .extracting(BigtableWriterRowOrderTest::record)
                .containsExactly("r2:x");
        assertThat(factory.batcherFor(SECOND_INSTANCE).sentBatches).hasSize(1);
        assertThat(writer.getInFlightEntries()).isZero();
    }

    @Test
    void closeForgetsWhatIsBuffered() throws Exception {
        BigtableWriter<String> writer = writer(perKey().build());
        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        writer.close();

        assertThat(writer.getBufferedEntries()).isZero();
        assertThat(writer.getBufferedBytes()).isZero();
        assertThat(metricGroup.<Integer>gaugeValue("bufferedEntries")).isEqualTo(0);
    }

    @Test
    void anAnswerArrivingAfterCloseEndsNoTurn() throws Exception {
        // Flink quiesces the mailbox before it closes operators, but a harness that still runs
        // mails after close must not find the cleared sequencer asked for a turn it forgot.
        batcher.autoComplete = false;
        BigtableWriter<String> writer = writer(perKey().build());
        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.close();

        batcher.succeed(0);

        assertThatCode(mailbox::drain).doesNotThrowAnyException();
        assertThat(writer.getBufferedEntries()).isZero();
    }

    private static BigtableWriterOptions.Builder perKey() {
        return BigtableWriterOptions.builder().orderingMode(OrderingMode.PER_KEY);
    }

    private BigtableWriter<String> writer(BigtableWriterOptions options) {
        return writer(options, CreateDisposition.CREATE_NEVER, null);
    }

    private BigtableWriter<String> writer(
            BigtableWriterOptions options,
            CreateDisposition disposition,
            TableCreateOptions createOptions) {
        BigtableSinkBuilder<String> builder =
                BigtableSink.<String>builder()
                        .destinationResolver(
                                (element, context) -> destinations.getOrDefault(element, TABLE))
                        .serializer(serializer())
                        .writerOptions(options)
                        .failedMutationHandler(handler)
                        .createDisposition(disposition);
        if (createOptions != null) {
            builder.tableCreateOptions(createOptions);
        }
        BigtableMutateRowsSink<String> sink = (BigtableMutateRowsSink<String>) builder.build();
        return new BigtableWriter<>(
                sink.getConfig(),
                factory,
                admin,
                mailbox,
                metricGroup,
                new RetrySchedule(1, 1, 3, 0),
                System::nanoTime,
                STALL_WARN_AFTER_NANOS);
    }

    /** The requests the batcher sent, each as the records it carried. */
    private List<List<String>> sent() {
        List<List<String>> requests = new ArrayList<>();
        for (List<Integer> batch : batcher.sentBatches) {
            List<String> records = new ArrayList<>();
            for (int index : batch) {
                records.add(record(batcher.entries.get(index)));
            }
            requests.add(records);
        }
        return requests;
    }

    private static BigtableSerializationSchema<String> serializer() {
        return (element, context) -> entry(element);
    }

    private static RowMutationEntry entry(String record) {
        int colon = record.indexOf(':');
        return RowMutationEntry.create(record.substring(0, colon))
                .setCell("cf", "q", 1_000L, record.substring(colon + 1));
    }

    private static String record(RowMutationEntry entry) {
        return entry.toProto().getRowKey().toStringUtf8()
                + ":"
                + entry.toProto().getMutations(0).getSetCell().getValue().toStringUtf8();
    }

    private static long size(String record) {
        return entry(record).toProto().getSerializedSize();
    }

    /** A handler that drops every mutation, recording what it saw. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedMutation> handled = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            handled.add((FailedMutation) element);
        }

        List<String> rowKeys() {
            List<String> keys = new ArrayList<>();
            handled.forEach(failed -> keys.add(failed.getRowKey().toStringUtf8()));
            return keys;
        }

        List<String> records() {
            List<String> records = new ArrayList<>();
            handled.forEach(failed -> records.add(record(failed.getEntry())));
            return records;
        }
    }
}
