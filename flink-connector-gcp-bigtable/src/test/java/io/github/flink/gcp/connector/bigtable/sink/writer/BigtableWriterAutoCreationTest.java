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
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableWriter}'s table auto-creation repair, against the fakes whose
 * missing-table state the {@link FakeTableAdmin#onEnsure} hook clears — so convergence emerges from
 * the ensure the way it does against the service, rather than being scripted turn by turn.
 *
 * <p>Timed out as a class, like {@code BigtableWriterTest}: the fake mailbox blocks on an empty
 * queue exactly as the real one does, so a repair loop that waits for a completion that cannot
 * arrive hangs the test rather than failing it.
 */
@Timeout(30)
class BigtableWriterAutoCreationTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final TableDestination OTHER_TABLE = TableDestination.of("p", "i", "events");

    private static final TableCreateOptions CREATE_OPTIONS =
            TableCreateOptions.builder().columnFamily("cf").build();

    /** Backoffs of one millisecond, so the production schedule stays out of the wall clock. */
    private static final RetrySchedule FAST_SCHEDULE = new RetrySchedule(1, 1, 5, 0);

    private final FakeMutationBatcherFactory factory = new FakeMutationBatcherFactory();
    private final FakeMutationBatcher batcher = factory.batcherFor(TABLE);
    private final FakeTableAdmin admin = new FakeTableAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();

    @Test
    void aMissingTableIsRepairedByTheFlush() throws Exception {
        admin.onEnsure = () -> batcher.tableMissing = false;
        BigtableWriter<String> writer = writer(FailureHandler.failJob());
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(admin.ensured).containsExactly(TABLE);
        assertThat(admin.ensureOptions).containsExactly(CREATE_OPTIONS);
        // The failed batch and its re-application, nothing else — and the records are counted
        // once, not per application.
        assertThat(batcher.sentRowKeys())
                .containsExactly(List.of("row-1", "row-2"), List.of("row-1", "row-2"));
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(2);
        assertThat(metricGroup.counterValue("tablesCreated")).isEqualTo(1);
        assertThat(metricGroup.counterValue("columnFamiliesAdded")).isZero();
        // Each entry's NOT_FOUND is a real per-entry give-up, counted like any other apply
        // failure; the repair leaves nothing behind.
        assertThat(metricGroup.counterValue("errorClass", "NOT_FOUND", "errors")).isEqualTo(2);
        assertThat(writer.getParkedMutations()).isZero();
        assertThat(writer.getInFlightMutations()).isZero();
    }

    @Test
    void oneRepairEnsuresEveryTableThatReportedItselfMissing() throws Exception {
        // Each ensure clears only the flag of the table it just ensured, so a repair that ensures
        // one table and calls the incident closed leaves the other missing and spends its whole
        // budget. That is what a single "have I ensured yet" flag would do: the tables of one
        // incident are ensured per table, not per repair.
        admin.onEnsure = () -> factory.batcherFor(lastEnsured()).tableMissing = false;
        BigtableWriter<String> writer = multiTableWriter(FailureHandler.failJob());
        factory.batcherFor(TABLE).tableMissing = true;
        factory.batcherFor(OTHER_TABLE).tableMissing = true;

        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(admin.ensured).containsExactly(TABLE, OTHER_TABLE);
        // One creation schema serves every table the sink creates.
        assertThat(admin.ensureOptions).containsExactly(CREATE_OPTIONS, CREATE_OPTIONS);
        assertThat(metricGroup.counterValue("tablesCreated")).isEqualTo(2);
        assertThat(factory.batcherFor(TABLE).sentRowKeys())
                .containsExactly(List.of("orders/row-1"), List.of("orders/row-1"));
        assertThat(factory.batcherFor(OTHER_TABLE).sentRowKeys())
                .containsExactly(List.of("events/row-2"), List.of("events/row-2"));
        assertThat(writer.getParkedMutations()).isZero();
    }

    @Test
    void anEnsureFailingPartWayThroughLeavesTheTablesItDidNotReachOwed() throws Exception {
        // Two attempts, so the tables the failed pass did not reach have exactly one attempt left:
        // a repair that dropped them would spend it re-applying against a table nothing created,
        // and fail. The budget is what makes the omission visible — it self-heals given enough
        // attempts, which is precisely why a looser one would not fail on the omission.
        admin.onEnsure = () -> factory.batcherFor(lastEnsured()).tableMissing = false;
        admin.ensureFailures.add(new IOException("the admin was unavailable"));
        BigtableWriter<String> writer =
                multiTableWriter(FailureHandler.failJob(), new RetrySchedule(1, 1, 2, 0));
        factory.batcherFor(TABLE).tableMissing = true;
        factory.batcherFor(OTHER_TABLE).tableMissing = true;

        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);
        writer.flush(false);

        // The first attempt's ensure of the first table threw, so both tables were still owed; the
        // second attempt ensured both.
        assertThat(admin.ensured).containsExactly(TABLE, TABLE, OTHER_TABLE);
        assertThat(writer.getParkedMutations()).isZero();
    }

    @Test
    void theNextWriteRepairsAParkedIncident() throws Exception {
        admin.onEnsure = () -> batcher.tableMissing = false;
        // A cap of one makes write() yield, which is how a parked incident can exist between two
        // writes at all: the failure mails run inside awaitCapacity.
        BigtableWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxInFlightMutations(1).build(),
                        FailureHandler.failJob(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        CREATE_OPTIONS);
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);
        // The SDK side of the fake: the batcher's own threshold or timer sends the batch without
        // the writer asking, which is how a NOT_FOUND surfaces between checkpoints.
        batcher.sendOutstanding();
        // This write runs the failure mail (parking row-1) while waiting for capacity; the park
        // did not exist when the write began, so the repair belongs to the next call.
        writer.write("row-2", TestContexts.NO_OP);
        assertThat(admin.ensured).isEmpty();
        assertThat(writer.getParkedMutations()).isEqualTo(1);

        writer.write("row-3", TestContexts.NO_OP);

        // The repair drained row-2 into the park too (still missing until the ensure), then
        // created the table and re-applied both.
        assertThat(admin.ensured).containsExactly(TABLE);
        assertThat(writer.getParkedMutations()).isZero();

        writer.flush(false);
        assertThat(batcher.sentRowKeys())
                .containsExactly(
                        List.of("row-1"),
                        List.of("row-2"),
                        List.of("row-1", "row-2"),
                        List.of("row-3"));
    }

    @Test
    void budgetExhaustionFailsWithTheCauseAndTheUndeclaredFamilyHint() throws Exception {
        // No onEnsure hook: the ensure "succeeds" but the service keeps answering NOT_FOUND —
        // which is exactly what a mutation naming an undeclared family produces forever.
        BigtableWriter<String> writer = writer(FailureHandler.failJob());
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("5 attempt(s)")
                .hasMessageContaining("tableCreateOptions")
                .hasStackTraceContaining("scripted missing table");
        // Once per repair, however many attempts the budget allowed: a succeeded ensure is not
        // repeated.
        assertThat(admin.ensured).hasSize(1);
        // The initial park plus one per failed re-application: each is a fresh give-up, per the
        // per-attempt counting rule.
        assertThat(metricGroup.counterValue("errorClass", "NOT_FOUND", "errors")).isEqualTo(6);
    }

    @Test
    void aTransientlyFailingEnsureSpendsAnAttemptRatherThanTheJob() throws Exception {
        // The admin client retries neither of its RPCs, so the recovery schedule is the only
        // thing standing between one transient creation failure and a restart.
        admin.ensureFailures.add(new IOException("one transient admin failure"));
        admin.onEnsure = () -> batcher.tableMissing = false;
        BigtableWriter<String> writer = writer(FailureHandler.failJob());
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(admin.ensured).hasSize(2);
        assertThat(metricGroup.counterValue("tablesCreated")).isEqualTo(1);
        assertThat(writer.getParkedMutations()).isZero();
    }

    @Test
    void aPersistentlyFailingEnsureExhaustsTheBudgetWithItsFailure() throws Exception {
        IOException failure = new IOException("admin boom");
        for (int i = 0; i < FAST_SCHEDULE.maxAttempts(); i++) {
            admin.ensureFailures.add(failure);
        }
        RecordingHandler handler = new RecordingHandler();
        BigtableWriter<String> writer = writer(handler);
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false)).isSameAs(failure);
        // One ensure per attempt, none repeated after success (none succeeded).
        assertThat(admin.ensured).hasSize(FAST_SCHEDULE.maxAttempts());
        // Abandoned rather than routed: the checkpoint did not complete, so the restart replays
        // the record. A handler that may drop must not see a failure creation could have fixed.
        assertThat(handler.handled).isEmpty();
        assertThat(writer.getParkedMutations()).isEqualTo(1);
        // The reporter-visible gauge covers the repair queue, not only the isolation park.
        assertThat(metricGroup.<Integer>gaugeValue("parkedMutations")).isEqualTo(1);
        assertThat(metricGroup.counterValue("tablesCreated")).isZero();
    }

    @Test
    void aLostRaceCountsFamiliesAddedRatherThanTablesCreated() throws Exception {
        admin.result = TableAdmin.EnsureResult.familiesAdded(2);
        admin.onEnsure = () -> batcher.tableMissing = false;
        BigtableWriter<String> writer = writer(FailureHandler.failJob());
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(metricGroup.counterValue("tablesCreated")).isZero();
        assertThat(metricGroup.counterValue("columnFamiliesAdded")).isEqualTo(2);
    }

    @Test
    void aRepairedBatchWithAGenuinelyInvalidEntryHandsItToTheIsolationPass() throws Exception {
        admin.onEnsure = () -> batcher.tableMissing = false;
        batcher.rejectedRowKeys.add("bad");
        RecordingHandler handler = new RecordingHandler();
        BigtableWriter<String> writer = writer(handler);
        batcher.tableMissing = true;

        writer.write("good", TestContexts.NO_OP);
        writer.write("bad", TestContexts.NO_OP);
        writer.flush(false);

        // One incident, two mechanisms in order: the repair creates the table and re-applies the
        // batch; the re-application's request-level INVALID_ARGUMENT parks both entries for
        // isolation, whose solos then split collateral damage from the true rejection.
        assertThat(admin.ensured).containsExactly(TABLE);
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("bad");
        assertThat(batcher.sentRowKeys())
                .containsExactly(
                        List.of("good", "bad"),
                        List.of("good", "bad"),
                        List.of("good"),
                        List.of("bad"));
        assertThat(writer.getParkedMutations()).isZero();
    }

    @Test
    void aSoloNotFoundMigratesToTheRepairQueueWithoutTrippingTheIsolationInvariant()
            throws Exception {
        // The table vanishes mid-flush: the first request draws the request-level
        // INVALID_ARGUMENT, and every later one — the isolation pass's solos — meets NOT_FOUND.
        // The solos must migrate to the repair queue rather than re-park for isolation, or the
        // pass's invariant tripwire would fail a repairable incident.
        admin.onEnsure =
                () -> {
                    batcher.tableMissing = false;
                    batcher.tableMissingAfterSends = Integer.MAX_VALUE;
                };
        batcher.rejectedRowKeys.add("bad");
        batcher.tableMissingAfterSends = 1;
        RecordingHandler handler = new RecordingHandler();
        BigtableWriter<String> writer = writer(handler);

        writer.write("good", TestContexts.NO_OP);
        writer.write("bad", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(admin.ensured).containsExactly(TABLE);
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("bad");
        // Batch rejected; both solos meet NOT_FOUND and migrate; the repair re-applies both; the
        // re-application is rejected over "bad" again; the second pass's solos settle it.
        assertThat(batcher.sentRowKeys())
                .containsExactly(
                        List.of("good", "bad"),
                        List.of("good"),
                        List.of("bad"),
                        List.of("good", "bad"),
                        List.of("good"),
                        List.of("bad"));
        assertThat(writer.getParkedMutations()).isZero();
    }

    @Test
    void createNeverFailsWithTheDispositionHintAndCreatesNothing() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        BigtableWriter<String> writer =
                writer(
                        BigtableWriterOptions.defaults(),
                        handler,
                        CreateDisposition.CREATE_NEVER,
                        null);
        batcher.tableMissing = true;

        writer.write("row-1", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("createDisposition is CREATE_NEVER");
        assertThat(admin.ensured).isEmpty();
        assertThat(handler.handled).isEmpty();
        assertThat(writer.getParkedMutations()).isZero();
    }

    @Test
    void closeClearsTheRepairQueueAndClosesTheAdmin() throws Exception {
        IOException failure = new IOException("admin boom");
        for (int i = 0; i < FAST_SCHEDULE.maxAttempts(); i++) {
            admin.ensureFailures.add(failure);
        }
        BigtableWriter<String> writer = writer(FailureHandler.failJob());
        batcher.tableMissing = true;
        writer.write("row-1", TestContexts.NO_OP);
        assertThatThrownBy(() -> writer.flush(false)).isSameAs(failure);
        assertThat(writer.getParkedMutations()).isEqualTo(1);

        writer.close();

        assertThat(writer.getParkedMutations()).isZero();
        assertThat(admin.closeCalls).isEqualTo(1);
        assertThat(batcher.closeCalls).isEqualTo(1);
    }

    @Test
    void theAdminIsClosedEvenWhenTheBatcherCloseThrows() throws Exception {
        batcher.closeFailure = new IllegalStateException("batcher close boom");
        BigtableWriter<String> writer = writer(FailureHandler.failJob());
        // A batcher exists only for a table a record was routed to, so the failing close this test
        // is about needs one record first.
        writer.write("row-1", TestContexts.NO_OP);

        assertThatThrownBy(writer::close).isSameAs(batcher.closeFailure);

        assertThat(admin.closeCalls).isEqualTo(1);
    }

    private BigtableWriter<String> writer(FailureHandler<? super FailedMutation> handler) {
        return writer(
                BigtableWriterOptions.defaults(),
                handler,
                CreateDisposition.CREATE_IF_NEEDED,
                CREATE_OPTIONS);
    }

    private BigtableWriter<String> writer(
            BigtableWriterOptions options,
            FailureHandler<? super FailedMutation> handler,
            CreateDisposition disposition,
            TableCreateOptions createOptions) {
        return writer(
                options,
                handler,
                disposition,
                createOptions,
                (element, context) -> TABLE,
                FAST_SCHEDULE);
    }

    /** The table the admin was last asked to ensure, for a hook clearing only that table's flag. */
    private TableDestination lastEnsured() {
        return admin.ensured.get(admin.ensured.size() - 1);
    }

    private BigtableWriter<String> multiTableWriter(
            FailureHandler<? super FailedMutation> handler) {
        return multiTableWriter(handler, FAST_SCHEDULE);
    }

    /**
     * A writer routing a row key prefixed {@code events/} to {@link #OTHER_TABLE}, everything else
     * to {@link #TABLE}.
     */
    private BigtableWriter<String> multiTableWriter(
            FailureHandler<? super FailedMutation> handler, RetrySchedule schedule) {
        return writer(
                BigtableWriterOptions.defaults(),
                handler,
                CreateDisposition.CREATE_IF_NEEDED,
                CREATE_OPTIONS,
                (element, context) -> element.startsWith("events/") ? OTHER_TABLE : TABLE,
                schedule);
    }

    private BigtableWriter<String> writer(
            BigtableWriterOptions options,
            FailureHandler<? super FailedMutation> handler,
            CreateDisposition disposition,
            TableCreateOptions createOptions,
            DestinationResolver<String> resolver,
            RetrySchedule schedule) {
        BigtableSinkBuilder<String> builder =
                BigtableSink.<String>builder()
                        .destinationResolver(resolver)
                        .serializer(
                                (element, context) ->
                                        RowMutationEntry.create(element)
                                                .setCell("cf", "q", element))
                        .writerOptions(options)
                        .failedMutationHandler(handler)
                        .createDisposition(disposition);
        if (createOptions != null) {
            builder.tableCreateOptions(createOptions);
        }
        BigtableMutateRowsSink<String> sink = (BigtableMutateRowsSink<String>) builder.build();
        return new BigtableWriter<>(
                sink.getConfig(), factory, admin, mailbox, metricGroup, schedule, System::nanoTime);
    }

    /** A handler that drops every mutation, recording what it saw. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedMutation> handled = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            handled.add((FailedMutation) element);
        }
    }
}
