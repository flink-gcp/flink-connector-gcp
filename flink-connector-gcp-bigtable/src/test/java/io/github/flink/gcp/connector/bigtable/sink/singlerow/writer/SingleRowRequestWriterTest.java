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

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SingleRowRequestWriter}, against a fake client factory and scripted
 * requests.
 *
 * <p>Each request's answer is under the test's control, so every terminal outcome — an answer, a
 * row-level rejection, an ambiguous or fatal failure, a cancellation — is driven explicitly and its
 * effect on the in-flight ledger, the counters and the failure handler asserted directly.
 *
 * <p>Timed out as a class: a wait the writer never leaves is a hang, not a failure, without one.
 */
@Timeout(30)
class SingleRowRequestWriterTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final TableDestination OTHER_TABLE = TableDestination.of("p", "i", "events");
    private static final TableDestination SECOND_INSTANCE =
            TableDestination.of("p", "i2", "orders");
    private static final TableDestination THIRD_INSTANCE = TableDestination.of("p", "i3", "orders");

    /** A stall-warning threshold no test here reaches; the stall test drives that path. */
    private static final long STALL_WARN_AFTER_NANOS = Duration.ofHours(1).toNanos();

    private final FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();
    private final MutableClock clock = new MutableClock();

    /** The request each record becomes, created on first use so a test can script it after. */
    private final Map<String, ScriptedRowRequest> requests = new LinkedHashMap<>();

    /** Records the serializer refuses, with the failure it throws. */
    private final Map<String, RuntimeException> unserializable = new LinkedHashMap<>();

    /** Records the serializer skips by returning {@code null}. */
    private final List<String> skipped = new ArrayList<>();

    /** Where the resolver sends each record; {@link #TABLE} unless a test says otherwise. */
    private final Map<String, TableDestination> destinations = new LinkedHashMap<>();

    @Test
    void anAnsweredRequestReleasesItsCapacityAndCounts() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(writer.getInFlight()).isEqualTo(2);
        assertThat(request("row-1").startedFor).isEqualTo(TABLE);
        assertThat(request("row-1").startedOn).isSameAs(factory.clientFor(TABLE));
        assertThat(metricGroup.counterValue("requestsAccepted")).isEqualTo(2);
        // Flink's standard counter moves at the hand-off, as in every sink here (ADR-0037), not
        // when the service answers.
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(2);
        assertThat(metricGroup.counterValue("requestsCompleted")).isZero();
        assertThat(metricGroup.<Integer>gaugeValue("inFlightRequests")).isEqualTo(2);
        assertThat(metricGroup.<Integer>gaugeValue("activeClients")).isEqualTo(1);

        request("row-1").succeed();
        request("row-2").succeed();
        mailbox.drain();

        assertThat(writer.getInFlight()).isZero();
        assertThat(metricGroup.counterValue("requestsCompleted")).isEqualTo(2);
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(2);
        assertThat(metricGroup.counterValue("requestsFailed")).isZero();
        assertThat(metricGroup.counterValue("numRecordsSendErrors")).isZero();
    }

    @Test
    void flushWaitsForEveryAcceptedRequest() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        // Answered from another thread while the flush is already inside its wait: what the wait
        // has to notice is a mail that arrives after it started polling, not one already queued.
        Thread answering =
                new Thread(
                        () -> {
                            sleepBriefly();
                            request("row-1").succeed();
                            sleepBriefly();
                            request("row-2").succeed();
                        });
        answering.start();

        writer.flush(false);
        answering.join();

        assertThat(writer.getInFlight()).isZero();
        assertThat(metricGroup.counterValue("requestsCompleted")).isEqualTo(2);
    }

    @Test
    void anInvalidRequestIsRoutedToTheHandlerAndReleasesItsCapacity() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SingleRowRequestWriter<String> writer = writer(options().build(), handler);
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        assertThat(writer.getInFlight()).isZero();
        assertThat(handler.handled).hasSize(1);
        FailedRequest failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(TABLE);
        assertThat(failed.getOperation()).isEqualTo(RowOperation.CHECK_AND_MUTATE_ROW);
        assertThat(failed.getRowKey()).isEqualTo(request("row-1").rowKey);
        assertThat(failed.getErrorMessage())
                .isEqualTo(
                        "The request was rejected because the request is invalid"
                                + " (INVALID_ARGUMENT).");
        assertThat(failed.getCause()).isNotNull();
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(1);
        assertThat(metricGroup.counterValue("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metricGroup.counterValue("errorClass", "INVALID_ARGUMENT", "errors"))
                .isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsTimedOut")).isZero();

        // Routed is not failed: the writer goes on.
        writer.write("row-2", TestContexts.NO_OP);
        request("row-2").succeed();
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
    }

    @Test
    void aMissingTableFailsTheJobFromTheNextCall() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").fail(StatusCode.Code.NOT_FOUND);
        mailbox.drain();

        assertThat(writer.getInFlight()).isZero();
        assertThatThrownBy(() -> writer.write("row-2", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "A CheckAndMutateRow request to Bigtable table "
                                + TABLE
                                + " failed because the table or one of its column families does"
                                + " not exist.");
        // The captured failure is sticky: flush reports it too, and does so before waiting.
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not exist");
        assertThat(metricGroup.counterValue("errorClass", "NOT_FOUND", "errors")).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(1);
        // A failure that fails the job reached no handler, so it is not a send error — the same
        // line the batching sink draws.
        assertThat(metricGroup.counterValue("numRecordsSendErrors")).isZero();
        assertThat(request("row-2").starts).isZero();
    }

    @Test
    void aDeadlineFailureIsAmbiguousAndCountsAsATimeout() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").fail(StatusCode.Code.DEADLINE_EXCEEDED);
        mailbox.drain();

        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsTimedOut")).isEqualTo(1);
        assertThat(metricGroup.counterValue("errorClass", "DEADLINE_EXCEEDED", "errors"))
                .isEqualTo(1);
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed with DEADLINE_EXCEEDED before the service answered")
                .hasMessageContaining("may or may not have applied it")
                .hasMessageContaining("at-least-once")
                .hasMessageContaining("ReadModifyWriteRow is not idempotent");
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "ABORTED", "CANCELLED"})
    void aFailureBeforeTheAnswerIsAmbiguous(StatusCode.Code code) throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").fail(code);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed with " + code.name() + " before the service answered")
                .hasMessageContaining("may or may not have applied it");
        assertThat(metricGroup.counterValue("requestsTimedOut")).isZero();
        assertThat(metricGroup.counterValue("errorClass", code.name(), "errors")).isEqualTo(1);
    }

    @Test
    void aCancelledFutureIsAmbiguousToo() throws Exception {
        // The client cancels a call when its channel shuts down under it; the future then reports
        // a CancellationException carrying no status at all.
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").future.cancel(true);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed with a cancellation before the service answered")
                .hasCauseInstanceOf(CancellationException.class);
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(1);
    }

    @Test
    void aRequestTheClientRefusesSynchronouslyCountsNothing() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        request("row-1").startFailure = new RejectedExecutionException("channel closed");

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Failed to start a CheckAndMutateRow request to Bigtable table "
                                + TABLE
                                + ".")
                .hasCauseInstanceOf(RejectedExecutionException.class);

        // Never accepted, so nothing is in flight to wait for or to release.
        assertThat(writer.getInFlight()).isZero();
        assertThat(metricGroup.counterValue("requestsAccepted")).isZero();
        assertThat(metricGroup.counterValue("requestsFailed")).isZero();
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
    }

    @Test
    void aRequestTheClientsValidationRefusesIsRoutedToTheHandlerWithoutCounting() throws Exception {
        // The SDK's request builders check the request's own content and throw synchronously from
        // the call — an IllegalStateException for a CheckAndMutateRow with neither then nor
        // otherwise, an IllegalArgumentException from its argument checks. That is the record's
        // failure, not the client's, so it goes where a rejected request goes.
        RecordingHandler handler = new RecordingHandler();
        SingleRowRequestWriter<String> writer = writer(options().build(), handler);
        request("row-1").startFailure =
                new IllegalStateException(
                        "ConditionalRowMutations must have `then` or `otherwise` mutations.");
        request("row-2").startFailure = new IllegalArgumentException("Family name can't be empty");

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(2);
        FailedRequest failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(TABLE);
        assertThat(failed.getOperation()).isEqualTo(RowOperation.CHECK_AND_MUTATE_ROW);
        assertThat(failed.getRowKey()).isEqualTo(request("row-1").rowKey);
        assertThat(failed.getErrorMessage())
                .isEqualTo(
                        "The request was refused by the client's validation before it was sent.");
        assertThat(failed.getCause()).isInstanceOf(IllegalStateException.class);
        assertThat(handler.handled.get(1).getCause()).isInstanceOf(IllegalArgumentException.class);
        // Never accepted, so nothing is in flight; failed, since it is routed.
        assertThat(writer.getInFlight()).isZero();
        assertThat(metricGroup.counterValue("requestsAccepted")).isZero();
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(2);
        assertThat(metricGroup.counterValue("numRecordsSendErrors")).isEqualTo(2);
        assertThatCode(() -> writer.flush(false)).doesNotThrowAnyException();
    }

    @Test
    void aRequestTheClientsValidationRefusesThrowsAtTheCallerUnderFailJob() {
        SingleRowRequestWriter<String> writer = writer(options().build());
        request("row-1").startFailure = new IllegalStateException("no mutations");

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(writer.getInFlight()).isZero();
    }

    @Test
    void aFailedResolutionFailsTheWriteRatherThanTheRecord() {
        RecordingHandler handler = new RecordingHandler();
        SingleRowRequestWriter<String> writer =
                writer(options().build(), serializer(), handler, (element, context) -> null);

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage("The destination resolver returned null for a record.");

        assertThat(handler.handled).isEmpty();
        assertThat(factory.created).isEmpty();
    }

    @Test
    void aRecordTheSerializerRejectsReachesTheHandlerWithoutLeasingAClient() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SingleRowRequestWriter<String> writer = writer(options().build(), handler);
        unserializable.put("bad", new IllegalArgumentException("no row key"));

        writer.write("bad", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(1);
        FailedRequest failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(TABLE);
        assertThat(failed.getOperation()).isNull();
        assertThat(failed.getRowKey()).isNull();
        assertThat(failed.getErrorMessage()).isEqualTo("The record could not be serialized.");
        assertThat(failed.getCause()).isInstanceOf(IllegalArgumentException.class);
        assertThat(metricGroup.counterValue("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(1);
        assertThat(factory.created).isEmpty();
        assertThat(writer.getInFlight()).isZero();
    }

    @Test
    void aRecordTheSerializerRejectsThrowsAtTheCallerUnderFailJob() {
        SingleRowRequestWriter<String> writer = writer(options().build());
        unserializable.put("bad", new IllegalArgumentException("no row key"));

        assertThatThrownBy(() -> writer.write("bad", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aSkippedRecordIsCountedAndLeasesNoClient() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        skipped.add("nothing");

        writer.write("nothing", TestContexts.NO_OP);

        assertThat(metricGroup.counterValue("recordsSkipped")).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsAccepted")).isZero();
        assertThat(factory.created).isEmpty();
    }

    @Test
    void aWriteAtTheInFlightCapWaitsForACompletion() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().maxInFlightRequests(1).build());
        writer.write("row-1", TestContexts.NO_OP);

        // The answer is already a queued mail: the write at the cap runs it through tryYield and
        // is then admitted. Without the wait the second request would be started at two in flight.
        request("row-1").succeed();
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(writer.getInFlight()).isEqualTo(1);
        assertThat(request("row-2").starts).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsCompleted")).isEqualTo(1);
    }

    @Test
    void aHandlerFailureInsideACompletionSurfacesOnTheNextCall() throws Exception {
        IOException refusal = new IOException("dead-letter queue unavailable");
        SingleRowRequestWriter<String> writer =
                writer(
                        options().build(),
                        element -> {
                            throw refusal;
                        });
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        assertThat(writer.getInFlight()).isZero();
        assertThatThrownBy(() -> writer.write("row-2", TestContexts.NO_OP)).isSameAs(refusal);
    }

    @Test
    void aHandlerThrowingUncheckedIsWrappedNamingTheTable() throws Exception {
        SingleRowRequestWriter<String> writer =
                writer(
                        options().build(),
                        element -> {
                            throw new IllegalStateException("boom");
                        });
        writer.write("row-1", TestContexts.NO_OP);

        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessage("The failed-request handler failed for Bigtable table " + TABLE + ".")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void theFirstTerminalFailureWinsAndLaterOnesAreStillCounted() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        request("row-1").fail(StatusCode.Code.NOT_FOUND);
        request("row-2").fail(StatusCode.Code.UNAVAILABLE);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not exist");
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(2);
        assertThat(writer.getInFlight()).isZero();
    }

    @Test
    void flushCallsTheHandlerFlushAfterTheDrain() throws Exception {
        OrderRecordingHandler handler = new OrderRecordingHandler();
        SingleRowRequestWriter<String> writer = writer(options().build(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);

        writer.flush(false);

        // The drain is what discovers this checkpoint's dead letter; a flush ahead of it would
        // checkpoint past it.
        assertThat(handler.events).containsExactly("handle", "flush");
    }

    @Test
    void closeCancelsWhatIsOutstandingAndClosesTheFactoryAndTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SingleRowRequestWriter<String> writer = writer(options().build(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        writer.close();

        assertThat(request("row-1").future.isCancelled()).isTrue();
        assertThat(request("row-2").future.isCancelled()).isTrue();
        assertThat(writer.getInFlight()).isZero();
        assertThat(metricGroup.<Integer>gaugeValue("inFlightRequests")).isZero();
        assertThat(metricGroup.<Integer>gaugeValue("activeClients")).isZero();
        assertThat(factory.closeCalls).isEqualTo(1);
        assertThat(handler.closes).isEqualTo(1);

        // The cancellations' callbacks queued mails on this un-quiesced mailbox; they find the
        // writer closed and report nothing — not a failure, not a dead letter.
        mailbox.drain();
        assertThat(metricGroup.counterValue("requestsFailed")).isZero();
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void anAnswerQueuedBeforeCloseIsANoOpAfterIt() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);
        // The answer's mail is queued but not yet run when close arrives — Flink's failure path
        // closes the operators with the mailbox open and undrained. The future is done, so the
        // close's cancel fires nothing; only the closed flag stands between the mail and the
        // ledger it would otherwise take below zero.
        request("row-1").succeed();

        writer.close();
        mailbox.drain();

        assertThat(writer.getInFlight()).isZero();
        assertThat(metricGroup.<Integer>gaugeValue("inFlightRequests")).isZero();
        assertThat(metricGroup.counterValue("requestsCompleted")).isZero();
    }

    @Test
    void closeClosesTheHandlerEvenWhenTheFactoryFails() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SingleRowRequestWriter<String> writer = writer(options().build(), handler);
        factory.closeFailure = new IOException("channel refused to close");

        assertThatThrownBy(writer::close).isSameAs(factory.closeFailure);

        assertThat(handler.closes).isEqualTo(1);
    }

    @Test
    void anAnswerArrivingAfterTheMailboxQuiescedIsLoggedAtDebugAndDropped() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture =
                LogCapture.of(SingleRowRequestWriter.class, LogCapture.Level.DEBUG)) {
            mailbox.quiesce();
            request("row-1").succeed();

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("Complete a Bigtable single-row request to " + TABLE)
                    .contains("arrived after the task mailbox was quiesced or closed");
        }
        // Nothing ran on the task thread, so the ledger is as it was; close cancels it away.
        assertThat(writer.getInFlight()).isEqualTo(1);
        writer.close();
        assertThat(writer.getInFlight()).isZero();
    }

    @Test
    void anInstanceClientIsSharedByItsTablesAndLeasedPerTable() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        destinations.put("row-1", TABLE);
        destinations.put("row-2", OTHER_TABLE);
        destinations.put("row-3", SECOND_INSTANCE);

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);

        assertThat(factory.created).containsExactly(TABLE, OTHER_TABLE, SECOND_INSTANCE);
        assertThat(request("row-2").startedOn).isSameAs(request("row-1").startedOn);
        assertThat(request("row-3").startedOn).isNotSameAs(request("row-1").startedOn);
        assertThat(writer.getActiveClients()).isEqualTo(2);
    }

    @Test
    void aFailedClientCreationFailsTheWriteNamingTheTable() {
        SingleRowRequestWriter<String> writer = writer(options().build());
        factory.createFailures.add(new IOException("no route to host"));

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to create a Bigtable client for table " + TABLE + ".")
                .hasCauseInstanceOf(IOException.class);
        assertThat(writer.getActiveClients()).isZero();
    }

    @Test
    void theInstanceCapEvictsTheLeastRecentlyUsedInstanceAfterDraining() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().maxActiveInstances(2).build());
        destinations.put("row-1", TABLE);
        destinations.put("row-2", SECOND_INSTANCE);
        destinations.put("row-3", TABLE);
        destinations.put("row-4", THIRD_INSTANCE);
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        // Touching the first instance again makes the second the least recently used.
        writer.write("row-3", TestContexts.NO_OP);
        assertThat(writer.getActiveClients()).isEqualTo(2);

        // Every answer is queued before the write, so the drain the eviction requires can finish:
        // an eviction that did not drain would release a client with requests over it.
        request("row-1").succeed();
        request("row-2").succeed();
        request("row-3").succeed();
        writer.write("row-4", TestContexts.NO_OP);

        assertThat(factory.released).containsExactly(SECOND_INSTANCE);
        assertThat(factory.created).containsExactly(TABLE, SECOND_INSTANCE, THIRD_INSTANCE);
        assertThat(metricGroup.counterValue("capacityEvictions")).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsCompleted")).isEqualTo(3);
        assertThat(writer.getActiveClients()).isEqualTo(2);
        assertThat(writer.getInFlight()).isEqualTo(1);
    }

    @Test
    void aTableOfAHeldInstanceNeedsNoEviction() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().maxActiveInstances(1).build());
        destinations.put("row-1", TABLE);
        destinations.put("row-2", OTHER_TABLE);
        writer.write("row-1", TestContexts.NO_OP);

        writer.write("row-2", TestContexts.NO_OP);

        assertThat(factory.released).isEmpty();
        assertThat(metricGroup.counterValue("capacityEvictions")).isZero();
        assertThat(writer.getInFlight()).isEqualTo(2);
    }

    @Test
    void idleTablesAreEvictedAfterANonFinalFlushOnly() throws Exception {
        SingleRowRequestWriter<String> writer =
                writer(options().destinationIdleTimeout(Duration.ofMinutes(5)).build());
        writer.write("row-1", TestContexts.NO_OP);
        request("row-1").succeed();
        writer.flush(false);
        assertThat(factory.released).isEmpty();

        clock.advance(Duration.ofMinutes(6));
        // The final flush skips the sweep: close releases everything a moment later, and a
        // sweep there would only reorder the same closes.
        writer.flush(true);
        assertThat(factory.released).isEmpty();
        assertThat(metricGroup.counterValue("idleEvictions")).isZero();

        writer.flush(false);

        assertThat(factory.released).containsExactly(TABLE);
        assertThat(metricGroup.counterValue("idleEvictions")).isEqualTo(1);
        assertThat(writer.getActiveClients()).isZero();

        // Transparent rebuild on the next record.
        writer.write("row-2", TestContexts.NO_OP);
        assertThat(factory.created).containsExactly(TABLE, TABLE);
    }

    @Test
    void aReleaseFailureDuringEvictionIsLoggedNotThrown() throws Exception {
        SingleRowRequestWriter<String> writer =
                writer(options().destinationIdleTimeout(Duration.ofMinutes(5)).build());
        writer.write("row-1", TestContexts.NO_OP);
        request("row-1").succeed();
        factory.releaseFailure = new IOException("close failed");
        clock.advance(Duration.ofMinutes(6));

        try (LogCapture capture = LogCapture.of(SingleRowRequestWriter.class)) {
            writer.flush(false);

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("idle Bigtable destinations");
        }
        assertThat(writer.getActiveClients()).isZero();
    }

    @Test
    void perTableCountersAreRegisteredOnlyWhenAsked() throws Exception {
        SingleRowRequestWriter<String> writer = writer(options().build());
        writer.write("row-1", TestContexts.NO_OP);
        request("row-1").succeed();
        mailbox.drain();

        assertThat(metricGroup.hasMetric("destination", TABLE.toString(), "recordsSend")).isFalse();
    }

    @Test
    void perTableCountersFollowTheStandardPair() throws Exception {
        // recordsSend at the hand-off and sendErrors for what reached the handler, as the batching
        // sink's per-table pair: three accepted, one rejected and routed, one fatal and not.
        SingleRowRequestWriter<String> writer =
                writer(options().perDestinationMetrics(true).build(), new RecordingHandler());
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);
        request("row-1").succeed();
        request("row-2").fail(StatusCode.Code.INVALID_ARGUMENT);
        request("row-3").fail(StatusCode.Code.NOT_FOUND);
        mailbox.drain();

        assertThat(metricGroup.counterValue("destination", TABLE.toString(), "recordsSend"))
                .isEqualTo(3);
        assertThat(metricGroup.counterValue("destination", TABLE.toString(), "sendErrors"))
                .isEqualTo(1);
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(3);
        assertThat(metricGroup.counterValue("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metricGroup.counterValue("requestsFailed")).isEqualTo(2);
    }

    private ScriptedRowRequest request(String rowKey) {
        return requests.computeIfAbsent(rowKey, ScriptedRowRequest::new);
    }

    private RowRequestSerializer<String> serializer() {
        return (element, context) -> {
            RuntimeException failure = unserializable.get(element);
            if (failure != null) {
                throw failure;
            }
            return skipped.contains(element) ? null : request(element);
        };
    }

    private static BigtableRequestOptions.Builder options() {
        return BigtableRequestOptions.builder();
    }

    private SingleRowRequestWriter<String> writer(BigtableRequestOptions options) {
        return writer(options, FailureHandler.failJob());
    }

    private SingleRowRequestWriter<String> writer(
            BigtableRequestOptions options, FailureHandler<? super FailedRequest> handler) {
        return writer(
                options,
                serializer(),
                handler,
                (element, context) -> destinations.getOrDefault(element, TABLE));
    }

    private SingleRowRequestWriter<String> writer(
            BigtableRequestOptions options,
            RowRequestSerializer<String> serializer,
            FailureHandler<? super FailedRequest> handler,
            DestinationResolver<String> resolver) {
        return new SingleRowRequestWriter<>(
                new SingleRowRequestConfig<>(
                        resolver, serializer, null, options, handler, null, null),
                factory,
                mailbox,
                metricGroup,
                clock,
                STALL_WARN_AFTER_NANOS);
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class MutableClock implements LongSupplier {

        private long nanos = Duration.ofDays(1).toNanos();

        @Override
        public long getAsLong() {
            return nanos;
        }

        void advance(Duration by) {
            nanos += by.toNanos();
        }
    }

    /** A handler that drops every request, recording what it saw and its lifecycle calls. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedRequest> handled = new ArrayList<>();
        private int closes;

        @Override
        public void handle(FailedElement element) {
            handled.add((FailedRequest) element);
        }

        @Override
        public void close() {
            closes++;
        }
    }

    /** A handler recording the order of its callbacks, for the drain-then-flush rule. */
    private static final class OrderRecordingHandler implements FailureHandler<FailedElement> {

        private final List<String> events = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            events.add("handle");
        }

        @Override
        public void flush() {
            events.add("flush");
        }
    }
}
