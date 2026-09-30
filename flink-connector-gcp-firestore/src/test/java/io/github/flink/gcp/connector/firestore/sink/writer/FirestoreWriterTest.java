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

package io.github.flink.gcp.connector.firestore.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.Timestamp;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.FirestoreMetricNames;
import io.github.flink.gcp.connector.firestore.sink.FailedWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreBulkWriterSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkBuilder;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkConfig;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static io.github.flink.gcp.connector.firestore.sink.writer.FakeFirestoreDatabaseAccess.failure;
import static io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup.NUM_BYTES_SEND;
import static io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup.NUM_RECORDS_SEND;
import static io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup.NUM_RECORDS_SEND_ERRORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class FirestoreWriterTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    private final FakeFirestoreDatabaseAccess access = new FakeFirestoreDatabaseAccess();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final List<FailedWrite> routed = new ArrayList<>();
    private final List<String> handlerEvents = new ArrayList<>();
    private BulkWriterRetryPolicy retryPolicy;

    /** The records are the writes themselves; a null record is a skip. */
    private static final FirestoreWriteSerializationSchema<FirestoreWrite> IDENTITY =
            (write, context) -> write;

    private FirestoreWriter<FirestoreWrite> writer(
            Consumer<FirestoreSinkBuilder<FirestoreWrite>> configure) throws IOException {
        FirestoreSinkBuilder<FirestoreWrite> builder =
                FirestoreSink.<FirestoreWrite>builder()
                        .database(DATABASE)
                        .serializer(IDENTITY)
                        .failedWriteHandler(collecting());
        configure.accept(builder);
        return writer(configOf(builder.build()));
    }

    private FirestoreWriter<FirestoreWrite> writer(FirestoreSinkConfig<FirestoreWrite> config)
            throws IOException {
        return new FirestoreWriter<>(
                config,
                policy -> {
                    retryPolicy = policy;
                    return access;
                },
                mailbox,
                metrics);
    }

    @SuppressWarnings("unchecked")
    private static FirestoreSinkConfig<FirestoreWrite> configOf(Sink<FirestoreWrite> sink) {
        return ((FirestoreBulkWriterSink<FirestoreWrite>) sink).getConfig();
    }

    private FailureHandler<FailedWrite> collecting() {
        return new FailureHandler<FailedWrite>() {
            @Override
            public void handle(FailedWrite element) {
                routed.add(element);
                handlerEvents.add("handle " + element.describeDestination());
            }

            @Override
            public void flush() {
                handlerEvents.add("flush");
            }

            @Override
            public void close() {
                handlerEvents.add("close");
            }
        };
    }

    private static FirestoreWrite set(String id) {
        return FirestoreWrite.set("c/" + id, Map.of("v", 1L));
    }

    private static FirestoreWrite create(String id) {
        return FirestoreWrite.create("c/" + id, Map.of("v", 1L));
    }

    private long errorClass(String code) {
        return metrics.counterValue(
                ErrorClassCounters.ERROR_CLASS_GROUP, code, ErrorClassCounters.ERRORS);
    }

    @Test
    void appliesWritesAndCountsEachRecordOnce() throws Exception {
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(set("a"), TestContexts.NO_OP);
        writer.write(set("b"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.sentBatches()).containsExactly(List.of(set("a"), set("b")));
        assertThat(metrics.counterValue(NUM_RECORDS_SEND)).isEqualTo(2);
        assertThat(metrics.counterValue(NUM_BYTES_SEND)).isPositive();
        assertThat(writer.getInFlightWrites()).isZero();
        assertThat(writer.getInFlightBytes()).isZero();
        assertThat(handlerEvents).containsExactly("flush");
    }

    @Test
    void aNullFromTheSerializerSkipsTheRecord() throws Exception {
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(null, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(metrics.counterValue(FirestoreMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
        assertThat(metrics.counterValue(NUM_RECORDS_SEND)).isZero();
        assertThat(access.sentBatches()).isEmpty();
        assertThat(routed).isEmpty();
    }

    @Test
    void aSerializerFailureIsRoutedWithoutAWrite() throws Exception {
        FirestoreWriter<FirestoreWrite> writer =
                writer(b -> b.serializer((write, context) -> FirestoreWrite.set("odd", Map.of())));

        writer.write(set("a"), TestContexts.NO_OP);

        assertThat(routed)
                .singleElement()
                .satisfies(
                        f -> {
                            assertThat(f.getWrite()).isNull();
                            assertThat(f.getCause()).isInstanceOf(IllegalArgumentException.class);
                        });
        assertThat(metrics.counterValue(NUM_RECORDS_SEND_ERRORS)).isEqualTo(1);
        assertThat(
                        metrics.hasMetric(
                                ErrorClassCounters.ERROR_CLASS_GROUP, "UNCLASSIFIED", "errors"))
                .isFalse();
    }

    @Test
    void anInvalidArgumentIsConfirmedAloneAndOnlyTheCulpritIsRouted() throws Exception {
        // A request-level refusal is reported against every write of the request; only the bad
        // write repeats it alone.
        access.respondWith(
                (write, batch) ->
                        batch.size() > 1 || write.getDocumentPath().equals("c/bad")
                                ? failure(Status.INVALID_ARGUMENT)
                                : null);
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(set("a"), TestContexts.NO_OP);
        writer.write(set("bad"), TestContexts.NO_OP);
        writer.write(set("b"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(routed)
                .singleElement()
                .satisfies(f -> assertThat(f.getWrite()).isEqualTo(set("bad")));
        assertThat(access.sentBatches())
                .containsExactly(
                        List.of(set("a"), set("bad"), set("b")),
                        List.of(set("a")),
                        List.of(set("bad")),
                        List.of(set("b")));
        assertThat(errorClass("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(metrics.counterValue(FirestoreMetricNames.WRITES_CONFIRMED_ALONE)).isEqualTo(3);
        assertThat(metrics.counterValue(NUM_RECORDS_SEND)).isEqualTo(3);
        assertThat(writer.getParkedWrites()).isZero();
        assertThat(handlerEvents)
                .containsExactly("handle projects/p/databases/(default)/documents/c/bad", "flush");
    }

    @Test
    void aParkedWriteIsIsolatedBeforeTheNextRecord() throws Exception {
        access.respondWith((write, batch) -> failure(Status.INVALID_ARGUMENT));
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxInFlightWrites(1)
                                                .build()));

        writer.write(set("a"), TestContexts.NO_OP);
        // At the cap: the wait sends the queued write, whose failure mail parks it.
        writer.write(set("b"), TestContexts.NO_OP);
        assertThat(writer.getParkedWrites()).isEqualTo(1);
        assertThat(routed).isEmpty();

        // The next record isolates the park first — and b, which its opening drain parks too.
        writer.write(set("c"), TestContexts.NO_OP);

        assertThat(routed).extracting(FailedWrite::getWrite).containsExactly(set("a"), set("b"));
        assertThat(writer.getParkedWrites()).isZero();
    }

    @Test
    void anAlreadyExistsIsRoutedForACreateWithoutConfirmationAndOutsideTheRejectionBound()
            throws Exception {
        access.respondWith((write, batch) -> failure(Status.ALREADY_EXISTS));
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxConsecutiveRejections(1)
                                                .build()));

        writer.write(create("a"), TestContexts.NO_OP);
        writer.write(create("b"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(routed)
                .extracting(FailedWrite::getWrite)
                .containsExactly(create("a"), create("b"));
        assertThat(access.sentBatches()).hasSize(1);
        assertThat(errorClass("ALREADY_EXISTS")).isEqualTo(2);
    }

    @Test
    void anAlreadyExistsForASetFailsTheJob() throws Exception {
        access.respondWith((write, batch) -> failure(Status.ALREADY_EXISTS));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(set("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("routed only for a create");
        assertThat(routed).isEmpty();
    }

    @Test
    void aNotFoundFailsTheJobAndNamesTheAlternative() throws Exception {
        access.respondWith((write, batch) -> failure(Status.NOT_FOUND));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(FirestoreWrite.update("c/a", Map.of("v", 2L)), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND")
                .hasMessageContaining("setMerge");
        assertThat(routed).isEmpty();
        assertThat(errorClass("NOT_FOUND")).isEqualTo(1);
    }

    @Test
    void aFailedPreconditionIsRoutedOnlyForAConditionalWriteUnderTheRoutingPolicy()
            throws Exception {
        access.respondWith((write, batch) -> failure(Status.FAILED_PRECONDITION));
        FirestoreWrite conditional =
                FirestoreWrite.update(
                        "c/a", Map.of("v", 2L), Timestamp.ofTimeSecondsAndNanos(1, 0));
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.preconditionFailurePolicy(
                                        PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER));

        writer.write(conditional, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(routed).extracting(FailedWrite::getWrite).containsExactly(conditional);

        writer.write(set("b"), TestContexts.NO_OP);
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without a precondition");
    }

    @Test
    void routedFailedPreconditionsStayOutsideTheRejectionBound() throws Exception {
        // A restart's replay answers FAILED_PRECONDITION for every conditional write it repeats;
        // counting them would fail the job again on each restart.
        access.respondWith((write, batch) -> failure(Status.FAILED_PRECONDITION));
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.preconditionFailurePolicy(
                                                PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER)
                                        .writerOptions(
                                                FirestoreWriterOptions.builder()
                                                        .maxConsecutiveRejections(1)
                                                        .build()));
        Timestamp time = Timestamp.ofTimeSecondsAndNanos(1, 0);

        writer.write(FirestoreWrite.delete("c/a", time), TestContexts.NO_OP);
        writer.write(FirestoreWrite.delete("c/b", time), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(routed).hasSize(2);
    }

    @Test
    void aNotFoundForAnythingButAnUpdateNamesTheDatabase() throws Exception {
        access.respondWith((write, batch) -> failure(Status.NOT_FOUND));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(set("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("projects/p/databases/(default) exists")
                .hasMessageNotContaining("setMerge");
    }

    @Test
    void aFailedPreconditionFailsTheJobUnderTheDefaultPolicy() throws Exception {
        access.respondWith((write, batch) -> failure(Status.FAILED_PRECONDITION));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(
                FirestoreWrite.delete("c/a", Timestamp.ofTimeSecondsAndNanos(1, 0)),
                TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("preconditionFailurePolicy is FAIL_JOB");
        assertThat(routed).isEmpty();
    }

    @Test
    void aTransientStatusAnywhereInTheChainIsNeverRouted() throws Exception {
        // INVALID_ARGUMENT first in the chain, UNAVAILABLE beneath it.
        access.respondWith(
                (write, batch) ->
                        new StatusRuntimeException(
                                Status.INVALID_ARGUMENT.withCause(
                                        new StatusRuntimeException(Status.UNAVAILABLE))));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        writer.write(set("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(routed).isEmpty();
    }

    @Test
    void consecutiveConfirmedRejectionsFailTheJobAfterRoutingAndAnAppliedWriteResetsTheRun()
            throws Exception {
        access.respondWith(
                (write, batch) ->
                        write.getDocumentPath().startsWith("c/bad")
                                ? failure(Status.INVALID_ARGUMENT)
                                : null);
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxConsecutiveRejections(2)
                                                .build()));

        writer.write(set("bad1"), TestContexts.NO_OP);
        writer.flush(false);
        writer.write(set("good"), TestContexts.NO_OP);
        writer.flush(false);
        writer.write(set("bad2"), TestContexts.NO_OP);
        writer.flush(false);
        writer.write(set("bad3"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)");
        assertThat(routed).hasSize(3);
    }

    @Test
    void theBulkWriterIsReplacedBeforeFailedWritesCouldFillItsPendingSlots() throws Exception {
        access.respondWith((write, batch) -> failure(Status.ALREADY_EXISTS));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        // One failed write short of the library's ceiling: nothing is replaced yet.
        for (int i = 0; i < FirestoreWriter.PENDING_OPERATION_LIMIT - 1; i++) {
            writer.write(create("a" + i), TestContexts.NO_OP);
        }
        writer.flush(false);
        assertThat(access.replacements()).isZero();

        access.respondWith((write, batch) -> null);
        writer.write(create("b"), TestContexts.NO_OP);
        // b is still in flight, so the slots in use reach the ceiling at c: the writer drains b
        // first (the fake refuses a replacement with a write unanswered) and then replaces.
        writer.write(create("c"), TestContexts.NO_OP);
        assertThat(access.replacements()).isEqualTo(1);
        assertThat(access.strandedWrites()).isZero();

        // The count starts again at zero on the fresh BulkWriter.
        writer.write(create("d"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.replacements()).isEqualTo(1);
        assertThat(access.strandedWrites()).isZero();
        assertThat(metrics.counterValue(FirestoreMetricNames.BULK_WRITERS_REPLACED)).isEqualTo(1);
    }

    @Test
    void aFullWriterSendsWhatTheClientHoldsInsteadOfWaitingForever() throws Exception {
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxInFlightWrites(2)
                                                .build()));

        writer.write(set("a"), TestContexts.NO_OP);
        writer.write(set("b"), TestContexts.NO_OP);
        // At the cap with both writes still unsent: only a send can free capacity.
        writer.write(set("c"), TestContexts.NO_OP);

        assertThat(access.sentBatches()).containsExactly(List.of(set("a"), set("b")));
        assertThat(access.queuedWrites()).isEqualTo(1);
    }

    @Test
    void theWriterSendsBeforeARequestWouldPassItsByteBudget() throws Exception {
        String fourMebibytes = "x".repeat(4 * 1024 * 1024);
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxInFlightBytes(Long.MAX_VALUE)
                                                .build()));

        for (String id : List.of("a", "b", "c")) {
            writer.write(
                    FirestoreWrite.set("c/" + id, Map.of("v", fourMebibytes)), TestContexts.NO_OP);
        }
        writer.flush(false);

        assertThat(access.sentBatches()).extracting(List::size).containsExactly(2, 1);
    }

    @Test
    void aSynchronousRefusalFailsTheJobWithoutCountingTheWrite() throws Exception {
        access.refuseNextSubmit(new IllegalArgumentException("cannot encode"));
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});

        assertThatThrownBy(() -> writer.write(set("a"), TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("synchronously");
        assertThat(writer.getInFlightWrites()).isZero();
        assertThat(metrics.counterValue(NUM_RECORDS_SEND)).isZero();
        assertThat(routed).isEmpty();
    }

    @Test
    void closeZeroesTheGaugesClosesEverythingAndDropsLateCompletions() throws Exception {
        access.holdAnswers();
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});
        writer.write(set("a"), TestContexts.NO_OP);
        access.sendOutstanding();
        assertThat(writer.getInFlightWrites()).isEqualTo(1);

        try (LogCapture capture = LogCapture.of(FirestoreWriter.class, LogCapture.Level.DEBUG)) {
            mailbox.quiesce();
            writer.close();
            access.answerHeld();

            assertThat(capture.getMessages())
                    .anySatisfy(m -> assertThat(m).contains("after the writer closed"));
        }
        assertThat(writer.getInFlightWrites()).isZero();
        assertThat(writer.getInFlightBytes()).isZero();
        assertThat(access.isClosed()).isTrue();
        assertThat(handlerEvents).containsExactly("close");
    }

    @Test
    void theByteBoundHoldsBackTheNextWriteAndSendsWhatTheClientHolds() throws Exception {
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxInFlightBytes(1)
                                                .build()));

        writer.write(set("a"), TestContexts.NO_OP);
        // a alone fills the byte bound, so b waits for it — and a is still unsent.
        writer.write(set("b"), TestContexts.NO_OP);

        assertThat(access.sentBatches()).containsExactly(List.of(set("a")));
        assertThat(access.queuedWrites()).isEqualTo(1);
    }

    @Test
    void aRoutedWriteUnderTheDefaultHandlerFailsTheNextFlush() throws Exception {
        access.respondWith((write, batch) -> failure(Status.ALREADY_EXISTS));
        FirestoreWriter<FirestoreWrite> writer =
                writer(b -> b.failedWriteHandler(FailureHandler.failJob()));

        writer.write(create("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(metrics.counterValue(NUM_RECORDS_SEND_ERRORS)).isEqualTo(1);
        // First failure wins: the next call rethrows the same one.
        assertThatThrownBy(() -> writer.write(set("b"), TestContexts.NO_OP))
                .isInstanceOf(IOException.class);
        assertThat(access.sentBatches()).hasSize(1);
    }

    @Test
    void theGaugesReadInFlightAndParkedWrites() throws Exception {
        access.respondWith((write, batch) -> failure(Status.INVALID_ARGUMENT));
        FirestoreWriter<FirestoreWrite> writer =
                writer(
                        b ->
                                b.writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .maxInFlightWrites(1)
                                                .build()));

        writer.write(set("a"), TestContexts.NO_OP);
        assertThat(metrics.<Integer>gaugeValue(FirestoreMetricNames.IN_FLIGHT_WRITES)).isEqualTo(1);
        assertThat(metrics.<Long>gaugeValue(FirestoreMetricNames.IN_FLIGHT_BYTES)).isPositive();

        // The wait for b sends a, whose refusal parks it.
        writer.write(set("b"), TestContexts.NO_OP);
        assertThat(metrics.<Integer>gaugeValue(FirestoreMetricNames.PARKED_WRITES)).isEqualTo(1);
        assertThat(metrics.<Integer>gaugeValue(FirestoreMetricNames.IN_FLIGHT_WRITES)).isEqualTo(1);
    }

    @Test
    void aStalledWaitWarnsOncePerPeriodAndStillCompletes() throws Exception {
        AtomicLong clock = new AtomicLong();
        long period = 100;
        access.holdAnswers();
        FirestoreWriter<FirestoreWrite> writer =
                new FirestoreWriter<>(
                        configOf(
                                FirestoreSink.<FirestoreWrite>builder()
                                        .database(DATABASE)
                                        .serializer(IDENTITY)
                                        .failedWriteHandler(collecting())
                                        .build()),
                        policy -> access,
                        mailbox,
                        metrics,
                        clock::get,
                        period);
        writer.write(set("a"), TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(FirestoreWriter.class)) {
            int sends = access.sendOutstandingCalls();
            CompletableFuture<Void> flush = runAsync(() -> writer.flush(false));
            // Every empty pass of the drain asks the client to send, so a few more requests mean
            // the drain has taken its start time and is waiting.
            awaitCondition(
                    () -> access.sendOutstandingCalls() >= sends + 3,
                    "the drain never asked the client to send what it holds");
            clock.addAndGet(period + 1);
            awaitMessages(capture, 1);
            assertThat(capture.getMessages().get(0))
                    .contains("No Firestore write to projects/p/databases/(default)");

            // Just short of a period after that warning: a pass that read the clock after this
            // step has finished once the drain asks the client to send twice more.
            clock.addAndGet(period - 1);
            int afterShortStep = access.sendOutstandingCalls();
            awaitCondition(
                    () -> access.sendOutstandingCalls() >= afterShortStep + 2,
                    "the drain stopped passing");
            assertThat(capture.getMessages()).as("a warning within one period").hasSize(1);

            // A full period after it: the next pass warns again.
            clock.addAndGet(1);
            awaitMessages(capture, 2);

            access.answerHeld();
            flush.get(10, TimeUnit.SECONDS);
        }
        assertThat(writer.getInFlightWrites()).isZero();
    }

    @Test
    void aWaitInterruptedWhileTheClientIsSilentThrowsInterruptedException() throws Exception {
        access.holdAnswers();
        FirestoreWriter<FirestoreWrite> writer = writer(b -> {});
        writer.write(set("a"), TestContexts.NO_OP);

        AtomicReference<Thread> thread = new AtomicReference<>();
        CompletableFuture<Void> flush =
                runAsync(
                        () -> {
                            thread.set(Thread.currentThread());
                            writer.flush(false);
                        });
        awaitCondition(() -> thread.get() != null, "the flush thread never started");
        thread.get().interrupt();

        assertThatThrownBy(() -> flush.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(InterruptedException.class);
    }

    @Test
    void aDeserializedNonPositiveBoundIsRefused() throws Exception {
        for (String name : List.of("maxInFlightWrites", "maxInFlightBytes")) {
            FirestoreWriterOptions forged = FirestoreWriterOptions.builder().build();
            Field field = FirestoreWriterOptions.class.getDeclaredField(name);
            field.setAccessible(true);
            if (field.getType() == int.class) {
                field.setInt(forged, 0);
            } else {
                field.setLong(forged, 0);
            }

            assertThatThrownBy(() -> writer(b -> b.writerOptions(forged)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(name);
        }
        assertThat(FirestoreWriterOptions.defaults().getMaxInFlightBytes())
                .isEqualTo(FirestoreWriterOptions.DEFAULT_MAX_IN_FLIGHT_BYTES);
    }

    /** A task the writer's thread would run, driven from a thread of its own. */
    @FunctionalInterface
    private interface WriterTask {
        void run() throws Exception;
    }

    private static CompletableFuture<Void> runAsync(WriterTask task) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                task.run();
                                result.complete(null);
                            } catch (Throwable e) {
                                result.completeExceptionally(e);
                            }
                        });
        thread.setDaemon(true);
        thread.start();
        return result;
    }

    /**
     * Waits, sleeping rather than spinning so that a test timeout's interrupt ends it, until the
     * condition holds; fails naming what never happened.
     */
    private static void awaitCondition(BooleanSupplier condition, String failure)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(failure);
            }
            Thread.sleep(1);
        }
    }

    private static void awaitMessages(LogCapture capture, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (capture.getMessages().size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(capture.getMessages()).hasSizeGreaterThanOrEqualTo(count);
    }

    @Test
    void theRetryPolicyCountsRetriesOnTheWritersMetrics() throws Exception {
        writer(b -> {});

        assertThat(retryPolicy.onError(failure(Status.UNAVAILABLE))).isTrue();
        assertThat(retryPolicy.onError(failure(Status.INVALID_ARGUMENT))).isFalse();

        assertThat(metrics.counterValue(FirestoreMetricNames.WRITES_RETRIED)).isEqualTo(1);
    }

    @Test
    void aDeserializedInFlightCapOutsideTheLibrarysCeilingIsRefused() throws Exception {
        // Forged on a fresh builder result, never on defaults(): that singleton is shared by the
        // whole surefire fork (ADR-0002).
        FirestoreWriterOptions forged = FirestoreWriterOptions.builder().build();
        Field field = FirestoreWriterOptions.class.getDeclaredField("maxInFlightWrites");
        field.setAccessible(true);
        field.setInt(forged, FirestoreWriter.PENDING_OPERATION_LIMIT + 1);

        assertThatThrownBy(() -> writer(b -> b.writerOptions(forged)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightWrites");
        assertThat(FirestoreWriterOptions.defaults().getMaxInFlightWrites())
                .isEqualTo(FirestoreWriterOptions.DEFAULT_MAX_IN_FLIGHT_WRITES);
    }
}
