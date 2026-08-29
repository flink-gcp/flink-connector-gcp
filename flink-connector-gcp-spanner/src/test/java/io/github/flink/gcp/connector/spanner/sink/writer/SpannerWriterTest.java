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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;
import io.github.flink.gcp.connector.spanner.sink.FailedMutation;
import io.github.flink.gcp.connector.spanner.sink.SpannerMutationsSink;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerWriter}. */
@Timeout(30)
class SpannerWriterTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "i", "d");

    private final FakeSpannerDatabaseAccess access = new FakeSpannerDatabaseAccess();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    // ---------------------------------------------------------------- serializer contract

    @Test
    void aNullFromTheSerializerSkipsTheRecordWithoutSendingAnything() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer((element, context) -> null, SpannerWriterOptions.defaults(), handler);

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).isEmpty();
        assertThat(handler.handled).isEmpty();
        assertThat(metrics.counterValue(SpannerMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
        // A skip is neither a send nor a failure — that is the whole contract.
        assertThat(metrics.counterValue("numRecordsSend")).isZero();
        assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
    }

    @Test
    void aSerializerFailureIsRoutedWithNoMutationToCarry() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        (element, context) -> {
                            throw new IOException("bad record");
                        },
                        SpannerWriterOptions.defaults(),
                        handler);

        writer.write("a", TestContexts.NO_OP);

        assertThat(access.requests()).isEmpty();
        assertThat(handler.handled).hasSize(1);
        FailedMutation failure = handler.handled.get(0);
        assertThat(failure.getMutation()).isNull();
        assertThat(failure.getCause()).hasMessage("bad record");
        assertThat(metrics.counterValue("numRecordsSendErrors")).isEqualTo(1);
        // No status to classify, so nothing lands in the error-class breakdown.
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    // ---------------------------------------------------------------- batching

    @Test
    void holdsMutationsUntilTheBarrier() throws Exception {
        SinkWriter<String> writer = writer(SpannerWriterOptions.defaults());

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        assertThat(access.requests()).isEmpty();

        writer.flush(false);

        assertThat(access.requests()).hasSize(1);
        assertThat(access.requests().get(0)).hasSize(2);
    }

    @Test
    void sendsAsSoonAsOneMoreMutationWouldNotFitTheRowLimit() throws Exception {
        SinkWriter<String> writer =
                writer(SpannerWriterOptions.builder().maxBatchMutations(2).build());

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.write("c", TestContexts.NO_OP);

        // The third record is what triggers the send, and it stays behind for the next batch —
        // so a batch never exceeds the limit rather than exceeding it by one.
        assertThat(access.requests()).hasSize(1);
        assertThat(access.requests().get(0)).hasSize(2);

        writer.flush(false);
        assertThat(access.requests()).hasSize(2);
        assertThat(access.requests().get(1)).hasSize(1);
    }

    @Test
    void sendsAsSoonAsOneMoreMutationWouldNotFitTheCellLimit() throws Exception {
        // Two indexes cover Name, so each mutation costs 1 (Id) + 3 (Name) = 4 cells — which is
        // the point of counting index entries at all: three of these rows are 12 cells, not 6.
        access.withCellWeights(
                CellWeights.builder()
                        .indexColumn("Orders", "Name", "ByName")
                        .indexColumn("Orders", "Name", "ByNameAndId")
                        .build());
        SinkWriter<String> writer = writer(SpannerWriterOptions.builder().maxBatchCells(8).build());

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.write("c", TestContexts.NO_OP);

        assertThat(access.requests()).hasSize(1);
        assertThat(access.requests().get(0)).hasSize(2);
    }

    @Test
    void sendsAsSoonAsOneMoreMutationWouldNotFitTheByteLimit() throws Exception {
        SinkWriter<String> writer =
                writer(SpannerWriterOptions.builder().maxBatchBytes(20).build());

        // Each record's mutation carries an 8-byte key and its name as a string value, so two of
        // these are 32 bytes and the second one cannot join the first.
        writer.write("aaaaaaaa", TestContexts.NO_OP);
        writer.write("bbbbbbbb", TestContexts.NO_OP);

        assertThat(access.requests()).hasSize(1);
        assertThat(access.requests().get(0)).hasSize(1);
        assertThat(names(access.requests().get(0))).containsExactly("aaaaaaaa");
    }

    @Test
    void aMutationTooLargeForTheLimitIsStillSentAlone() throws Exception {
        SinkWriter<String> writer = writer(SpannerWriterOptions.builder().maxBatchBytes(1).build());

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        // Refusing it here would be this connector inventing a limit; Spanner's own refusal names
        // the real one far better.
        assertThat(access.requests()).hasSize(1);
        assertThat(access.requests().get(0)).hasSize(1);
    }

    @Test
    void anEmptyBarrierSendsNothing() throws Exception {
        SinkWriter<String> writer = writer(SpannerWriterOptions.defaults());

        writer.flush(false);

        assertThat(access.requests()).isEmpty();
    }

    // ---------------------------------------------------------------- per-mutation failures

    @Test
    void routesOnlyTheMutationTheServiceRefusedAndKeepsTheRestOfTheBatch() throws Exception {
        access.script(
                FakeSpannerDatabaseAccess.Response.allApplied()
                        .refused(1, StatusCode.Code.INVALID_ARGUMENT, "bad value"));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(serializer(), SpannerWriterOptions.defaults(), handler);

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.write("c", TestContexts.NO_OP);
        writer.flush(false);

        // One request only: a routed refusal is settled, so nothing is re-sent.
        assertThat(access.requests()).hasSize(1);
        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getMutation().asMap().get("Name").getString())
                .isEqualTo("b");
        assertThat(handler.handled.get(0).getErrorMessage())
                .contains("INVALID_ARGUMENT")
                .contains("bad value");
        assertThat(metrics.counterValue("numRecordsSendErrors")).isEqualTo(1);
        assertThat(metrics.counterValue("errorClass", "INVALID_ARGUMENT", "errors")).isEqualTo(1);
    }

    @Test
    void routesAReplayedInsertTheServiceSaysIsAlreadyThere() throws Exception {
        access.script(
                FakeSpannerDatabaseAccess.Response.allApplied()
                        .refused(0, StatusCode.Code.ALREADY_EXISTS, "row exists"));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(serializer(), SpannerWriterOptions.defaults(), handler);

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getErrorMessage()).contains("ALREADY_EXISTS");
    }

    @Test
    void failsTheJobOnAStatusThatIsNotAboutThatOneMutation() {
        access.script(
                FakeSpannerDatabaseAccess.Response.allApplied()
                        .refused(0, StatusCode.Code.NOT_FOUND, "Table not found: Orders"));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(serializer(), SpannerWriterOptions.defaults(), handler);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", TestContexts.NO_OP);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND")
                .hasMessageContaining("Orders");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void doesNotRouteAStatusThatOnlyLooksDataShaped() {
        // FAILED_PRECONDITION covers a NOT NULL violation and a database that is not ready yet.
        // A dropping policy handed both would turn the second into silent data loss, so the job
        // fails and the record stays visible.
        access.script(
                FakeSpannerDatabaseAccess.Response.allApplied()
                        .refused(0, StatusCode.Code.FAILED_PRECONDITION, "must not be NULL"));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(serializer(), SpannerWriterOptions.defaults(), handler);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", TestContexts.NO_OP);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("FAILED_PRECONDITION");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void aFatalGroupStatusKeepsTheRequestFailureAsWell() {
        // Both happened. The group status is the better message, but discarding the transport
        // failure entirely would leave an operator with half the story.
        access.script(
                FakeSpannerDatabaseAccess.Response.nothingReported()
                        .refused(0, StatusCode.Code.NOT_FOUND, "Table not found: orders")
                        .andThenFailing(spannerException(ErrorCode.UNAVAILABLE)));
        SinkWriter<String> writer = writer(fastRetries().build());

        assertThatThrownBy(
                        () -> {
                            writer.write("a", TestContexts.NO_OP);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND")
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .anySatisfy(
                                                suppressed ->
                                                        assertThat(suppressed)
                                                                .hasMessageContaining(
                                                                        "UNAVAILABLE")));
    }

    @Test
    void ignoresAGroupIndexOutsideTheRequestItSent() throws Exception {
        // Defence in depth against a service reporting a stale index. The stray status decides
        // nothing, so the mutation stays undecided and is re-sent — rather than being routed to a
        // dead-letter queue over an outcome that was never about it.
        access.script(
                FakeSpannerDatabaseAccess.Response.nothingReported()
                        .refused(99, StatusCode.Code.INVALID_ARGUMENT, "not ours"));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(serializer(), fastRetries().build(), handler);

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).hasSize(2);
        assertThat(names(access.requests().get(1))).containsExactly("a");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void routesAConstraintViolationWhenTheJobAsksForIt() throws Exception {
        access.script(
                FakeSpannerDatabaseAccess.Response.allApplied()
                        .refused(0, StatusCode.Code.FAILED_PRECONDITION, "must not be NULL"));
        RecordingHandler handler = new RecordingHandler();
        Sink<String> sink =
                SpannerSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .failedMutationHandler(handler)
                        .constraintViolationPolicy(
                                ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER)
                        .build();
        SinkWriter<String> writer =
                ((SpannerMutationsSink<String>) sink).createWriter(access, weights(), metrics);

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getErrorMessage())
                .contains("FAILED_PRECONDITION")
                .contains("must not be NULL");
    }

    // ---------------------------------------------------------------- retries

    @Test
    void resendsOnlyWhatTheServiceLeftUndecided() throws Exception {
        access.script(
                FakeSpannerDatabaseAccess.Response.nothingReported()
                        .applied(0)
                        .refused(1, StatusCode.Code.ABORTED, "contention"));
        SinkWriter<String> writer = writer(fastRetries().build());

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.write("c", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).hasSize(2);
        assertThat(access.requests().get(0)).hasSize(3);
        // The applied one is not re-sent; the aborted one and the unreported one are.
        assertThat(names(access.requests().get(1))).containsExactly("b", "c");
        assertThat(metrics.counterValue(SpannerMetricNames.MUTATIONS_RETRIED)).isEqualTo(2);
        assertThat(metrics.counterValue(SpannerMetricNames.BATCHES_SENT)).isEqualTo(2);
    }

    @Test
    void resendsTheUndecidedAfterAStreamThatFailedPartWayThrough() throws Exception {
        access.script(
                FakeSpannerDatabaseAccess.Response.nothingReported()
                        .applied(0)
                        .andThenFailing(spannerException(ErrorCode.UNAVAILABLE)));
        SinkWriter<String> writer = writer(fastRetries().build());

        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).hasSize(2);
        assertThat(names(access.requests().get(1))).containsExactly("b");
    }

    @Test
    void aBatchWriteDeadlineRemainsAConnectorOwnedRetry() throws Exception {
        access.script(failing(ErrorCode.DEADLINE_EXCEEDED));
        SinkWriter<String> writer = writer(fastRetries().build());

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).hasSize(2);
        assertThat(metrics.counterValue(SpannerMetricNames.BATCHES_SENT)).isEqualTo(2);
        assertThat(metrics.counterValue(SpannerMetricNames.MUTATIONS_RETRIED)).isEqualTo(1);
        assertThat(metrics.counterValue("errorClass", "DEADLINE_EXCEEDED", "errors")).isEqualTo(1);
    }

    @Test
    void countsEachRecordAsOneSendHoweverOftenItIsRetried() throws Exception {
        access.script(
                FakeSpannerDatabaseAccess.Response.nothingReported()
                        .andThenFailing(spannerException(ErrorCode.UNAVAILABLE)),
                FakeSpannerDatabaseAccess.Response.nothingReported()
                        .andThenFailing(spannerException(ErrorCode.UNAVAILABLE)));
        SinkWriter<String> writer = writer(fastRetries().build());

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).hasSize(3);
        // Three attempts, one record. numRecordsSend is a record count, not an attempt count.
        assertThat(metrics.counterValue("numRecordsSend")).isEqualTo(1);
        assertThat(metrics.counterValue(SpannerMetricNames.MUTATIONS_RETRIED)).isEqualTo(2);
    }

    @Test
    void failsTheJobWhenTheRetryBudgetIsSpent() {
        access.script(
                failing(ErrorCode.UNAVAILABLE),
                failing(ErrorCode.UNAVAILABLE),
                failing(ErrorCode.UNAVAILABLE));
        SinkWriter<String> writer = writer(fastRetries().recoveryMaxAttempts(3).build());

        assertThatThrownBy(
                        () -> {
                            writer.write("a", TestContexts.NO_OP);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Giving up on 1 Spanner mutation(s) after 3 attempt(s)")
                .hasMessageContaining("recoveryMaxAttempts");
        assertThat(access.requests()).hasSize(3);
    }

    @Test
    void failsTheJobImmediatelyOnARequestFailureThatWillNotClear() {
        access.script(failing(ErrorCode.PERMISSION_DENIED));
        SinkWriter<String> writer = writer(fastRetries().build());

        assertThatThrownBy(
                        () -> {
                            writer.write("a", TestContexts.NO_OP);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("PERMISSION_DENIED");
        // Not retried: one request, and the budget was never touched.
        assertThat(access.requests()).hasSize(1);
        assertThat(metrics.counterValue(SpannerMetricNames.MUTATIONS_RETRIED)).isZero();
    }

    @Test
    void neverRoutesARequestLevelRefusal() {
        // The same status is row-level on one group. On the whole request it names no mutation, so
        // dropping every mutation over it would discard records the service never looked at.
        access.script(failing(ErrorCode.INVALID_ARGUMENT));
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(serializer(), fastRetries().build(), handler);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", TestContexts.NO_OP);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class);
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void countsARetriedFailureInTheErrorClassBreakdown() throws Exception {
        access.script(failing(ErrorCode.UNAVAILABLE));
        SinkWriter<String> writer = writer(fastRetries().build());

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        // Unlike the sibling sinks, this connector sees its own retries — so a transient status it
        // recovered from is still counted, which is what makes the breakdown a retry-cause view.
        assertThat(metrics.counterValue("errorClass", "UNAVAILABLE", "errors")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- lifecycle and metrics

    @Test
    void flushesTheFailureHandlerAfterTheWritePath() throws Exception {
        OrderRecordingHandler handler = new OrderRecordingHandler();
        access.script(
                FakeSpannerDatabaseAccess.Response.allApplied()
                        .refused(0, StatusCode.Code.INVALID_ARGUMENT, "bad"));
        SinkWriter<String> writer = writer(serializer(), SpannerWriterOptions.defaults(), handler);

        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.events).containsExactly("handle", "flush");
    }

    @Test
    void closeDropsWhatIsBufferedAndReleasesEverything() throws Exception {
        OrderRecordingHandler handler = new OrderRecordingHandler();
        SinkWriter<String> writer = writer(serializer(), SpannerWriterOptions.defaults(), handler);

        writer.write("a", TestContexts.NO_OP);
        writer.close();

        // No flush on close: the job is already coming down, and nothing buffered was promised to
        // a checkpoint, so it is replayed from the source instead.
        assertThat(access.requests()).isEmpty();
        assertThat(access.closeCalls()).isEqualTo(1);
        assertThat(handler.events).containsExactly("close");
        assertThat(metrics.<Integer>gaugeValue(SpannerMetricNames.BUFFERED_MUTATIONS)).isZero();
    }

    @Test
    void reportsWhatTheBatchIsHolding() throws Exception {
        access.withCellWeights(
                CellWeights.builder().indexColumn("Orders", "Name", "ByName").build());
        SinkWriter<String> writer = writer(SpannerWriterOptions.defaults());

        writer.write("abc", TestContexts.NO_OP);

        assertThat(metrics.<Integer>gaugeValue(SpannerMetricNames.BUFFERED_MUTATIONS)).isEqualTo(1);
        // Id costs one cell, Name one plus its index.
        assertThat(metrics.<Integer>gaugeValue(SpannerMetricNames.BUFFERED_CELLS)).isEqualTo(3);
        // An 8-byte key and a three-character name.
        assertThat(metrics.<Long>gaugeValue(SpannerMetricNames.BUFFERED_BYTES)).isEqualTo(11L);

        writer.flush(false);

        assertThat(metrics.<Integer>gaugeValue(SpannerMetricNames.BUFFERED_MUTATIONS)).isZero();
        assertThat(metrics.<Integer>gaugeValue(SpannerMetricNames.BUFFERED_CELLS)).isZero();
        assertThat(metrics.<Long>gaugeValue(SpannerMetricNames.BUFFERED_BYTES)).isZero();
        // The same estimate the gauge carried before the flush, now as the sent byte count — which
        // is the only place the size estimator is checked through the writer rather than alone.
        assertThat(metrics.counterValue("numBytesSend")).isEqualTo(11L);
        assertThat(metrics.counterValue("numRecordsSend")).isEqualTo(1);
    }

    @Test
    void rejectsOptionsThatCouldNotHaveComeFromTheBuilder() {
        // Java deserialization does not run the builder, so the writer re-checks what a rebuilt
        // options object could otherwise smuggle past it.
        SpannerWriterOptions forged =
                forge(SpannerWriterOptions.builder().build(), "maxBatchMutations");

        assertThatThrownBy(() -> writer(serializer(), forged, new RecordingHandler()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchMutations");
        // The shared defaults singleton must survive the forge — see ADR-0002.
        assertThat(SpannerWriterOptions.defaults().getMaxBatchMutations()).isEqualTo(500);
    }

    // ---------------------------------------------------------------- helpers

    private SinkWriter<String> writer(SpannerWriterOptions options) {
        return writer(serializer(), options, new RecordingHandler());
    }

    private SinkWriter<String> writer(
            SpannerMutationSerializationSchema<String> serializer,
            SpannerWriterOptions options,
            FailureHandler<? super FailedMutation> handler) {
        Sink<String> sink =
                SpannerSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer)
                        .writerOptions(options)
                        .failedMutationHandler(handler)
                        .build();
        return ((SpannerMutationsSink<String>) sink).createWriter(access, weights(), metrics);
    }

    private CellWeights weights() {
        return access.readCellWeights();
    }

    private static SpannerWriterOptions.Builder fastRetries() {
        return SpannerWriterOptions.builder()
                .recoveryInitialBackoff(Duration.ofMillis(1))
                .recoveryMaxBackoff(Duration.ofMillis(1));
    }

    private static SpannerMutationSerializationSchema<String> serializer() {
        return (element, context) ->
                Mutation.newInsertOrUpdateBuilder("Orders")
                        .set("Id")
                        .to((long) element.hashCode())
                        .set("Name")
                        .to(element)
                        .build();
    }

    private static FakeSpannerDatabaseAccess.Response failing(ErrorCode code) {
        return FakeSpannerDatabaseAccess.Response.nothingReported()
                .andThenFailing(spannerException(code));
    }

    private static SpannerException spannerException(ErrorCode code) {
        return SpannerExceptionFactory.newSpannerException(code, "scripted " + code);
    }

    private static List<String> names(List<Mutation> mutations) {
        List<String> names = new ArrayList<>();
        for (Mutation mutation : mutations) {
            names.add(mutation.asMap().get("Name").getString());
        }
        return names;
    }

    /** Reflectively breaks one field of a freshly built options object. */
    private static SpannerWriterOptions forge(SpannerWriterOptions options, String field) {
        try {
            java.lang.reflect.Field target = SpannerWriterOptions.class.getDeclaredField(field);
            target.setAccessible(true);
            target.setInt(options, 0);
            return options;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Collects what the writer routed. */
    private static final class RecordingHandler implements FailureHandler<FailedMutation> {

        private static final long serialVersionUID = 1L;

        private final transient List<FailedMutation> handled = new ArrayList<>();

        @Override
        public void handle(FailedMutation element) {
            handled.add(element);
        }
    }

    /** Records the order of the handler's lifecycle calls. */
    private static final class OrderRecordingHandler implements FailureHandler<FailedElement> {

        private static final long serialVersionUID = 1L;

        private final transient List<String> events = new ArrayList<>();

        @Override
        public void handle(@Nullable FailedElement element) {
            events.add("handle");
        }

        @Override
        public void flush() {
            events.add("flush");
        }

        @Override
        public void close() {
            events.add("close");
        }
    }
}
