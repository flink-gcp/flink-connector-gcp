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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.failure.FailureHandlerContext;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.FailedMessage;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the per-message failure policy of {@link PubSubWriter}.
 *
 * <p>Kept apart from {@link PubSubWriterTest}, which is the regression guard for the default policy
 * — that class must keep passing untouched, since {@code failJob()} is today's capture-and-rethrow.
 *
 * <p>Timed out as a class for the reason {@link PubSubWriterTest} records: the fake mailbox blocks
 * on an empty mailbox exactly as the real one does.
 */
@Timeout(30)
class PubSubWriterFailureHandlerTest {

    private static final String PROJECT = "test-project";

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private static final RetrySchedule UNUSED_RECOVERY = new RetrySchedule(1, 1, 1, 0);

    private static final RetrySchedule FAST_SCHEDULE = new RetrySchedule(1, 1, 5, 0);

    private static final TopicDestination ORDERED_TOPIC = TopicDestination.of(PROJECT, "ordered");

    private final FakePublisherFactory factory = new FakePublisherFactory();
    private final FakeTopicAdmin admin = new FakeTopicAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    /** Records what it is handed, and optionally fails. */
    private static final class RecordingHandler implements FailureHandler<FailedMessage> {

        private static final long serialVersionUID = 1L;

        private final transient List<FailedMessage> handled = new ArrayList<>();
        private final transient List<String> events = new ArrayList<>();
        private transient Exception failure;
        private transient int openCalls;
        private transient int flushCalls;
        private transient int closeCalls;

        @Override
        public void open(FailureHandlerContext context) {
            openCalls++;
        }

        @Override
        public void handle(FailedMessage message) throws IOException {
            handled.add(message);
            events.add("handle");
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
        }

        @Override
        public void flush() {
            flushCalls++;
            events.add("flush");
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private final RecordingHandler handler = new RecordingHandler();

    private PubSubWriter<String> newWriter() {
        return newWriter(PubSubSerializationSchema.payload(new SimpleStringSchema()));
    }

    private PubSubWriter<String> newWriter(PubSubSerializationSchema<String> serializer) {
        return newWriter(serializer, CreateDisposition.CREATE_IF_NEEDED);
    }

    private PubSubWriter<String> newWriter(
            PubSubSerializationSchema<String> serializer, CreateDisposition disposition) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> TopicDestination.of(PROJECT, element),
                        serializer,
                        PubSubPublisherOptions.defaults(),
                        handler,
                        disposition),
                factory,
                admin,
                mailbox,
                metrics,
                UNUSED_RECOVERY);
    }

    /**
     * A writer publishing every record to one topic without ordering, so several records land in
     * one publisher — the fixture for the co-batched rejection tests, where the element has to be
     * the payload rather than the topic name.
     */
    private PubSubWriter<String> newFixedTopicWriter() {
        return newFixedTopicWriter(PubSubPublisherOptions.defaults());
    }

    private PubSubWriter<String> newFixedTopicWriter(PubSubPublisherOptions options) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> topic("fixed"),
                        PubSubSerializationSchema.payload(new SimpleStringSchema()),
                        options,
                        handler,
                        CreateDisposition.CREATE_IF_NEEDED),
                factory,
                admin,
                mailbox,
                metrics,
                FAST_SCHEDULE);
    }

    /**
     * A writer publishing every record to one topic with message ordering enabled, taking the
     * ordering key from the record's prefix before {@code ':'} — the fixture {@link
     * PubSubWriterAutoCreationTest} uses, so the two ordering suites read alike.
     *
     * <p>The recovery schedule is a real one rather than {@link #UNUSED_RECOVERY}: these tests do
     * reach the repair.
     */
    private PubSubWriter<String> newOrderingWriter(CreateDisposition disposition) {
        return newOrderingWriter(
                PubSubSerializationSchema.payload(new SimpleStringSchema())
                        .withOrderingKey(element -> element.split(":")[0]),
                disposition);
    }

    private PubSubWriter<String> newOrderingWriter(
            PubSubSerializationSchema<String> serializer, CreateDisposition disposition) {
        return newOrderingWriter(serializer, disposition, FAST_SCHEDULE);
    }

    private PubSubWriter<String> newOrderingWriter(
            PubSubSerializationSchema<String> serializer,
            CreateDisposition disposition,
            RetrySchedule recoverySchedule) {
        return newOrderingWriter(
                serializer,
                PubSubPublisherOptions.builder().enableMessageOrdering(true).build(),
                disposition,
                recoverySchedule);
    }

    private PubSubWriter<String> newOrderingWriter(
            PubSubSerializationSchema<String> serializer,
            PubSubPublisherOptions options,
            CreateDisposition disposition,
            RetrySchedule recoverySchedule) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> ORDERED_TOPIC,
                        serializer,
                        options,
                        handler,
                        disposition),
                factory,
                admin,
                mailbox,
                metrics,
                recoverySchedule);
    }

    private FakePublisherFactory.FakeTopicPublisher orderedPublisher() {
        return factory.publishers.get(ORDERED_TOPIC);
    }

    private List<String> orderedPayloads() {
        return payloads(orderedPublisher().published);
    }

    /** Payloads of the keyed publishes the fake turned away because their key was paused. */
    private List<String> rejectedPayloads() {
        return payloads(orderedPublisher().rejected);
    }

    private static List<String> payloads(List<PubsubMessage> messages) {
        return messages.stream()
                .map(PubsubMessage::getData)
                .map(data -> data.toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private static TopicDestination topic(String topic) {
        return TopicDestination.of(PROJECT, topic);
    }

    private List<String> fixedTopicPayloads() {
        return payloads(factory.publishers.get(topic("fixed")).published);
    }

    private static StatusRuntimeException invalidArgument() {
        return new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("message too large"));
    }

    private static Exception gaxInvalidArgument() {
        return ApiExceptionFactory.createException(
                invalidArgument(), GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT), false);
    }

    /** The SDK publisher's cancellation of an ordering key's queued publishes (no cause). */
    private static CancellationException cascade() {
        return new CancellationException(
                "Execution cancelled because executing previous runnable failed.");
    }

    @Test
    void aSerializationFailureIsRoutedAndTheRecordIsDropped() throws Exception {
        IOException failure = new IOException("bad record");
        PubSubWriter<String> writer =
                newWriter(
                        element -> {
                            if (element.equals("topic-a")) {
                                throw failure;
                            }
                            return PubsubMessage.newBuilder().build();
                        });

        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);

        assertThat(handler.handled).hasSize(1);
        FailedMessage failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(topic("topic-a"));
        // Serialization never produced a message, so the shared contract's payload is null.
        assertThat(failed.getPubsubMessage()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.getErrorMessage()).contains("could not be serialized");
        // The topic is on the element, not repeated in the description — which is what the
        // built-in handlers compose, so a handler still reports it exactly once.
        assertThat(failed.describeDestination()).endsWith("/topics/topic-a");
        assertThat(failed.getCause()).isSameAs(failure);
        // Dropped, not published — and the writer carried on with the next record.
        assertThat(factory.publishers.keySet()).containsExactly(topic("topic-b"));
    }

    @Test
    void skipsRecordsTheSerializerReturnsNullFor() throws Exception {
        PubSubWriter<String> writer =
                newWriter(
                        element ->
                                element.equals("topic-a")
                                        ? null
                                        : PubsubMessage.newBuilder().build());

        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);

        // Skipped, not failed: never offered to the handler, and no publisher was opened for the
        // topic the skipped record would have gone to.
        assertThat(handler.handled).isEmpty();
        assertThat(factory.publishers.keySet()).containsExactly(topic("topic-b"));
        assertThat(metrics.counterValue("numRecordsSend")).isEqualTo(1);
        assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
        assertThat(metrics.counterValue("recordsSkipped")).isEqualTo(1);
    }

    @Test
    void anInvalidArgumentPublishIsRoutedWithItsMessage() throws Exception {
        PubSubWriter<String> writer = newWriter();
        StatusRuntimeException batchReport = invalidArgument();
        StatusRuntimeException soloRejection = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(soloRejection));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        FailedMessage failed = handler.handled.get(0);
        assertThat(failed.getDestination()).isEqualTo(topic("topic-a"));
        assertThat(failed.getPubsubMessage()).isNotNull();
        assertThat(failed.getPubsubMessage().getData().toString(StandardCharsets.UTF_8))
                .isEqualTo("topic-a");
        assertThat(failed.getErrorMessage()).contains("INVALID_ARGUMENT");
        assertThat(failed.describeDestination()).endsWith("/topics/topic-a");
        // The cause is the solo rejection, not the batch report: the report says only that the
        // request failed, and the verdict the handler acts on is the one the message earned alone.
        assertThat(failed.getCause()).isSameAs(soloRejection);
    }

    @Test
    void aGaxInvalidArgumentIsRoutedToo() throws Exception {
        // The SDK publisher surfaces gax ApiExceptions; the raw gRPC form above is defense in
        // depth. Both have to reach the handler or the policy covers only half its class.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(gaxInvalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(gaxInvalidArgument()));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
    }

    @Test
    void aRoutedFailureReleasesItsInFlightCountersAndDoesNotFailTheJob() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);
        writer.write("topic-a", CONTEXT);

        assertThat(writer.getInFlightMessages()).isEqualTo(1);
        assertThat(handler.handled).hasSize(1);
    }

    @Test
    void aValidMessageCoBatchedWithAnInvalidOneIsPublishedNotDropped() throws Exception {
        // The #264 pin. Publish is a batch RPC that rejects all-or-nothing, and the SDK sets the
        // ONE request-level INVALID_ARGUMENT on every co-batched future — scripted here as three
        // failures sharing a single exception instance, which is exactly what
        // Publisher.OutstandingBatch.onFailure produces. Only the message the service rejects
        // when republished alone may reach a dropping handler; its neighbours must be published.
        PubSubWriter<String> writer = newFixedTopicWriter();
        StatusRuntimeException batchReport = invalidArgument();
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        }
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.write("m2", CONTEXT);
        // Solo verdicts, in publish order: m0 accepted, m1 rejected, m2 accepted (default).
        StatusRuntimeException soloRejection = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFuture("id-m0"));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(soloRejection));

        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getPubsubMessage().getData().toStringUtf8())
                .isEqualTo("m1");
        assertThat(handler.handled.get(0).getCause()).isSameAs(soloRejection);
        // Each neighbour was published twice — the rejected batch, then its own accepted request.
        assertThat(fixedTopicPayloads()).containsExactly("m0", "m1", "m2", "m0", "m1", "m2");
    }

    @Test
    void aBatchLevelRejectionWhoseMessagesAllPassSoloRoutesNothing() throws Exception {
        // Routing requires a solo verdict: a request-level report alone identifies no message, so
        // when every message of the failed batch is accepted on its own request — the service
        // rejected the batch for a reason the republish shape cured, request size being the
        // measured example — nothing reaches the handler at all.
        PubSubWriter<String> writer = newFixedTopicWriter();
        StatusRuntimeException batchReport = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));

        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).isEmpty();
        assertThat(fixedTopicPayloads()).containsExactly("m0", "m1", "m0", "m1");
    }

    // --- The dropping-policy bound on the isolation pass (#361) ------------------------------

    @Test
    void failsTheJobOnceConsecutiveConfirmedRejectionsReachTheBound() throws Exception {
        // #361: a dropping policy keeps the job green through anomalous records, but a stream
        // refused wholesale is broken data degraded to one solo publish per message — so the
        // bound fails the job, with every rejected message routed before it does.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder().maxConsecutiveRejections(2).build());
        StatusRuntimeException batchReport = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        // Both solo verdicts reject; the second reaches the bound.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)")
                .hasMessageContaining("refused 2 messages in a row")
                .hasMessageContaining("INVALID_ARGUMENT")
                // The recovery budget's exhaustion reports a repair that could not drain; the
                // bound reports the stream. An operator must be able to tell them apart.
                .hasMessageNotContaining("recovery");
        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0", "m1");
    }

    @Test
    void aRunAccumulatesAcrossFlushesWithNoSuccessBetween() throws Exception {
        // "Consecutive" is about successes, not checkpoint intervals: two rejections in one flush
        // and a third in the next, with nothing published between them, are one run of three.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder().maxConsecutiveRejections(3).build());
        StatusRuntimeException firstReport = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(firstReport));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(firstReport));
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("m2", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(3)");
        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0", "m1", "m2");
    }

    @Test
    void serializerRejectionsDoNotCountTowardTheBound() throws Exception {
        // A record the serializer rejects says nothing about the service's view of the stream, so
        // a serializer-failure storm under a small bound stays green — routed, dropped, and never
        // accumulated.
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forResolver(
                                (element, context) -> topic("fixed"),
                                element -> {
                                    throw new IOException("bad record " + element);
                                },
                                PubSubPublisherOptions.builder()
                                        .maxConsecutiveRejections(2)
                                        .build(),
                                handler,
                                CreateDisposition.CREATE_IF_NEEDED),
                        factory,
                        admin,
                        mailbox,
                        metrics,
                        FAST_SCHEDULE);

        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.write("m2", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(3);
    }

    @Test
    void aBoundOfOneFailsOnTheFirstConfirmedRejection() throws Exception {
        // The strictest legal setting, and the message's singular form.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder().maxConsecutiveRejections(1).build());
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("m0", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("refused a message (status INVALID_ARGUMENT)")
                .hasMessageContaining("maxConsecutiveRejections(1)");
        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0");
    }

    @Test
    void theBoundTrippingOnAKeyedDropLeavesItsKeyToTheFailedJob() throws Exception {
        // The ordering interplay: the tripping drop registers its key for a resume the aborted
        // repair never runs. That is safe — the job is failing, and the restart opens fresh
        // publishers with no paused keys — but the resume history has to stop where the bound
        // spoke: the attempt-start resume and the one after the first drop, none after the trip.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        PubSubPublisherOptions.builder()
                                .enableMessageOrdering(true)
                                .maxConsecutiveRejections(2)
                                .build(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        FAST_SCHEDULE);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)");
        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("k1:a", "k1:b");
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1");
    }

    @Test
    void aCollateralSuccessInsideTheIsolationPassResetsTheCount() throws Exception {
        // The interleaving the pass was designed around: a good message batched between two bad
        // ones is published by its solo republish, and that success resets the count mid-pass —
        // two bad messages in one batch are two runs of one, not one run of two.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder().maxConsecutiveRejections(2).build());
        StatusRuntimeException batchReport = invalidArgument();
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        }
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.write("m2", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFuture("id-m1"));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0", "m2");
    }

    @Test
    void aSuccessZeroesTheCountRatherThanCancellingOneRejection() throws Exception {
        // Two confirmed rejections, one success, two more: a counter that merely decremented on
        // success would reach the bound of 3 at the fourth rejection; zeroing keeps every run at
        // two. "Any successful publish resets the count" means reset, not repayment.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder().maxConsecutiveRejections(3).build());
        StatusRuntimeException firstReport = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(firstReport));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(firstReport));
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        writer.write("m2", CONTEXT);
        writer.flush(false);

        StatusRuntimeException secondReport = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(secondReport));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(secondReport));
        writer.write("m3", CONTEXT);
        writer.write("m4", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0", "m1", "m3", "m4");
    }

    @Test
    void theUnboundedSentinelKeepsIsolatingThroughConsecutiveRejections() throws Exception {
        // -1 restores the unbounded pass for a pipeline that really does want to trickle through
        // arbitrarily bad data. Discriminating: a sentinel misread as a bound of -1 or 0 would
        // fail the job on the first confirmed rejection here.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder()
                                .maxConsecutiveRejections(PubSubPublisherOptions.UNBOUNDED)
                                .build());
        StatusRuntimeException batchReport = invalidArgument();
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        }
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.write("m2", CONTEXT);
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        }

        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0", "m1", "m2");
    }

    @Test
    void theBoundTrippingMidPassAbandonsTheRestOfThePark() throws Exception {
        // The throw escapes the pass with messages still parked behind it: neither published nor
        // routed, which the failed checkpoint covers — the restart replays them. Pins that the
        // abandoned message is not silently routed after the bound has spoken.
        PubSubWriter<String> writer =
                newFixedTopicWriter(
                        PubSubPublisherOptions.builder().maxConsecutiveRejections(2).build());
        StatusRuntimeException batchReport = invalidArgument();
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        }
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.write("m2", CONTEXT);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)");
        assertThat(handler.handled)
                .extracting(failed -> failed.getPubsubMessage().getData().toStringUtf8())
                .containsExactly("m0", "m1");
        assertThat(writer.getParkedMessages()).isEqualTo(1);
    }

    @Test
    void theHandlerFlushesAfterTheDrainThatDiscoversTheFailure() throws Exception {
        // The pin is the order, not the count: the failure is discovered inside flush() — by the
        // repair's isolation pass — so a handler flushed before it would checkpoint past a dead
        // letter it has not seen.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("topic-a", CONTEXT);

        writer.flush(false);

        assertThat(handler.events).containsExactly("handle", "flush");
    }

    @Test
    void theHandlerIsFlushedEvenWhenNothingFailed() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);

        writer.flush(false);

        assertThat(handler.flushCalls).isEqualTo(1);
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void aThrowingHandlerFailsTheNextWrite() throws Exception {
        PubSubWriter<String> writer = newWriter();
        handler.failure = new IOException("dead-letter queue is down");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessage("dead-letter queue is down");
    }

    @Test
    void aHandlerThrowingUncheckedIsWrappedWithTheTopic() throws Exception {
        PubSubWriter<String> writer = newWriter();
        RuntimeException failure = new IllegalStateException("handler exploded");
        handler.failure = failure;
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
    }

    @Test
    void aMissingTopicIsRepairedRatherThanRouted() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.NOT_FOUND)));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).isEmpty();
        assertThat(admin.created).containsExactly(topic("topic-a"));
    }

    @Test
    void aMissingTopicUnderCreateNeverFailsTheJobRatherThanBeingRouted() throws Exception {
        PubSubWriter<String> writer =
                newWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema()),
                        CreateDisposition.CREATE_NEVER);
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.NOT_FOUND)));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void anOutageFailsTheJobRatherThanBeingRouted() throws Exception {
        // The reason MESSAGE_LEVEL stays INVALID_ARGUMENT-only: a dropping policy must not turn an
        // outage into silent data loss, one message at a time.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.UNAVAILABLE)));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a");
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void aCancellationIsNeverRouted() throws Exception {
        // Ordering is off in this writer, so a cancellation is not parked either — it must fail the
        // job, never be dead-lettered as if it were the cause. With ordering on it is parked and
        // republished instead, which theCascadesOfADroppedMessageAreRepublishedInPublishOrder
        // covers; either way it never reaches the handler, because it is never a root cause (#78).
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new CancellationException("key paused")));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(handler.handled).isEmpty();
    }

    // --- Message ordering (#215) -------------------------------------------------------------
    //
    // The fake publisher models the SDK's paused ordering keys (#277): a failed keyed publish
    // pauses its key, a publish on a paused key comes back cancelled without being published, and
    // only resumePublish clears it. So a cascade no longer has to be scripted — publishing to a
    // key whose root failed produces one — and a resume that is missing, or that runs at the
    // wrong time, is observable through the publishes it lets through or turns away, not only
    // through resumedKeys.
    //
    // A key appears in resumedKeys twice per dropped message: once from the attempt-start batch
    // resume, and once from the isolation pass handing the key back right after the solo
    // rejection re-paused it. The second is what lets the rest of the pass publish at all.

    @Test
    void aDroppedKeyedMessageResumesItsOrderingKey() throws Exception {
        // The SDK pauses an ordering key on any non-retryable failure without inspecting it, and
        // never resumes one itself. So dropping the message is not the end of the writer's
        // obligations: without this the key is dead for the rest of the writer's life.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("k1:a", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1");
        // The batch report and then the solo rejection; no topic was missing, so the repair
        // creates none.
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:a");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void theCascadesOfADroppedMessageAreRepublishedInPublishOrder() throws Exception {
        // The messages behind the dropped one are cancelled by the publisher's paused key — the
        // fake turns them away itself — not rejected by the service. They are perfectly
        // publishable, and the isolation pass republishing the whole batch in publish sequence —
        // the rejected head dropped on its solo verdict, the survivors accepted on theirs — is
        // what keeps the survivors of the key in their relative order.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        writer.write("k1:c", CONTEXT);
        // The solo verdict that confirms a as the invalid one; b and c pass by default.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getPubsubMessage().getOrderingKey()).isEqualTo("k1");
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1");
        // b and c came back cancelled from the paused key and reached the topic only through the
        // isolation pass, in publish order; a is the gap the drop leaves behind.
        assertThat(rejectedPayloads()).containsExactly("k1:b", "k1:c");
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:a", "k1:b", "k1:c");
    }

    @Test
    void aKeyPausedByADropStaysPausedUntilTheRepairResumesIt() throws Exception {
        // The reason the resume lives in resumeOrderingKeys and not in routeFailedMessage: write()
        // tests repairNeeded before awaitCapacity(), and mails — the drop among them — run inside
        // it, so a key resumed from the drop's mail could be published to by the rest of that same
        // write() while the key's cascades were still parked: a newer message ahead of older ones.
        // Left paused, the racing publish comes back cancelled and is republished in
        // publish-sequence order with the rest.
        //
        // The in-flight cap is what forces the drop's mail to run mid-write: with two publishes
        // outstanding, the third write yields inside awaitCapacity() after it already tested
        // repairNeeded. This is the test #277 exists to make possible — with a fake that accepted
        // every publish, the eager-resume design published k1:c ahead of the parked k1:b and
        // nothing could see it.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        PubSubPublisherOptions.builder()
                                .enableMessageOrdering(true)
                                .maxInFlightMessages(2)
                                .build(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        FAST_SCHEDULE);
        SettableApiFuture<String> root = SettableApiFuture.create();
        SettableApiFuture<String> queued = SettableApiFuture.create();
        factory.enqueueFuture(root);
        factory.enqueueFuture(queued);
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        // a's failure pauses the key and queues the drop's mail; b's cascade trails it.
        root.setException(invalidArgument());
        queued.setException(cascade());

        writer.write("k1:c", CONTEXT);
        // The solo verdict confirming a; b and c pass their own requests by default.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        // The racing publish was turned away by the still-paused key…
        assertThat(rejectedPayloads()).containsExactly("k1:c");
        // …so one repair resumed the key and republished a, b and c in publish order — a dropped
        // on its solo verdict, b and c accepted on theirs.
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1");
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:b", "k1:a", "k1:b", "k1:c");
    }

    @Test
    void aCascadeObservedBeforeItsDroppedRootIsStillRepublished() throws Exception {
        // #78's shape with a drop as the root rather than a NOT_FOUND. The SDK cancels a key's
        // queued publishes from its own thread, so the cascade's mail can reach the mailbox first;
        // completing the futures out of publish order reproduces that, since the callbacks run
        // inline here. The pre-repair drainInFlight() is what makes it safe: every mail, the drop
        // among them, has run before a batch is snapshotted.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        SettableApiFuture<String> root = SettableApiFuture.create();
        SettableApiFuture<String> cascade = SettableApiFuture.create();
        factory.enqueueFuture(root);
        factory.enqueueFuture(cascade);
        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);

        cascade.setException(cascade());
        root.setException(invalidArgument());
        mailbox.drain();

        // The solo verdict confirming the root; the cascade passes its own request by default.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1");
        assertThat(orderedPayloads())
                .containsExactly("k1:first", "k1:second", "k1:first", "k1:second");
    }

    @Test
    void aDropUnderCreateNeverParksTheCascadeAndCreatesNoTopic() throws Exception {
        // The case that forces cascade parking to be independent of the disposition: routing a
        // cascade to asyncError under CREATE_NEVER would fail the job over a dropped message's
        // queued neighbours, the opposite of the configured policy. The disposition still decides
        // that no topic is created, which is asserted here rather than assumed.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_NEVER);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        // The solo verdict confirming a; b passes its own request by default.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1");
        assertThat(rejectedPayloads()).containsExactly("k1:b");
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:a", "k1:b");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void aResumedKeyIsNotCarriedIntoTheNextRepair() throws Exception {
        // The registered keys are drained, not accumulated. Resuming an unpaused key is a no-op, so
        // what this protects is the set itself: kept, it would grow with every distinct key ever
        // dropped on the destination and make each later repair re-resume all of them.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("k1:a", CONTEXT);
        writer.flush(false);

        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.NOT_FOUND)));
        writer.write("k2:b", CONTEXT);
        writer.flush(false);

        // k1 twice, both by the repair that drained it — not again alongside k2.
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1", "k2");
        assertThat(admin.created).containsExactly(ORDERED_TOPIC);
    }

    @Test
    void aPoisonedOrderingKeyDrainsInOneRepairAttempt() throws Exception {
        // The #269 pin. A run of consecutively invalid keyed messages can be longer than the
        // recovery budget; the isolation pass gives every parked message its own verdict within a
        // single attempt and hands the key back after each drop, so the run's length does not
        // count against the budget — four drops under a budget of two, and the flush completes.
        // Without the mid-pass resume, every solo publish after the first drop would come back
        // cancelled from the paused key, and the budget would be spent re-parking them.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        CreateDisposition.CREATE_IF_NEEDED,
                        new RetrySchedule(1, 1, 2, 0));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        writer.write("k1:c", CONTEXT);
        writer.write("k1:d", CONTEXT);
        // Every solo republish is rejected: the whole run really is invalid.
        for (int i = 0; i < 4; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        }

        writer.flush(false);

        assertThat(handler.handled).hasSize(4);
        // One attempt-start resume for the batch, then one after each of the four drops.
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k1", "k1", "k1", "k1");
        assertThat(rejectedPayloads()).containsExactly("k1:b", "k1:c", "k1:d");
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:a", "k1:b", "k1:c", "k1:d");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void aRepairThatCannotDrainSaysHowManyMessagesWereDropped() throws Exception {
        // The #269 exhaustion message: when the budget runs out on a repair that was dropping
        // messages, the failure must say so — a reader sent looking for a topic problem by the
        // topic-shaped text would find nothing wrong with the topic. Here the head is dropped on
        // its solo verdict but the second message's republishes keep coming back cancelled, so
        // the repair cannot drain within a budget of two.
        //
        // The created-topic exhaustion texts stay pinned by PubSubWriterAutoCreationTest's
        // budget-exhaustion tests, where no message is ever routed.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        CreateDisposition.CREATE_IF_NEEDED,
                        new RetrySchedule(1, 1, 2, 0));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        mailbox.drain();
        // Attempt 1, isolating: a is rejected solo and dropped; b's republish comes back
        // cancelled and is re-parked. Attempt 2 republishes b as a batch and it is cancelled
        // again — budget spent.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("could not drain its parked messages within the recovery")
                .hasMessageContaining("2 attempt(s)")
                .hasMessageContaining("1 message(s) were handed to the failure handler")
                .hasMessageNotContaining("after creating the topic");
        assertThat(handler.handled).hasSize(1);
        assertThat(admin.created).isEmpty();
    }

    @Test
    void aRepairThatCreatedTheTopicAndKeptDroppingSaysBoth() throws Exception {
        // The two exhaustion facts are not exclusive: a repair can create the topic and then run
        // out of budget while dropping messages. The drain-shaped text must keep the creation
        // clause, or the reader loses the topic half of the story.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        CreateDisposition.CREATE_IF_NEEDED,
                        new RetrySchedule(1, 1, 2, 0));
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.NOT_FOUND)));
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        mailbox.drain();
        // Attempt 1 creates the topic and republishes as a batch; a's republish is rejected with
        // a request-level INVALID_ARGUMENT, which re-pauses the key and turns b away again.
        // Attempt 2 isolates: a is dropped on its solo verdict, b's solo republish is cancelled —
        // budget spent with the topic created and one message dropped.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("could not drain its parked messages within the recovery")
                .hasMessageContaining("after creating the topic")
                .hasMessageContaining("1 message(s) were handed to the failure handler");
        assertThat(handler.handled).hasSize(1);
        assertThat(admin.created).containsExactly(ORDERED_TOPIC);
    }

    @Test
    void aThrowingHandlerOnAKeyedMessageDoesNotResumeAfterTheRefusedDrop() throws Exception {
        // Returning from handle() is the SPI's only way of saying "dropped". A handler that threw
        // refused, so the job is failing and its key is never published to again.
        //
        // The outcome is enforced twice over, and this test cannot tell the two apart: routing
        // returns early on a throw, and asyncError gates every path into a repair regardless. So a
        // mutant that deletes that early return survives — measured, not assumed. What is asserted
        // here is the outcome a user sees, which holds either way.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        handler.failure = new IOException("refused");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("k1:a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        // Only the attempt-start resume, which any parked message provokes: the refused drop must
        // not add the post-drop resume a completed drop earns.
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1");
    }

    @Test
    void aDroppedUnkeyedMessageResumesNothing() throws Exception {
        // An unkeyed message never enters the SDK's sequential executor — every ordering branch in
        // Publisher is guarded on a non-empty key — so it pauses nothing, even with ordering on.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema()),
                        CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("unkeyed", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).isEmpty();
    }

    @Test
    void aDroppedSerializationFailureResumesNothing() throws Exception {
        // The record never became a message, so no key ever reached the publisher — and the key is
        // not even knowable, since deriving it is part of what failed. A second record publishes
        // successfully so the destination does have a publisher: asserting on an absent one would
        // pass whatever the drop did.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        element -> {
                            if (element.startsWith("bad")) {
                                throw new IOException("bad record");
                            }
                            return PubsubMessage.newBuilder()
                                    .setOrderingKey(element.split(":")[0])
                                    .build();
                        },
                        CreateDisposition.CREATE_IF_NEEDED);

        writer.write("k1:good", CONTEXT);
        writer.write("bad:k1", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).isEmpty();
    }

    @Test
    void closeClosesTheHandler() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);

        writer.close();

        assertThat(handler.closeCalls).isEqualTo(1);
    }

    @Test
    void closeClosesTheHandlerEvenWhenAPublisherCloseFails() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        RuntimeException failure = new RuntimeException("shutdown exploded");
        factory.publishers.get(topic("topic-a")).closeFailure = failure;

        assertThatThrownBy(writer::close).isSameAs(failure);

        // The lifecycle contract promises close on the failure path too.
        assertThat(handler.closeCalls).isEqualTo(1);
    }

    @Test
    void closeClosesTheHandlerEvenWhenAPublisherCloseThrowsAnError() throws Exception {
        // #276: the comment above this close() said the handler is closed even when a publisher's
        // shutdown throws, and Flink's IOUtils.closeAll made that false for an Error — it rethrows
        // from inside its loop, so both the topic admin and the handler stayed open. Since #211
        // the handler can own an SDK publisher and a gRPC channel, and a gax shutdown is a
        // plausible place for a NoClassDefFoundError. That the Error reaches the caller as an
        // Error is the other half: Flink halts the JVM on a fatal one, and only if it arrives
        // unwrapped.
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        factory.publishers.get(topic("topic-a")).closeFailure =
                new NoClassDefFoundError("shutdown blew up");

        assertThatThrownBy(writer::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("shutdown blew up");
        assertThat(admin.closeCalls).isEqualTo(1);
        assertThat(handler.closeCalls).isEqualTo(1);
    }

    @Test
    void theWriterDoesNotOpenTheHandler() throws Exception {
        // Opening belongs to the production createWriter (PubSubSinkFailureHandlerOpenTest), so a
        // writer built against injected fakes must not open a second time.
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.close();

        assertThat(handler.openCalls).isZero();
    }
}
