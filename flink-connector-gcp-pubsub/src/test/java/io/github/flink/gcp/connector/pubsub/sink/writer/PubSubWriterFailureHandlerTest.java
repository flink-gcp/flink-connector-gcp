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
        return newWriter(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
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
     * A writer publishing every record to one topic with message ordering enabled, taking the
     * ordering key from the record's prefix before {@code ':'} — the fixture {@link
     * PubSubWriterAutoCreationTest} uses, so the two ordering suites read alike.
     *
     * <p>The recovery schedule is a real one rather than {@link #UNUSED_RECOVERY}: these tests do
     * reach the repair.
     */
    private PubSubWriter<String> newOrderingWriter(CreateDisposition disposition) {
        return newOrderingWriter(
                PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
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
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> ORDERED_TOPIC,
                        serializer,
                        PubSubPublisherOptions.builder().enableMessageOrdering(true).build(),
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
        return orderedPublisher().published.stream()
                .map(PubsubMessage::getData)
                .map(data -> data.toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private static TopicDestination topic(String topic) {
        return TopicDestination.of(PROJECT, topic);
    }

    private static StatusRuntimeException invalidArgument() {
        return new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("message too large"));
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
        assertThat(metrics.counterValue("numRecordsSkipped")).isEqualTo(1);
    }

    @Test
    void anInvalidArgumentPublishIsRoutedWithItsMessage() throws Exception {
        PubSubWriter<String> writer = newWriter();
        StatusRuntimeException failure = invalidArgument();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));

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
        assertThat(failed.getCause()).isSameAs(failure);
    }

    @Test
    void aGaxInvalidArgumentIsRoutedToo() throws Exception {
        // The SDK publisher surfaces gax ApiExceptions; the raw gRPC form above is defense in
        // depth. Both have to reach the handler or the policy covers only half its class.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(
                        ApiExceptionFactory.createException(
                                invalidArgument(),
                                GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                                false)));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
    }

    @Test
    void aRoutedFailureReleasesItsInFlightCountersAndDoesNotFailTheJob() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);
        writer.write("topic-a", CONTEXT);

        assertThat(writer.getInFlightMessages()).isEqualTo(1);
        assertThat(handler.handled).hasSize(1);
    }

    @Test
    void theHandlerFlushesAfterTheDrainThatDiscoversTheFailure() throws Exception {
        // The pin is the order, not the count: the failure is discovered by flush()'s own drain, so
        // a handler flushed before the drain would checkpoint past a dead letter it has not seen.
        PubSubWriter<String> writer = newWriter();
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
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
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
    // Note what the fake publisher cannot do: it holds no paused-key state, so it accepts a publish
    // the real SDK would reject outright with a cancellation. That the writer hands a dropped
    // message's key back is therefore only observable through resumedKeys, never through a publish
    // that would otherwise fail.
    //
    // And only one test below discriminates it: aDroppedKeyedMessageResumesItsOrderingKey, whose
    // batch is empty. Wherever a cascade is parked, the batch carries the same key and would resume
    // it anyway — so those tests pin the republish, not the registration.

    @Test
    void aDroppedKeyedMessageResumesItsOrderingKey() throws Exception {
        // The SDK pauses an ordering key on any non-retryable failure without inspecting it, and
        // never resumes one itself. So dropping the message is not the end of the writer's
        // obligations: without this the key is dead for the rest of the writer's life.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));

        writer.write("k1:a", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1");
        // Nothing to republish — the only message for the key was the dropped one — and no topic
        // was missing, so the repair creates none.
        assertThat(orderedPayloads()).containsExactly("k1:a");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void theCascadesOfADroppedMessageAreRepublishedInPublishOrder() throws Exception {
        // The messages queued behind the dropped one were cancelled by the SDK, not rejected by the
        // service. They are perfectly publishable, and republishing them in publish sequence is
        // what keeps the survivors of the key in their relative order.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));

        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        writer.write("k1:c", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getPubsubMessage().getOrderingKey()).isEqualTo("k1");
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1");
        // b and c republished, in that order; a is the gap the drop leaves behind.
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:b", "k1:c", "k1:b", "k1:c");
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

        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1");
        assertThat(orderedPayloads()).containsExactly("k1:first", "k1:second", "k1:second");
    }

    @Test
    void aDropUnderCreateNeverParksTheCascadeAndCreatesNoTopic() throws Exception {
        // The case that forces cascade parking to be independent of the disposition: routing a
        // cascade to asyncError under CREATE_NEVER would fail the job over a dropped message's
        // queued neighbours, the opposite of the configured policy. The disposition still decides
        // that no topic is created, which is asserted here rather than assumed.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_NEVER);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));

        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        writer.flush(false);

        assertThat(handler.handled).hasSize(1);
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1");
        assertThat(orderedPayloads()).containsExactly("k1:a", "k1:b", "k1:b");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void aResumedKeyIsNotCarriedIntoTheNextRepair() throws Exception {
        // The registered keys are drained, not accumulated. Resuming an unpaused key is a no-op, so
        // what this protects is the set itself: kept, it would grow with every distinct key ever
        // dropped on the destination and make each later repair re-resume all of them.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        writer.write("k1:a", CONTEXT);
        writer.flush(false);

        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.NOT_FOUND)));
        writer.write("k2:b", CONTEXT);
        writer.flush(false);

        // k1 once, by the repair that drained it — not again alongside k2.
        assertThat(orderedPublisher().resumedKeys).containsExactly("k1", "k2");
        assertThat(admin.created).containsExactly(ORDERED_TOPIC);
    }

    @Test
    void aRepairThatCreatedNoTopicSaysSoWhenItRunsOutOfAttempts() throws Exception {
        // The budget bounds any repair, not only a topic-creation one, so the exhaustion message
        // must not claim a creation that never happened. Here the root is a dropped message and the
        // cascade's republishes keep being cancelled.
        //
        // This also pins the bound itself: a key whose messages are rejected one batch per attempt
        // fails the job once the budget runs out, rather than being drained. Whether that is the
        // right bound is #269, and this test is what changes when it is answered.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        CreateDisposition.CREATE_IF_NEEDED,
                        new RetrySchedule(1, 1, 2, 0));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(invalidArgument()));
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        }
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2 attempt(s)")
                .hasMessageNotContaining("after creating the topic");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void aThrowingHandlerOnAKeyedMessageResumesNothing() throws Exception {
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
        writer.write("k1:a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(orderedPublisher().resumedKeys).isEmpty();
    }

    @Test
    void aDroppedUnkeyedMessageResumesNothing() throws Exception {
        // An unkeyed message never enters the SDK's sequential executor — every ordering branch in
        // Publisher is guarded on a non-empty key — so it pauses nothing, even with ordering on.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        CreateDisposition.CREATE_IF_NEEDED);
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
