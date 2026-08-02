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

    private static TopicDestination topic(String topic) {
        return TopicDestination.of(PROJECT, topic);
    }

    private static StatusRuntimeException invalidArgument() {
        return new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("message too large"));
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
        // Ordering is off here (the builder rejects it beside a handler), so a cancellation is not
        // parked either — it must fail the job, never be dead-lettered as if it were the cause.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new CancellationException("key paused")));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(handler.handled).isEmpty();
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
    void theWriterDoesNotOpenTheHandler() throws Exception {
        // Opening belongs to the production createWriter (PubSubSinkFailureHandlerOpenTest), so a
        // writer built against injected fakes must not open a second time.
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.close();

        assertThat(handler.openCalls).isZero();
    }
}
