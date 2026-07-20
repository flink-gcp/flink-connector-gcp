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

package io.github.flink.gcp.connector.pubsub.sink.publisher.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFutures;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the topic auto-creation (NOT_FOUND repair) paths of {@link PubSubWriter}. */
class PubSubWriterAutoCreationTest {

    private static final String PROJECT = "test-project";
    private static final TopicDestination TOPIC = TopicDestination.of(PROJECT, "auto-topic");

    /** A fast schedule keeping repair backoffs out of the test wall clock. */
    private static final RetrySchedule FAST_SCHEDULE = new RetrySchedule(1, 1, 5, 0);

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private final FakePublisherFactory factory = new FakePublisherFactory();
    private final FakeTopicAdmin admin = new FakeTopicAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();

    /** Publishes every record (the payload) to the fixed {@link #TOPIC}. */
    private PubSubWriter<String> newWriter(CreateDisposition disposition) {
        return newWriter(disposition, FAST_SCHEDULE);
    }

    private PubSubWriter<String> newWriter(
            CreateDisposition disposition, RetrySchedule recoverySchedule) {
        return new PubSubWriter<>(
                TestSinkConfigs.forTopic(
                        TOPIC,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        disposition,
                        PubSubPublisherOptions.defaults()),
                factory,
                admin,
                mailbox,
                PubSubPublisherOptions.defaults().getMaxInFlightMessages(),
                recoverySchedule);
    }

    /**
     * Publishes every record (the payload) to the fixed {@link #TOPIC} with message ordering
     * enabled, taking the ordering key from the record's prefix before {@code ':'}.
     */
    private PubSubWriter<String> newOrderingWriter(CreateDisposition disposition) {
        return newOrderingWriter(disposition, FAST_SCHEDULE);
    }

    private PubSubWriter<String> newOrderingWriter(
            CreateDisposition disposition, RetrySchedule recoverySchedule) {
        return new PubSubWriter<>(
                TestSinkConfigs.forTopic(
                        TOPIC,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        disposition,
                        PubSubPublisherOptions.builder().enableMessageOrdering(true).build()),
                factory,
                admin,
                mailbox,
                PubSubPublisherOptions.defaults().getMaxInFlightMessages(),
                recoverySchedule);
    }

    private static StatusRuntimeException notFound() {
        return new StatusRuntimeException(Status.NOT_FOUND);
    }

    /** The SDK publisher's cancellation of an ordering key's queued publishes (no cause). */
    private static CancellationException cascade() {
        return new CancellationException(
                "Execution cancelled because executing previous runnable failed.");
    }

    private FakePublisherFactory.FakeTopicPublisher publisher() {
        return factory.publishers.get(TOPIC);
    }

    private List<String> publishedPayloads() {
        return factory.publishers.get(TOPIC).published.stream()
                .map(PubsubMessage::getData)
                .map(data -> data.toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    @Test
    void notFoundPublishCreatesTopicAndRepublishesOnNextWrite() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));

        writer.write("first", CONTEXT);
        mailbox.drain();
        writer.write("second", CONTEXT);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads()).containsExactly("first", "first", "second");
        mailbox.drain();
        assertThat(writer.getInFlightMessages()).isZero();
    }

    @Test
    void gaxNotFoundIsRecoveredLikeGrpcNotFound() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(
                        ApiExceptionFactory.createException(
                                notFound(), GrpcStatusCode.of(Status.Code.NOT_FOUND), false)));

        writer.write("first", CONTEXT);
        mailbox.drain();
        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads()).containsExactly("first", "first");
    }

    @Test
    void flushRepairsPendingRetries() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("first", CONTEXT);
        mailbox.drain();

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads()).containsExactly("first", "first");
        assertThat(writer.getInFlightMessages()).isZero();
    }

    @Test
    void flushRepairsNotFoundDiscoveredDuringFinalDrain() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        // The failure mail is left unprocessed: flush's own drain discovers it and must loop
        // into a repair instead of reporting a successful checkpoint.
        writer.write("first", CONTEXT);

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads()).containsExactly("first", "first");
        assertThat(writer.getInFlightMessages()).isZero();
    }

    @Test
    void createNeverFailsWithDispositionHint() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_NEVER);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("first", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.write("second", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("auto-topic")
                .hasMessageContaining("CREATE_NEVER");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void retryBudgetExhaustionFailsWithCause() throws Exception {
        PubSubWriter<String> writer =
                newWriter(CreateDisposition.CREATE_IF_NEEDED, new RetrySchedule(1, 1, 2, 0));
        StatusRuntimeException failure = notFound();
        // The initial publish and both republish attempts fail with NOT_FOUND.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        writer.write("first", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("auto-topic")
                .hasMessageContaining("2 attempt(s)")
                .hasCause(failure);
        assertThat(admin.created).containsExactly(TOPIC);
    }

    @Test
    void terminalFailureDuringRepairAborts() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        StatusRuntimeException denied = new StatusRuntimeException(Status.PERMISSION_DENIED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(denied));
        writer.write("first", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("auto-topic")
                .hasCause(denied);
        assertThat(admin.created).containsExactly(TOPIC);
    }

    @Test
    void topicCreatedOncePerRepairForMultiplePendingMessages() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("first", CONTEXT);
        writer.write("second", CONTEXT);
        mailbox.drain();

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads()).containsExactly("first", "second", "first", "second");
    }

    @Test
    void topicCreationFailureSurfacesFromRepair() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        admin.createFailure = new IOException("no pubsub.topics.create permission");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("first", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false)).isSameAs(admin.createFailure);
        assertThat(admin.created).isEmpty();
    }

    @Test
    void cascadesParkBehindNotFoundAndRepublishInOrderAfterResume() throws Exception {
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);
        writer.write("k1:third", CONTEXT);
        mailbox.drain();

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publisher().resumedKeys).containsExactly("k1");
        assertThat(publishedPayloads())
                .containsExactly(
                        "k1:first", "k1:second", "k1:third", "k1:first", "k1:second", "k1:third");
    }

    @Test
    void repairResumesEveryDistinctOrderingKeyOfTheBatch() throws Exception {
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("k1:a", CONTEXT);
        writer.write("k2:b", CONTEXT);
        mailbox.drain();

        writer.flush(false);

        assertThat(publisher().resumedKeys).containsExactly("k1", "k2");
        assertThat(publishedPayloads()).containsExactly("k1:a", "k2:b", "k1:a", "k2:b");
    }

    @Test
    void everyRepublishAttemptResumesTheKeysAgain() throws Exception {
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        // The initial publish and the first republish attempt fail; the second attempt succeeds.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("k1:a", CONTEXT);
        mailbox.drain();

        writer.flush(false);

        assertThat(publisher().resumedKeys).containsExactly("k1", "k1");
        assertThat(publishedPayloads()).containsExactly("k1:a", "k1:a", "k1:a");
    }

    @Test
    void createNeverIsNotMaskedByCascades() throws Exception {
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_NEVER);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void fatalRootFailureDropsItsCascades() throws Exception {
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        StatusRuntimeException denied = new StatusRuntimeException(Status.PERMISSION_DENIED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(denied));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasCause(denied);
        assertThat(admin.created).isEmpty();
    }

    @Test
    void cancellationWithOrderingDisabledIsTerminalEvenWithAPendingRepair() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        CancellationException cancellation = cascade();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cancellation));
        writer.write("first", CONTEXT);
        writer.write("second", CONTEXT);
        mailbox.drain();

        // Without ordering there are no key cascades, so a cancellation must surface as a
        // terminal failure (with no ordering-key wording) instead of being parked for repair.
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasCause(cancellation)
                .hasMessageNotContaining("ordering key");
    }

    @Test
    void bareCascadeWithoutAPendingRepairIsTerminalWithOrderingWording() throws Exception {
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:a", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ordering key");
        assertThat(admin.created).isEmpty();
    }

    @Test
    void budgetExhaustionCauseIsTheRealNotFoundDespiteCascades() throws Exception {
        PubSubWriter<String> writer =
                newOrderingWriter(
                        CreateDisposition.CREATE_IF_NEEDED, new RetrySchedule(1, 1, 2, 0));
        StatusRuntimeException failure = notFound();
        // The initial publishes and both republish attempts fail: NOT_FOUND root + cascade each.
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        }
        writer.write("k1:a", CONTEXT);
        writer.write("k1:b", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2 attempt(s)")
                .hasCause(failure);
    }
}
