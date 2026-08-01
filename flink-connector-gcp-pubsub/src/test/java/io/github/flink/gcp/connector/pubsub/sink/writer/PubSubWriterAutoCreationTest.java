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
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the topic auto-creation (NOT_FOUND repair) paths of {@link PubSubWriter}.
 *
 * <p>Timed out as a class: {@link FakeMailboxExecutor#yield()} blocks on an empty mailbox, so a
 * drain or admission predicate that can no longer be satisfied hangs rather than fails.
 */
@Timeout(30)
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
        return newOrderingWriter(
                disposition,
                recoverySchedule,
                PubSubPublisherOptions.defaults().getMaxInFlightBytes());
    }

    private PubSubWriter<String> newOrderingWriter(
            CreateDisposition disposition, RetrySchedule recoverySchedule, long maxInFlightBytes) {
        return new PubSubWriter<>(
                TestSinkConfigs.forTopic(
                        TOPIC,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        disposition,
                        PubSubPublisherOptions.builder()
                                .enableMessageOrdering(true)
                                .maxInFlightBytes(maxInFlightBytes)
                                .build()),
                factory,
                admin,
                mailbox,
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

    /**
     * Serialized size of the message the ordering serializer produces for this payload. Goes
     * through the serializer rather than rebuilding the message, so it cannot drift from what the
     * writer actually measures.
     */
    private static int sizeOf(String payload) throws IOException {
        return PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                .withOrderingKey(element -> element.split(":")[0])
                .serialize(payload)
                .getSerializedSize();
    }

    @Test
    void notFoundPublishCreatesTopicAndRepublishesOnNextWrite() throws Exception {
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));

        writer.write("first", CONTEXT);
        mailbox.drain();
        writer.write("second", CONTEXT);

        assertThat(admin.created).containsExactly(TOPIC);
        // The default config carries no creation settings, so the repair passes none.
        assertThat(admin.createOptions).containsExactly((TopicCreateOptions) null);
        assertThat(publishedPayloads()).containsExactly("first", "first", "second");
        mailbox.drain();
        assertThat(writer.getInFlightMessages()).isZero();
        assertThat(writer.getInFlightBytes()).isZero();
    }

    @Test
    void theRepairCreatesTheTopicWithTheConfiguredCreateOptions() throws Exception {
        TopicCreateOptions createOptions =
                TopicCreateOptions.builder().messageRetention(java.time.Duration.ofDays(7)).build();
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forTopic(
                                TOPIC,
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                                CreateDisposition.CREATE_IF_NEEDED,
                                createOptions,
                                PubSubPublisherOptions.defaults()),
                        factory,
                        admin,
                        mailbox,
                        FAST_SCHEDULE);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));

        writer.write("first", CONTEXT);
        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(admin.createOptions).containsExactly(createOptions);
    }

    @Test
    void parkedMessagesAreCountedByNeitherInFlightCap() throws Exception {
        // A parked message's bytes were released by its failure mail, so the caps describe only
        // what the SDK still holds. The parked payload is the same object the repair republishes,
        // so nothing is hidden — but a reader comparing the counters against writer memory needs
        // to know this, and a future "count parked bytes too" change would break the drain.
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        writer.write("first", CONTEXT);
        mailbox.drain();

        assertThat(writer.getInFlightMessages()).isZero();
        assertThat(writer.getInFlightBytes()).isZero();

        // Still parked, so the repair republishes it.
        writer.flush(false);
        assertThat(publishedPayloads()).containsExactly("first", "first");
    }

    @Test
    void theRepairRepublishesTheParkedBatchWithoutRecheckingTheCaps() throws Exception {
        // The byte cap admits one message at a time, yet the repair republishes both parked
        // messages in one batch: re-checking the cap inside the republish loop would interleave
        // failure mails between a key's publishes and break the ordering the sorted batch exists
        // to preserve (#78). The overshoot is bounded by one destination's parked batch.
        PubSubWriter<String> writer =
                newOrderingWriter(
                        CreateDisposition.CREATE_IF_NEEDED, FAST_SCHEDULE, sizeOf("k1:first"));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads())
                .containsExactly("k1:first", "k1:second", "k1:first", "k1:second");
        assertThat(writer.getInFlightBytes()).isZero();
    }

    @Test
    void aZeroByteMessageStillCountsAsInFlightForTheDrain() throws Exception {
        // A PubsubMessage with an empty payload serializes to zero bytes, so inFlightBytes == 0
        // does NOT imply an empty writer. This is why drainInFlight() keys on the message count:
        // a byte-keyed drain would return immediately here, skip the failure mail, and complete a
        // checkpoint with the message neither published nor repaired.
        PubSubWriter<String> writer = newWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));

        writer.write("", CONTEXT);
        assertThat(writer.getInFlightMessages()).isEqualTo(1);
        assertThat(writer.getInFlightBytes()).isZero();

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads()).containsExactly("", "");
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
    void theRepairedBatchIsRepublishedInPublishOrderWhateverOrderTheFailuresArriveIn()
            throws Exception {
        // Both halves of #78 in one test. Parking must not depend on something being parked
        // already — the cascade's mail arrives first here, so that guard fails the job — and the
        // batch must be sorted on publish sequence rather than appended in mail order, which would
        // republish k1:second ahead of k1:first: a silent per-key ordering violation, and the
        // reason parking a cascade cannot simply be made unconditional on its own.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        SettableApiFuture<String> root = SettableApiFuture.create();
        SettableApiFuture<String> cascade = SettableApiFuture.create();
        factory.enqueueFuture(root);
        factory.enqueueFuture(cascade);
        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);

        // The SDK cancels an ordering key's queued publishes from its own thread, so a cascade's
        // failure mail can reach the mailbox before its root's. Completing the futures out of
        // publish order reproduces that exactly: the callbacks run inline on this thread.
        cascade.setException(cascade());
        root.setException(notFound());
        mailbox.drain();

        writer.flush(false);

        assertThat(admin.created).containsExactly(TOPIC);
        assertThat(publishedPayloads())
                .containsExactly("k1:first", "k1:second", "k1:first", "k1:second");
    }

    @Test
    void aRepairWaitsForACascadeThatIsStillInFlight() throws Exception {
        // #78 names a second window — a cascade arriving after the repair emptied the buffer — but
        // the in-flight accounting already closes it. repairPendingTopics opens with
        // drainInFlight(), which waits for inFlightMessages to reach zero, and the count
        // drops only inside a failure or completion mail. So every publish's mail has run before a
        // batch is snapshotted. Here the cascade's mail is deliberately left queued when the
        // repair is triggered: without that drain the root is repaired alone and the cascade
        // re-parks, forcing a second attempt — which is what resumedKeys counts.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(notFound()));
        SettableApiFuture<String> cascade = SettableApiFuture.create();
        factory.enqueueFuture(cascade);
        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);
        mailbox.drain();

        cascade.setException(cascade());
        writer.write("k1:third", CONTEXT);
        writer.flush(false);

        assertThat(publisher().resumedKeys).containsExactly("k1");
    }

    @Test
    void aCascadeIsNotParkedUnderCreateNever() throws Exception {
        // Parking is what leads to createTopic, so every parking branch — not only the NOT_FOUND
        // one — has to honour the disposition, or CREATE_NEVER creates a topic.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_NEVER);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:a", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ordering key");
        assertThat(admin.created).isEmpty();
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
    void aCascadeWithNoRootIsRepublishedRatherThanTerminal() throws Exception {
        // A cancellation never originates a failure: the SDK raises it for an ordering key whose
        // earlier publish failed, so the root is always another publish of this writer and its
        // verdict is reached in the same drain. Whether that root has been observed *yet* is a
        // race (#78), so it cannot gate parking — leaving a rootless cascade, unreachable in
        // production, to be republished. That is the right outcome regardless: the message was
        // never published.
        PubSubWriter<String> writer = newOrderingWriter(CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(cascade()));
        writer.write("k1:a", CONTEXT);
        mailbox.drain();

        writer.flush(false);

        assertThat(publishedPayloads()).containsExactly("k1:a", "k1:a");
        assertThat(publisher().resumedKeys).containsExactly("k1");
        // The surprising consequence, worth stating outright: it provokes a topic-creation RPC
        // even though no NOT_FOUND was ever observed. Harmless — createTopic is idempotent, and
        // CREATE_NEVER is excluded — but not something to discover by accident.
        assertThat(admin.created).containsExactly(TOPIC);
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
