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
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.DestinationResolver;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PubSubWriter} against fake publishers and a fake mailbox.
 *
 * <p>Timed out as a class: {@link FakeMailboxExecutor#yield()} blocks on an empty mailbox exactly
 * as the real one does, so an in-flight predicate that can hold with nothing in flight hangs rather
 * than fails. The timeout turns that into a failed test instead of a stuck CI job.
 */
@Timeout(30)
class PubSubWriterTest {

    private static final String PROJECT = "test-project";

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /**
     * The ordering-lifecycle tests can create a missing topic, but script a successful first
     * republish. One attempt keeps those tests fast and makes an unexpected repeated failure fail
     * immediately rather than inheriting production backoff values that could drift.
     */
    private static final RetrySchedule SINGLE_ATTEMPT_RECOVERY = new RetrySchedule(1, 1, 1, 0);

    private final FakePublisherFactory factory = new FakePublisherFactory();
    private final FakeTopicAdmin admin = new FakeTopicAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    /** Routes each record to the topic named by the record itself. */
    private PubSubWriter<String> newWriter() {
        return newWriter(PubSubPublisherOptions.defaults());
    }

    private PubSubWriter<String> newWriter(PubSubPublisherOptions options) {
        return newWriter(PubSubSerializationSchema.payload(new SimpleStringSchema()), options);
    }

    private PubSubWriter<String> newWriter(
            PubSubSerializationSchema<String> serializer, PubSubPublisherOptions options) {
        return newWriter(
                (element, context) -> TopicDestination.of(PROJECT, element), serializer, options);
    }

    /** Routes each record through the given resolver, for the tests that vary the routing. */
    private PubSubWriter<String> newWriter(DestinationResolver<? super String> resolver) {
        return newWriter(
                resolver,
                PubSubSerializationSchema.payload(new SimpleStringSchema()),
                PubSubPublisherOptions.defaults());
    }

    private PubSubWriter<String> newWriter(
            DestinationResolver<? super String> resolver,
            PubSubSerializationSchema<String> serializer,
            PubSubPublisherOptions options) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(resolver, serializer, options),
                factory,
                admin,
                mailbox,
                metrics,
                SINGLE_ATTEMPT_RECOVERY);
    }

    private PubSubWriter<String> newWriter(PubSubPublisherOptions options, AtomicLong nanoClock) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> TopicDestination.of(PROJECT, element),
                        PubSubSerializationSchema.payload(new SimpleStringSchema()),
                        options),
                factory,
                admin,
                mailbox,
                metrics,
                SINGLE_ATTEMPT_RECOVERY,
                nanoClock::get);
    }

    private PubSubWriter<String> newOrderingLifecycleWriter(
            PubSubPublisherOptions options, AtomicLong nanoClock) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> TopicDestination.of(PROJECT, element),
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> "key"),
                        options,
                        FailureHandler.failJob(),
                        CreateDisposition.CREATE_IF_NEEDED),
                factory,
                admin,
                mailbox,
                metrics,
                SINGLE_ATTEMPT_RECOVERY,
                nanoClock::get);
    }

    /** Caps only the message count, leaving the byte cap at its default. */
    private static PubSubPublisherOptions messageCap(int maxInFlightMessages) {
        return PubSubPublisherOptions.builder().maxInFlightMessages(maxInFlightMessages).build();
    }

    /** Caps only the bytes, leaving the message cap at its default 1000. */
    private static PubSubPublisherOptions byteCap(long maxInFlightBytes) {
        return PubSubPublisherOptions.builder().maxInFlightBytes(maxInFlightBytes).build();
    }

    private static TopicDestination topic(String topic) {
        return TopicDestination.of(PROJECT, topic);
    }

    /**
     * Serialized size of the message this test's serializer produces for the payload — the unit the
     * writer's byte cap is expressed in. Goes through the serializer rather than rebuilding the
     * message, so it cannot drift from what the writer actually measures.
     */
    private static int sizeOf(String payload) throws IOException {
        return PubSubSerializationSchema.payload(new SimpleStringSchema())
                .serialize(payload)
                .getSerializedSize();
    }

    /**
     * A {@link io.github.flink.gcp.connector.pubsub.sink.DestinationResolver} is user code, and a
     * resolver returning {@code null} — the shape a per-record routing table with a missing entry
     * takes — is refused with a message naming the resolver. Nothing drove it before #786: every
     * test resolver here returns a destination, so deleting the guard left the whole module suite
     * passing. What a job would meet instead was measured rather than supposed: {@code Cannot
     * invoke TopicDestination.toTopicPath() because "destination" is null}, thrown three frames
     * deeper in {@code PubSubWriterMetrics.forTopic}, which names neither the resolver nor the
     * record.
     *
     * <p>Not routed to the failure handler: a resolver returning null is a configuration defect,
     * not a record the service rejected, and ADR-0005 routes exactly two failures.
     */
    @Test
    void aResolverReturningNullIsRefusedByName() {
        PubSubWriter<String> writer = newWriter((element, context) -> null);

        assertThatThrownBy(() -> writer.write("anything", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessage("The destination resolver returned null for a record.");
    }

    @Test
    void fansOutRecordsToPerTopicPublishers() throws Exception {
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        writer.write("topic-c", CONTEXT);
        writer.write("topic-a", CONTEXT);

        assertThat(factory.publishers.keySet())
                .containsExactly(topic("topic-a"), topic("topic-b"), topic("topic-c"));
        assertThat(factory.publishers.get(topic("topic-a")).published)
                .extracting(PubsubMessage::getData)
                .extracting(data -> data.toString(StandardCharsets.UTF_8))
                .containsExactly("topic-a", "topic-a");
        assertThat(factory.publishers.get(topic("topic-b")).published).hasSize(1);
        assertThat(factory.publishers.get(topic("topic-c")).published).hasSize(1);
    }

    @Test
    void reusesOnePublisherPerTopic() throws Exception {
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);

        assertThat(factory.createCalls).isEqualTo(1);
    }

    @Test
    void flushFlushesAllPublishersAndAwaitsInFlightPublishes() throws Exception {
        PubSubWriter<String> writer = newWriter();
        SettableApiFuture<String> pending = SettableApiFuture.create();
        factory.enqueueFuture(pending);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        assertThat(writer.getInFlightMessages()).isEqualTo(2);

        CompletableFuture.runAsync(
                () -> {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    pending.set("message-id");
                });
        writer.flush(false);

        assertThat(writer.getInFlightMessages()).isZero();
        assertThat(factory.publishers.get(topic("topic-a")).flushCalls).isEqualTo(1);
        assertThat(factory.publishers.get(topic("topic-b")).flushCalls).isEqualTo(1);
    }

    @Test
    void flushSurfacesFailedPublishWithTopicContext() throws Exception {
        PubSubWriter<String> writer = newWriter();
        RuntimeException failure = new RuntimeException("publish exploded");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
    }

    @Test
    void asyncPublishFailureFailsSubsequentWrite() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new RuntimeException("publish exploded")));
        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a");
    }

    @Test
    void serializationFailureFailsWriteAndPublishesNothing() {
        IOException failure = new IOException("bad record");
        PubSubWriter<String> writer =
                newWriter(
                        element -> {
                            throw failure;
                        },
                        PubSubPublisherOptions.defaults());

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
        assertThat(factory.publishers).isEmpty();
    }

    @Test
    void synchronousPublishFailureFailsWriteWithTopicContext() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        RuntimeException failure = new IllegalStateException("publisher is shut down");
        factory.publishers.get(topic("topic-a")).publishFailure = failure;

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
        // The rejected publish registers no callback, so counting it would leak both counters.
        assertThat(writer.getInFlightMessages()).isEqualTo(1);
        assertThat(writer.getInFlightBytes()).isEqualTo(sizeOf("topic-a"));
    }

    @Test
    void thePublicConstructorAppliesTheCapsFromPublisherOptions() throws Exception {
        // The production path (PubSubPublisherSink.createWriter) uses this constructor; every other
        // test here uses the schedule-injecting one, so this is what pins the delegation. The byte
        // cap is the binding one deliberately: with both caps binding at the same count, breaking
        // either would leave the other in charge and the test would not notice.
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forResolver(
                                (element, context) -> TopicDestination.of(PROJECT, element),
                                PubSubSerializationSchema.payload(new SimpleStringSchema()),
                                byteCap(2L * sizeOf("topic-a"))),
                        factory,
                        admin,
                        mailbox,
                        metrics);
        SettableApiFuture<String> first = SettableApiFuture.create();
        factory.enqueueFuture(first);
        factory.enqueueFuture(SettableApiFuture.create());
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        assertThat(writer.getInFlightBytes()).isEqualTo(2L * sizeOf("topic-a"));
        // Well under the default message cap of 1000, so only the byte cap can hold the third
        // write.
        assertThat(writer.getInFlightMessages()).isEqualTo(2);

        first.set("message-id");
        writer.write("topic-a", CONTEXT);

        assertThat(writer.getInFlightBytes()).isEqualTo(2L * sizeOf("topic-a"));
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(3);
    }

    @Test
    void rejectsOrderingKeyWhenMessageOrderingIsDisabled() {
        PubSubWriter<String> writer =
                newWriter(
                        PubSubSerializationSchema.payload(new SimpleStringSchema())
                                .withOrderingKey(element -> "some-key"),
                        PubSubPublisherOptions.defaults());

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasMessageContaining("some-key")
                .hasMessageContaining("enableMessageOrdering");
        assertThat(factory.publishers).isEmpty();
    }

    @Test
    void writeAtInFlightCapWaitsForCompletionsBeforePublishing() throws Exception {
        PubSubWriter<String> writer = newWriter(messageCap(2));
        SettableApiFuture<String> first = SettableApiFuture.create();
        SettableApiFuture<String> second = SettableApiFuture.create();
        factory.enqueueFuture(first);
        factory.enqueueFuture(second);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        assertThat(writer.getInFlightMessages()).isEqualTo(2);

        // Completing a publish enqueues a completion mail; the capped write processes it while
        // yielding and only then publishes the third record.
        first.set("message-id");
        writer.write("topic-a", CONTEXT);

        assertThat(writer.getInFlightMessages()).isEqualTo(2);
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(3);
    }

    @Test
    void writeAtInFlightCapSurfacesFailedPublishInsteadOfPublishing() throws Exception {
        PubSubWriter<String> writer = newWriter(messageCap(1));
        RuntimeException failure = new RuntimeException("publish exploded");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(1);
    }

    @Test
    void writeAtInFlightByteCapWaitsForCompletionsBeforePublishing() throws Exception {
        // The message cap stays at its default 1000, so only the byte cap can trip here — the
        // gap #85 exists to close, since a message may be 10 MiB and the count bounds no memory.
        PubSubWriter<String> writer = newWriter(byteCap(2L * sizeOf("topic-a")));
        SettableApiFuture<String> first = SettableApiFuture.create();
        SettableApiFuture<String> second = SettableApiFuture.create();
        factory.enqueueFuture(first);
        factory.enqueueFuture(second);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        assertThat(writer.getInFlightBytes()).isEqualTo(2L * sizeOf("topic-a"));
        assertThat(writer.getInFlightMessages()).isEqualTo(2);

        // Completing a publish enqueues a completion mail. That the third write returns at all is
        // the evidence it yielded to the mailbox rather than blocking: nothing else runs the mail.
        first.set("message-id");
        writer.write("topic-a", CONTEXT);

        assertThat(writer.getInFlightBytes()).isEqualTo(2L * sizeOf("topic-a"));
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(3);
    }

    @Test
    void writeAtInFlightByteCapSurfacesFailedPublishInsteadOfPublishing() throws Exception {
        PubSubWriter<String> writer = newWriter(byteCap(sizeOf("topic-a")));
        RuntimeException failure = new RuntimeException("publish exploded");
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(failure));
        writer.write("topic-a", CONTEXT);

        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic-a")
                .hasCause(failure);
        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(1);
    }

    @Test
    void aMessageLargerThanTheByteCapIsAdmittedOnAnEmptyWriter() throws Exception {
        // Admission is "below the cap", not "does this message fit". A fits-predicate would never
        // admit this message, and since yield() blocks until a mail arrives and no mail can arrive
        // with nothing in flight, the write would hang instead of applying backpressure.
        PubSubWriter<String> writer = newWriter(byteCap(sizeOf("topic-a") - 1L));
        SettableApiFuture<String> oversized = SettableApiFuture.create();
        factory.enqueueFuture(oversized);

        writer.write("topic-a", CONTEXT);

        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(1);
        assertThat(writer.getInFlightBytes()).isGreaterThan(sizeOf("topic-a") - 1L);

        // ... and the overshoot is transient: the next write waits for it to complete.
        oversized.set("message-id");
        writer.write("topic-a", CONTEXT);

        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(2);
    }

    @Test
    void completionMailsKeepInFlightCountAndBytesBounded() throws Exception {
        PubSubWriter<String> writer = newWriter();

        for (int i = 0; i < 10; i++) {
            writer.write("topic-a", CONTEXT);
        }
        mailbox.drain();

        assertThat(writer.getInFlightMessages()).isZero();
        assertThat(writer.getInFlightBytes()).isZero();
    }

    @Test
    void aFailedPublishReleasesItsBytesToo() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new RuntimeException("publish exploded")));
        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        // The failure mail releases both counters; only the terminal error is retained.
        assertThat(writer.getInFlightMessages()).isZero();
        assertThat(writer.getInFlightBytes()).isZero();
    }

    @Test
    void capacityEvictsTheLeastRecentlyUsedCleanPublisher() throws Exception {
        PubSubWriter<String> writer =
                newWriter(PubSubPublisherOptions.builder().maxActivePublishers(2).build());
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        writer.write("topic-b", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        FakePublisherFactory.FakeTopicPublisher topicB = factory.publishers.get(topic("topic-b"));

        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        writer.write("topic-c", CONTEXT);

        assertThat(topicA.closeCalls).isZero();
        assertThat(topicB.shutdownCalls).isEqualTo(1);
        assertThat(topicB.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isEqualTo(2);
        assertThat(metrics.counterValue("capacityEvictions")).isEqualTo(1);
    }

    @Test
    void capacityDrainsBeforeEvictingWhenEveryPublisherHasWork() throws Exception {
        PubSubWriter<String> writer =
                newWriter(PubSubPublisherOptions.builder().maxActivePublishers(1).build());
        SettableApiFuture<String> pending = SettableApiFuture.create();
        factory.enqueueFuture(pending);
        writer.write("topic-a", CONTEXT);
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));

        // Complete on the SDK side but leave the completion mail queued. The state is not clean
        // until the capacity path drains that mail itself.
        pending.set("message-a");
        writer.write("topic-b", CONTEXT);

        assertThat(topicA.flushCalls).isEqualTo(1);
        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isEqualTo(1);
        assertThat(writer.getInFlightMessages()).isEqualTo(1);
    }

    @Test
    void idleEvictionIsStrictAndRecreatesThePublisher() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher original = factory.publishers.get(topic("topic-a"));

        nanoClock.set(7);
        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        nanoClock.set(17);
        writer.flush(false);
        assertThat(original.closeCalls).isZero();

        nanoClock.set(18);
        writer.flush(false);
        assertThat(original.shutdownCalls).isEqualTo(1);
        assertThat(original.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(1);

        writer.write("topic-a", CONTEXT);
        assertThat(factory.createCalls).isEqualTo(2);
        assertThat(factory.publishers.get(topic("topic-a"))).isNotSameAs(original);
    }

    @Test
    void terminalFlushDoesNotPerformIdleEviction() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher publisher =
                factory.publishers.get(topic("topic-a"));

        nanoClock.set(11);
        writer.flush(true);

        assertThat(publisher.closeCalls).isZero();
        assertThat(writer.getActivePublishers()).isEqualTo(1);
    }

    @Test
    void idleEvictionShutsDownEveryPublisherBeforeClosingAny() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        mailbox.drain();

        nanoClock.set(11);
        writer.flush(false);

        assertBothShutdownsPrecedeBothCloses();
    }

    @Test
    void orderedRootFailureCanBeEvictedAfterRepair() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .enableMessageOrdering(true)
                        .maxActivePublishers(1)
                        .destinationIdleTimeout(Duration.ofNanos(10))
                        .build();
        PubSubWriter<String> writer = newOrderingLifecycleWriter(options, nanoClock);
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(Status.NOT_FOUND.asRuntimeException()));
        writer.write("topic-a", CONTEXT);

        nanoClock.set(11);
        writer.flush(false);

        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(1);

        writer.write("topic-b", CONTEXT);

        assertThat(writer.getActivePublishers()).isEqualTo(1);
    }

    @Test
    void orderedPreBatchCancellationCanBeEvictedAfterRepair() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .enableMessageOrdering(true)
                        .maxActivePublishers(1)
                        .destinationIdleTimeout(Duration.ofNanos(10))
                        .build();
        PubSubWriter<String> writer = newOrderingLifecycleWriter(options, nanoClock);
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(Status.NOT_FOUND.asRuntimeException()));
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);

        nanoClock.set(11);
        writer.flush(false);

        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isEqualTo(1);

        writer.write("topic-b", CONTEXT);

        assertThat(writer.getActivePublishers()).isEqualTo(1);
    }

    @Test
    void abandonedInBatchPublisherFailsEvictionBeforeOpeningAReplacement() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .enableMessageOrdering(true)
                        .maxActivePublishers(1)
                        .destinationIdleTimeout(Duration.ofNanos(10))
                        .build();
        PubSubWriter<String> writer = newOrderingLifecycleWriter(options, nanoClock);
        SettableApiFuture<String> rootFailure = SettableApiFuture.create();
        SettableApiFuture<String> batchedCancellation = SettableApiFuture.create();
        factory.enqueueFuture(rootFailure);
        factory.enqueueFuture(batchedCancellation);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);

        rootFailure.setException(Status.NOT_FOUND.asRuntimeException());
        batchedCancellation.setException(
                new CancellationException(
                        "Execution cancelled because executing previous runnable failed."));
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        topicA.shutdownIncomplete = true;
        assertThat(topicA.wasShutdownIncomplete()).isFalse();
        assertThatThrownBy(() -> writer.write("topic-b", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("shutdown did not finish during capacity eviction")
                .hasMessageContaining("topic-a")
                .hasMessageContaining("PubSubPublisherOptions.builder().shutdownTimeout(...)")
                .hasMessageContaining("sink.shutdown-timeout");
        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(topicA.wasShutdownIncomplete()).isTrue();
        assertThat(factory.createCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(metrics.counterValue("capacityEvictions")).isZero();
    }

    @Test
    void inspectingDestinationStateDoesNotRefreshCapacityLruOrder() throws Exception {
        PubSubWriter<String> writer =
                newWriter(PubSubPublisherOptions.builder().maxActivePublishers(2).build());
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        writer.write("topic-b", CONTEXT);
        mailbox.drain();

        assertThat(writer.getDestinationState(topic("topic-a"))).isNotNull();
        writer.write("topic-c", CONTEXT);
        mailbox.drain();

        assertThat(factory.publishers.get(topic("topic-a")).closeCalls).isEqualTo(1);
        assertThat(factory.publishers.get(topic("topic-b")).closeCalls).isZero();
    }

    @Test
    void publisherSetAndCheckpointScanStayBoundedAfterTenThousandDestinations() throws Exception {
        PubSubWriter<String> writer =
                newWriter(PubSubPublisherOptions.builder().maxActivePublishers(100).build());

        for (int i = 0; i < 10_000; i++) {
            writer.write("topic-" + i, CONTEXT);
            mailbox.drain();
        }

        assertThat(writer.getActivePublishers()).isEqualTo(100);
        assertThat(factory.createdPublishers).hasSize(10_000);
        assertThat(factory.createdPublishers.subList(0, 9_900))
                .allSatisfy(
                        publisher -> {
                            assertThat(publisher.closeCalls).isEqualTo(1);
                            assertThat(publisher.flushCalls).isZero();
                        });

        writer.flush(false);

        assertThat(factory.createdPublishers.subList(9_900, 10_000))
                .allSatisfy(publisher -> assertThat(publisher.flushCalls).isEqualTo(1));
        assertThat(metrics.counterValue("capacityEvictions")).isEqualTo(9_900);
    }

    @Test
    void idleEvictionRethrowsAnInterruptSuppressedBehindAnEarlierFailure() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        mailbox.drain();
        RuntimeException firstCloseFailure = new RuntimeException("first close failed");
        factory.publishers.get(topic("topic-a")).closeFailure = firstCloseFailure;
        InterruptedException interrupted = new InterruptedException("task cancelled");
        factory.publishers.get(topic("topic-b")).closeFailure = interrupted;

        nanoClock.set(11);
        try {
            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(InterruptedException.class)
                    .isNotSameAs(interrupted)
                    .satisfies(
                            failure -> {
                                assertThat(failure.getSuppressed())
                                        .containsExactly(firstCloseFailure);
                                assertThat(firstCloseFailure.getSuppressed())
                                        .containsExactly(interrupted);
                            });
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Do not leak the deliberately restored flag into the rest of the test fork.
            Thread.interrupted();
        }
    }

    @Test
    void idleEvictionDoesNotTreatACloseFailureCauseAsTaskThreadInterruption() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        topicA.closeFailure =
                new IOException(
                        "channel shutdown failed", new InterruptedException("client worker"));

        nanoClock.set(11);
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to release Pub/Sub publishers during idle eviction")
                .hasCause(topicA.closeFailure);

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void idleEvictionFailsWhenPublisherShutdownIsAbandoned() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        topicA.shutdownIncomplete = true;
        assertThat(topicA.wasShutdownIncomplete()).isFalse();

        nanoClock.set(11);
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("shutdown did not finish during idle eviction")
                .hasMessageContaining("topic-a");

        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(topicA.wasShutdownIncomplete()).isTrue();
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isZero();
    }

    @Test
    void capacityEvictionFailsBeforeReplacementWhenCloseFails() throws Exception {
        PubSubWriter<String> writer =
                newWriter(PubSubPublisherOptions.builder().maxActivePublishers(1).build());
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        topicA.closeFailure = new RuntimeException("close exploded");

        assertThatThrownBy(() -> writer.write("topic-b", CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Failed to release Pub/Sub publishers during capacity eviction")
                .hasMessageContaining("will not open replacement publishers")
                .hasCause(topicA.closeFailure);

        assertThat(topicA.shutdownCalls).isEqualTo(1);
        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(factory.createCalls).isEqualTo(1);
        assertThat(metrics.counterValue("capacityEvictions")).isZero();
    }

    @Test
    void idleEvictionFailsWhenShutdownThrows() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PubSubWriter<String> writer =
                newWriter(
                        PubSubPublisherOptions.builder()
                                .destinationIdleTimeout(Duration.ofNanos(10))
                                .build(),
                        nanoClock);
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        FakePublisherFactory.FakeTopicPublisher topicA = factory.publishers.get(topic("topic-a"));
        topicA.shutdownFailure = new RuntimeException("shutdown exploded");

        nanoClock.set(11);
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to release Pub/Sub publishers during idle eviction")
                .hasCause(topicA.shutdownFailure);

        assertThat(topicA.shutdownCalls).isEqualTo(1);
        assertThat(topicA.closeCalls).isEqualTo(1);
        assertThat(writer.getActivePublishers()).isZero();
        assertThat(metrics.counterValue("idleEvictions")).isZero();
    }

    @Test
    void closeClosesEveryPublisherWithoutFlushing() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);

        writer.close();

        for (FakePublisherFactory.FakeTopicPublisher publisher : factory.publishers.values()) {
            assertThat(publisher.closeCalls).isEqualTo(1);
            assertThat(publisher.flushCalls).isZero();
        }
        assertThat(admin.closeCalls).isEqualTo(1);
    }

    @Test
    void closeClosesRemainingPublishersWhenOneFails() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        RuntimeException failure = new RuntimeException("shutdown exploded");
        factory.publishers.get(topic("topic-a")).closeFailure = failure;

        assertThatThrownBy(writer::close).isSameAs(failure);

        for (FakePublisherFactory.FakeTopicPublisher publisher : factory.publishers.values()) {
            assertThat(publisher.closeCalls).isEqualTo(1);
        }
        assertThat(admin.closeCalls).isEqualTo(1);
    }

    @Test
    void closeAsksEveryPublisherToShutDownBeforeItWaitsOnAny() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);

        writer.close();

        // The waits then overlap, so the close costs one shutdown timeout rather than one per
        // topic. Interleaving them would still pass every per-publisher assertion above.
        assertBothShutdownsPrecedeBothCloses();
    }

    @Test
    void aShutdownThatThrowsSkipsNeitherTheOtherShutdownsNorAnyClose() throws Exception {
        PubSubWriter<String> writer = newWriter();
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        RuntimeException failure = new RuntimeException("shutdown exploded");
        factory.publishers.get(topic("topic-a")).shutdownFailure = failure;

        assertThatThrownBy(writer::close).isSameAs(failure);

        // The order has to survive a failure too, not only the happy path: a loop that stopped at
        // the first throw would leave the other publisher never asked and every close skipped.
        assertBothShutdownsPrecedeBothCloses();
        assertThat(admin.closeCalls).isEqualTo(1);
    }

    /**
     * The map's order is an LRU implementation detail; what this asserts is the lifecycle contract:
     * no publisher is waited on until every one has been asked to stop.
     */
    private void assertBothShutdownsPrecedeBothCloses() {
        assertThat(factory.teardownCalls).hasSize(4);
        assertThat(factory.teardownCalls.subList(0, 2))
                .containsExactlyInAnyOrder(
                        "shutdown:test-project/topic-a", "shutdown:test-project/topic-b");
        assertThat(factory.teardownCalls.subList(2, 4))
                .containsExactlyInAnyOrder(
                        "close:test-project/topic-a", "close:test-project/topic-b");
    }
}
