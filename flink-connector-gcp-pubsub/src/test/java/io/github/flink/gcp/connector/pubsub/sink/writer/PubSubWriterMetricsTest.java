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
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.FailedMessage;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the metrics {@link PubSubWriter} registers, against the same fake publishers, fake
 * topic admin and fake mailbox its behavioural tests use.
 *
 * <p>Every assertion goes through the name a metric registered under rather than through a counter
 * object, so renaming one — or failing to register it — fails here.
 *
 * <p>Timed out as a class for the reason {@link PubSubWriterTest} records: the fake mailbox blocks
 * on an empty queue exactly as the real one does.
 */
@Timeout(30)
class PubSubWriterMetricsTest {

    private static final String PROJECT = "test-project";
    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** Fast, so a repair's backoff stays out of the test's wall clock. */
    private static final RetrySchedule FAST_SCHEDULE = new RetrySchedule(1, 1, 5, 0);

    private final FakePublisherFactory factory = new FakePublisherFactory();
    private final FakeTopicAdmin admin = new FakeTopicAdmin();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    /** Routes each record to the topic named by the record itself. */
    private PubSubWriter<String> newWriter() {
        return newWriter(PubSubPublisherOptions.defaults());
    }

    private PubSubWriter<String> newWriter(PubSubPublisherOptions options) {
        return newWriter(
                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                options,
                FailureHandler.failJob(),
                CreateDisposition.CREATE_IF_NEEDED);
    }

    private PubSubWriter<String> newWriter(
            PubSubSerializationSchema<String> serializer,
            PubSubPublisherOptions options,
            FailureHandler<? super FailedMessage> handler,
            CreateDisposition disposition) {
        return new PubSubWriter<>(
                TestSinkConfigs.forResolver(
                        (element, context) -> TopicDestination.of(PROJECT, element),
                        serializer,
                        options,
                        handler,
                        disposition),
                factory,
                admin,
                mailbox,
                metrics,
                FAST_SCHEDULE);
    }

    private static TopicDestination topic(String topic) {
        return TopicDestination.of(PROJECT, topic);
    }

    private static int sizeOf(String payload) throws IOException {
        return PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                .serialize(payload)
                .getSerializedSize();
    }

    private static StatusRuntimeException status(Status status) {
        return new StatusRuntimeException(status);
    }

    private long counter(String... identifier) {
        return metrics.counterValue(identifier);
    }

    private long errors(String errorClass) {
        return counter("errorClass", errorClass, "errors");
    }

    @Test
    void countsEveryRecordHandedToTheClientWithItsSerializedSize() throws Exception {
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);

        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numBytesSend")).isEqualTo(sizeOf("topic-a") + sizeOf("topic-b"));
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    @Test
    void countsARepublishedRecordOnlyOnce() throws Exception {
        // The metric is named numRecordsSend, not numPublishAttempts: the topic-creation repair
        // publishes this record a second time, and counting it again would inflate the throughput
        // of a job that is merely recovering. The retry is visible as errorClass.NOT_FOUND.errors
        // instead. A mutant moving the increment into publishTo's unconditional path dies here.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.NOT_FOUND)));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);

        assertThat(factory.publishers.get(topic("topic-a")).published).hasSize(2);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numBytesSend")).isEqualTo(sizeOf("topic-a"));
        assertThat(counter("topicsCreated")).isEqualTo(1);
        assertThat(errors("NOT_FOUND")).isEqualTo(1);
    }

    /**
     * The one metric here that does not read this writer's state: it reports the process-wide count
     * of publisher closes that overran their budget, so it is registered in the constructor and
     * reads whatever earlier attempts left behind (#311). Asserted as a delta, since the count is
     * process-wide by design, so the test resets it first and then asserts absolutely.
     */
    @Test
    void theAbandonedShutdownCounterReportsTheProcessWideResidue() throws Exception {
        PubSubShutdownResidue.resetForTests();
        newWriter();

        assertThat(counter("publisherShutdownsAbandoned")).isZero();

        CountDownLatch blocked = new CountDownLatch(1);
        BoundedShutdown abandons =
                new BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        "topic projects/p/topics/t",
                        null,
                        Duration.ofMillis(50),
                        PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED);
        try {
            abandons.close();

            // Read back through the registered metric a reporter would call, not through the count
            // directly: what is pinned is that it is wired to the live figure rather than to one
            // snapshotted when the writer was built.
            assertThat(counter("publisherShutdownsAbandoned")).isEqualTo(1);

            // And it reports the sink's publishers alone. A dead-letter queue's abandoned
            // teardowns count into a residue of their own (#329), because that queue registers on
            // whichever sink hosts it and would otherwise collide with this very name here.
            PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.increment();
            assertThat(counter("publisherShutdownsAbandoned")).isEqualTo(1);
        } finally {
            blocked.countDown();
            PubSubShutdownResidue.resetForTests();
        }
    }

    @Test
    void gaugesReportTheWritersInFlightAndParkedCounts() throws Exception {
        PubSubWriter<String> writer = newWriter();
        SettableApiFuture<String> pending = SettableApiFuture.create();
        factory.enqueueFuture(pending);

        writer.write("topic-a", CONTEXT);

        assertThat(metrics.<Integer>gaugeValue("inFlightMessages")).isEqualTo(1);
        assertThat(metrics.<Long>gaugeValue("inFlightBytes")).isEqualTo(sizeOf("topic-a"));
        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isZero();

        pending.setException(status(Status.NOT_FOUND));
        mailbox.drain();

        // The failure mail released the publish from both in-flight counters and parked the
        // message for the repair, so the three gauges together account for it at every step.
        assertThat(metrics.<Integer>gaugeValue("inFlightMessages")).isZero();
        assertThat(metrics.<Long>gaugeValue("inFlightBytes")).isZero();
        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isEqualTo(1);

        writer.flush(false);

        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isZero();
    }

    @Test
    void theParkedGaugeCountsABatchAwaitingItsVerdictAndItsCascade() throws Exception {
        // parkedMessages is not a topic-creation gauge: since #215 a dropped message's ordering key
        // is repaired the same way, and since #264 the rejected root itself is parked too,
        // awaiting the solo verdict that decides whether it is dropped. Under CREATE_NEVER
        // deliberately — that is the disposition under which nothing was parked at all before
        // #215, so this is the assertion the widened behaviour actually needs, and nothing else
        // pins it.
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forResolver(
                                (element, context) -> topic("ordered"),
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                        .withOrderingKey(element -> element.split(":")[0]),
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .build(),
                                FailureHandler.logAndDrop(),
                                CreateDisposition.CREATE_NEVER),
                        factory,
                        admin,
                        mailbox,
                        metrics,
                        FAST_SCHEDULE);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));

        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);
        mailbox.drain();

        // Both are held: the rejected root awaits its solo verdict — a request-level
        // INVALID_ARGUMENT does not say which co-batched message is invalid (#264) — and the
        // cascade the fake's paused key produced waits beside it for the resume-and-republish.
        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isEqualTo(2);

        // The solo verdict that confirms the root as invalid; the cascade passes by default.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));
        writer.flush(false);

        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isZero();
    }

    @Test
    void theParkedGaugeIsClearedWhenTheWriterIsClosed() throws Exception {
        // Parked messages are dropped with the writer (no checkpoint covered them), so a gauge
        // still reporting them would outlive what it describes.
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.NOT_FOUND)));
        writer.write("topic-a", CONTEXT);
        mailbox.drain();
        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isEqualTo(1);

        writer.close();

        assertThat(metrics.<Integer>gaugeValue("parkedMessages")).isZero();
    }

    @Test
    void countsAFailedPublishUnderItsStatusCode() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.PERMISSION_DENIED)));

        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        assertThat(errors("PERMISSION_DENIED")).isEqualTo(1);
    }

    @Test
    void countsAFailureCarryingNoStatusAsUnclassified() throws Exception {
        PubSubWriter<String> writer = newWriter();
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(new RuntimeException("publish exploded")));

        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        assertThat(errors("UNCLASSIFIED")).isEqualTo(1);
    }

    @Test
    void doesNotCountACascadeCancellationBesideItsRoot() throws Exception {
        // With ordering enabled the SDK cancels an ordering key's queued publishes after the key's
        // first failure. Those cancellations are not incidents of their own — counting them would
        // multiply one NOT_FOUND by the length of the key's queue, and they carry no status, so
        // they would pile up under UNCLASSIFIED where genuinely unclassifiable failures live.
        PubSubWriter<String> writer =
                newWriter(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                .withOrderingKey(element -> element.split(":")[0]),
                        PubSubPublisherOptions.builder().enableMessageOrdering(true).build(),
                        FailureHandler.failJob(),
                        CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.NOT_FOUND)));
        factory.enqueueFuture(
                ApiFutures.immediateFailedFuture(
                        new CancellationException(
                                "Execution cancelled because executing previous runnable"
                                        + " failed.")));

        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);
        writer.flush(false);

        assertThat(errors("NOT_FOUND")).isEqualTo(1);
        assertThat(metrics.hasMetric("errorClass", "CANCELLED", "errors")).isFalse();
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    @Test
    void doesNotCountACascadeCancellationBesideADroppedRoot() throws Exception {
        // The same rule with a dropped message as the root instead of a NOT_FOUND (#215). The
        // cascade is republished rather than counted, so one bad message stays one incident however
        // many of its neighbours the SDK cancelled — and its republish is not a second record,
        // since numRecordsSend counts records rather than publish attempts.
        //
        // One fixed topic, not this class's usual element-as-topic resolver: a cascade only trails
        // its root through the publisher's queue for one ordering key, so two topics would be two
        // unrelated failures that happen to assert the same numbers.
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forResolver(
                                (element, context) -> topic("ordered"),
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema())
                                        .withOrderingKey(element -> element.split(":")[0]),
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .build(),
                                FailureHandler.logAndDrop(),
                                CreateDisposition.CREATE_IF_NEEDED),
                        factory,
                        admin,
                        mailbox,
                        metrics,
                        FAST_SCHEDULE);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));

        writer.write("k1:first", CONTEXT);
        writer.write("k1:second", CONTEXT);
        // The solo verdict confirming the root; the cascade — the fake's paused key turned the
        // second write away itself — passes its own request by default.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));
        writer.flush(false);

        // One incident, one count — the batch-level report on the root is not counted when it is
        // parked (it names no message), only the solo rejection that confirms it is.
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(metrics.hasMetric("errorClass", "CANCELLED", "errors")).isFalse();
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        // The repair a drop provokes creates no topic, so it counts none either. The counter is
        // registered unconditionally, so this has to read its value rather than its presence.
        assertThat(counter("topicsCreated")).isZero();
    }

    @Test
    void countsARecordTheSerializerRejectedAsASendError() throws Exception {
        PubSubWriter<String> writer =
                newWriter(
                        element -> {
                            throw new IOException("bad record");
                        },
                        PubSubPublisherOptions.defaults(),
                        FailureHandler.logAndDrop(),
                        CreateDisposition.CREATE_IF_NEEDED);

        writer.write("topic-a", CONTEXT);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        // Never handed to the client, so it is not a send.
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void countsAMessageTheServiceRejectedAsASendError() throws Exception {
        PubSubWriter<String> writer =
                newWriter(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        PubSubPublisherOptions.defaults(),
                        FailureHandler.logAndDrop(),
                        CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));

        writer.write("topic-a", CONTEXT);
        writer.flush(false);

        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
    }

    @Test
    void aCoBatchedMessageRescuedByTheIsolationPassCountsNoError() throws Exception {
        // The mirror of the cascade exclusion for batches: the SDK reports one request-level
        // INVALID_ARGUMENT against every co-batched message, so counting each report would
        // multiply one incident by the batch size — and a message the isolation pass then
        // publishes successfully was never an error at all. Only the solo rejection counts.
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        TestSinkConfigs.forResolver(
                                (element, context) -> topic("fixed"),
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                                PubSubPublisherOptions.defaults(),
                                FailureHandler.logAndDrop(),
                                CreateDisposition.CREATE_IF_NEEDED),
                        factory,
                        admin,
                        mailbox,
                        metrics,
                        FAST_SCHEDULE);
        StatusRuntimeException batchReport = status(Status.INVALID_ARGUMENT);
        for (int i = 0; i < 3; i++) {
            factory.enqueueFuture(ApiFutures.immediateFailedFuture(batchReport));
        }
        writer.write("m0", CONTEXT);
        writer.write("m1", CONTEXT);
        writer.write("m2", CONTEXT);
        // Solo verdicts: m0 accepted, m1 rejected, m2 accepted (default).
        factory.enqueueFuture(ApiFutures.immediateFuture("id-m0"));
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));

        writer.flush(false);

        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isEqualTo(3);
    }

    @Test
    void registersNoPerDestinationCountersByDefault() throws Exception {
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);

        // Off means nothing registered, not a counter sitting at zero: Flink cannot unregister a
        // metric, so the switch has to be checked before registration.
        assertThat(metrics.hasMetric("destination", topic("topic-a").toTopicPath(), "recordsSend"))
                .isFalse();
        assertThat(counter("numRecordsSend")).isEqualTo(1);
    }

    @Test
    void countsPerTopicWhenPerDestinationMetricsAreOn() throws Exception {
        PubSubWriter<String> writer =
                newWriter(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        PubSubPublisherOptions.builder().perDestinationMetrics(true).build(),
                        FailureHandler.logAndDrop(),
                        CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));

        writer.write("topic-a", CONTEXT);
        writer.write("topic-a", CONTEXT);
        writer.write("topic-b", CONTEXT);
        // The first topic-a publish is parked awaiting its solo verdict, which rejects it.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));
        writer.flush(false);

        String topicA = topic("topic-a").toTopicPath();
        String topicB = topic("topic-b").toTopicPath();
        assertThat(counter("destination", topicA, "recordsSend")).isEqualTo(2);
        assertThat(counter("destination", topicA, "sendErrors")).isEqualTo(1);
        assertThat(counter("destination", topicB, "recordsSend")).isEqualTo(1);
        assertThat(metrics.hasMetric("destination", topicB, "sendErrors")).isTrue();
        assertThat(counter("destination", topicB, "sendErrors")).isZero();
    }

    @Test
    void theProductionPathRegistersOnTheContextsOwnMetricGroup() throws Exception {
        // Everything else here injects the group directly, so this is what pins the one line
        // carrying it from Flink: a writer registering on a group of its own would report nothing
        // any reporter sees, and every other test in this class would still pass.
        StubWriterInitContext context = new StubWriterInitContext(0);

        SinkWriter<String> writer =
                PubSubSink.<String>builder()
                        .topic(topic("topic-a"))
                        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                        .build()
                        .createWriter(context);

        assertThat(context.getSinkWriterMetricGroup().hasMetric("inFlightMessages")).isTrue();
        assertThat(context.getSinkWriterMetricGroup().hasMetric("topicsCreated")).isTrue();
        writer.close();
    }

    @Test
    void leavesCurrentSendTimeUnset() throws Exception {
        // Deliberate (#37): the SDK batches publishes and completes their futures asynchronously,
        // so any latency this writer could report would measure its own bookkeeping.
        PubSubWriter<String> writer = newWriter();

        writer.write("topic-a", CONTEXT);

        assertThat(metrics.getCurrentSendTimeGauge()).isNull();
    }

    @Test
    void countsTheSendErrorEvenWhenTheHandlerFailsTheJob() throws Exception {
        // The counter is what a user watching a non-default handler is told to trust, so it must
        // not depend on what the handler then does with the message.
        PubSubWriter<String> writer =
                newWriter(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        PubSubPublisherOptions.defaults(),
                        FailureHandler.failJob(),
                        CreateDisposition.CREATE_IF_NEEDED);
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));

        writer.write("topic-a", CONTEXT);
        mailbox.drain();

        // The solo verdict, delivered by the repair the next write runs: the handler throws, and
        // the counter must have moved anyway.
        factory.enqueueFuture(ApiFutures.immediateFailedFuture(status(Status.INVALID_ARGUMENT)));
        assertThatThrownBy(() -> writer.write("topic-a", CONTEXT)).isInstanceOf(IOException.class);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
