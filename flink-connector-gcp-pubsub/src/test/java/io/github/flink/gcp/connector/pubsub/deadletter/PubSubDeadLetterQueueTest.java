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

package io.github.flink.gcp.connector.pubsub.deadletter;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue.MAX_ATTRIBUTE_VALUE_BYTES;
import static io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue.TRUNCATION_MARKER;
import static io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue.truncateToAttributeLimit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests for the parts of {@link PubSubDeadLetterQueue} that need no <em>reachable</em> publisher:
 * the envelope, the attribute-value truncation, the builder, the shutdown budget and the flush
 * budget. The round trip through a topic is {@link PubSubDeadLetterQueueITCase}.
 *
 * <p>{@code @Timeout} for the reason {@code DefaultPublisherFactoryTest} gives, and one of its own:
 * several tests here build and close real SDK publishers, and several await publishes that never
 * resolve, so a teardown or a wait that stopped bounding itself would hang the build rather than
 * fail it. Sixty rather than the thirty its siblings use, because this class also holds a test
 * whose <em>subject</em> is a 30 s shutdown budget — at thirty, a shutdown spending the budget it
 * is asserted to have would be reported as a hang.
 */
@Timeout(60)
class PubSubDeadLetterQueueTest {

    private static final TopicDestination TOPIC = TopicDestination.of("my-project", "dead-letters");

    private static final Instant OFFERED_AT = Instant.parse("2026-08-02T04:05:06Z");

    @TempDir Path tempDir;

    /**
     * The group {@link #metrics} registers on, read back by the tests that drive {@code
     * flushOutstanding} directly. Per test instance, so every count starts at zero.
     */
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();

    /**
     * What the static flush seam is handed. Its outstanding supplier is a constant zero: the tests
     * using it hold their own list, and the gauge over a queue's own list is asserted through a
     * queue that opened.
     */
    private final PubSubDeadLetterQueueMetrics metrics =
            new PubSubDeadLetterQueueMetrics(metricGroup, () -> 0);

    /** A failed element from some other connector, which is the case this queue exists for. */
    private static final class StubElement implements FailedElement {

        @Nullable private final ByteString payload;
        private final String errorMessage;

        private StubElement(@Nullable ByteString payload, String errorMessage) {
            this.payload = payload;
            this.errorMessage = errorMessage;
        }

        @Override
        public String getConnector() {
            return "bigquery";
        }

        @Override
        public String describeDestination() {
            return "my-project.my_dataset.orders";
        }

        @Override
        @Nullable
        public ByteString getPayloadBytes() {
            return payload;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        @Nullable
        public Throwable getCause() {
            return null;
        }
    }

    // ---------------------------------------------------------------- envelope

    @Test
    void carriesThePayloadAndTheFiveAttributes() {
        PubsubMessage message =
                PubSubDeadLetterQueue.envelope(
                        new StubElement(ByteString.copyFromUtf8("row bytes"), "Rejected: bad row."),
                        7,
                        OFFERED_AT);

        assertThat(message.getData().toStringUtf8()).isEqualTo("row bytes");
        assertThat(message.getAttributesMap())
                .containsOnly(
                        entry("dlq-connector", "bigquery"),
                        entry("dlq-destination", "my-project.my_dataset.orders"),
                        entry("dlq-error", "Rejected: bad row."),
                        entry("dlq-timestamp", "2026-08-02T04:05:06Z"),
                        entry("dlq-subtask", "7"));
        // Nothing keys the dead-letter topic itself, so no ordering key is set.
        assertThat(message.getOrderingKey()).isEmpty();
    }

    @Test
    void carriesEmptyDataWhenSerializationItselfFailed() {
        PubsubMessage message =
                PubSubDeadLetterQueue.envelope(
                        new StubElement(null, "The record could not be serialized."),
                        0,
                        OFFERED_AT);

        // Empty rather than absent: a consumer tells the two cases apart by data length, and the
        // attributes still say which destination and which error.
        assertThat(message.getData()).isEqualTo(ByteString.EMPTY);
        assertThat(message.getAttributesMap())
                .containsEntry("dlq-error", "The record could not be serialized.");
    }

    @Test
    void truncatesAnOverlongErrorInTheEnvelope() {
        PubsubMessage message =
                PubSubDeadLetterQueue.envelope(
                        new StubElement(ByteString.EMPTY, repeat('x', 2000)), 0, OFFERED_AT);

        assertThat(utf8Length(message.getAttributesMap().get("dlq-error")))
                .isLessThanOrEqualTo(MAX_ATTRIBUTE_VALUE_BYTES);
    }

    // ---------------------------------------------------------------- truncation

    @Test
    void keepsAValueThatExactlyFits() {
        String value = repeat('x', MAX_ATTRIBUTE_VALUE_BYTES);

        assertThat(truncateToAttributeLimit(value)).isEqualTo(value);
    }

    @Test
    void truncatesOneByteOverTheLimit() {
        String value = repeat('x', MAX_ATTRIBUTE_VALUE_BYTES + 1);

        String truncated = truncateToAttributeLimit(value);

        assertThat(utf8Length(truncated)).isEqualTo(MAX_ATTRIBUTE_VALUE_BYTES);
        assertThat(truncated).endsWith(TRUNCATION_MARKER).startsWith("xxx");
    }

    @Test
    void countsBytesRatherThanCharacters() {
        // 512 three-byte characters are 1536 bytes: within the limit by character count, over it
        // by the one the service applies.
        String value = repeat('あ', 512);

        String truncated = truncateToAttributeLimit(value);

        assertThat(utf8Length(value)).isEqualTo(1536);
        assertThat(utf8Length(truncated)).isLessThanOrEqualTo(MAX_ATTRIBUTE_VALUE_BYTES);
    }

    @Test
    void neverCutsAMultiByteCharacterInHalf() {
        // 1021 is the byte budget before the marker, and 1021 = 340 * 3 + 1, so the 341st
        // character straddles the cut. Splitting it would make the attribute invalid UTF-8, which
        // the service rejects — turning a dead letter into a job failure.
        String value = repeat('あ', 400);

        String truncated = truncateToAttributeLimit(value);

        assertThat(truncated).isEqualTo(repeat('あ', 340) + TRUNCATION_MARKER);
        assertThat(utf8Length(truncated))
                .isEqualTo(340 * 3 + TRUNCATION_MARKER.length())
                .isLessThanOrEqualTo(MAX_ATTRIBUTE_VALUE_BYTES);
        // The round trip is the property that matters: bytes that decode back to what was written.
        assertThat(new String(truncated.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))
                .isEqualTo(truncated);
    }

    // ---------------------------------------------------------------- builder and lifecycle

    @Test
    void requiresATopic() {
        assertThatThrownBy(() -> PubSubDeadLetterQueue.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("topic is required");
    }

    @Test
    void rejectsNullAndBlankSettings() {
        PubSubDeadLetterQueue.Builder builder = PubSubDeadLetterQueue.builder();

        assertThatThrownBy(() -> builder.topic(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> builder.serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
        assertThatThrownBy(() -> builder.emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class);
        // Parsed at the setter, so a typo fails on the client rather than in open() on a
        // TaskManager; the full parse table is EmulatorEndpointTest's.
        assertThatThrownBy(() -> builder.emulatorEndpoint("localhost8085"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost8085'");
        assertThatThrownBy(() -> builder.maxOutstandingMessages(-2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("write through");
        assertThatThrownBy(() -> builder.shutdownTimeout(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
        // The queue's two budgets validate alike; there is no unbounded value for either.
        assertThatThrownBy(() -> builder.flushTimeout(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.flushTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushTimeout");
        assertThatThrownBy(() -> builder.flushTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushTimeout");
        // The knob's own documentation offers a long budget as the way to say "effectively
        // unbounded", so one too long to express in nanoseconds is rejected here rather than
        // throwing an ArithmeticException out of the first flush on a TaskManager. The message
        // says how long the ceiling is, because Duration.toString() renders it as
        // PT2562047H47M16.854775807S and a reader cannot see a year count in that (ADR-0068).
        assertThatThrownBy(() -> builder.flushTimeout(Duration.ofDays(400_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most")
                .hasMessageContaining("292 years");
        assertThatCode(() -> builder.flushTimeout(Duration.ofDays(1000)))
                .doesNotThrowAnyException();
        // The queue's other budget reaches BoundedShutdown.start(), which converts it the same way.
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofDays(400_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most")
                .hasMessageContaining("292 years");
    }

    @Test
    void rejectsServiceAccountKeyBesideTheEmulator() {
        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.builder()
                                        .topic(TOPIC)
                                        .serviceAccountKeyFile("/mounted/pubsub-key.json")
                                        .emulatorEndpoint("localhost:8085")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "serviceAccountKeyFile(...) cannot be combined with"
                                + " emulatorEndpoint(...)")
                .hasMessageContaining("plaintext channel with no credentials");
    }

    @Test
    void acceptsTheTwoSpecialOutstandingValues() {
        assertThat(
                        PubSubDeadLetterQueue.builder()
                                .topic(TOPIC)
                                .maxOutstandingMessages(PubSubDeadLetterQueue.WRITE_THROUGH)
                                .build())
                .isNotNull();
        assertThat(
                        PubSubDeadLetterQueue.builder()
                                .topic(TOPIC)
                                .maxOutstandingMessages(PubSubDeadLetterQueue.UNBOUNDED)
                                .build())
                .isNotNull();
    }

    @Test
    void survivesJobGraphSerialization() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:8085")
                        .maxOutstandingMessages(5)
                        .build();

        PubSubDeadLetterQueue restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(queue), getClass().getClassLoader());

        // The publisher is created in open(), so the configured instance carries only values.
        assertThat(restored.toString()).isEqualTo(queue.toString());
        assertThat(restored.getOutstandingMessages()).isZero();
    }

    @Test
    void serializedQueueCarriesThePathButNotCredentialMaterial() throws Exception {
        Path keyFile = tempDir.resolve("mounted-pubsub-key.json");
        String credentialMaterial = serviceAccountKeyJson();
        Files.writeString(keyFile, credentialMaterial, StandardCharsets.UTF_8);
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .serviceAccountKeyFile(keyFile.toString())
                        .build();

        byte[] serialized = InstantiationUtil.serializeObject(queue);
        String bytes = new String(serialized, StandardCharsets.ISO_8859_1);

        assertThat(bytes).contains(keyFile.toString());
        assertThat(bytes)
                .doesNotContain("credential-private-key-id-must-not-be-serialized")
                .doesNotContain("service-account@example.invalid");
    }

    @Test
    void loadsTheServiceAccountOnlyWhenTheHostWriterOpensTheQueue() throws Exception {
        Path keyFile = tempDir.resolve("created-after-serialization.json");
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .serviceAccountKeyFile(keyFile.toString())
                        .build();
        byte[] serialized = InstantiationUtil.serializeObject(queue);
        Files.writeString(keyFile, serviceAccountKeyJson(), StandardCharsets.UTF_8);
        PubSubDeadLetterQueue restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        restored.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            assertThat(restored.publisherShutdown).isNotNull();
        } finally {
            restored.close();
        }
    }

    @Test
    void missingServiceAccountKeyFailsWithoutLeakingThePathOrCause() {
        Path keyFile = tempDir.resolve("mounted-secret-name.json");
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .serviceAccountKeyFile(keyFile.toString())
                        .build();

        assertThatThrownBy(
                        () ->
                                queue.open(
                                        DefaultFailureHandlerContext.of(
                                                new StubWriterInitContext(0))))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Pub/Sub service-account key file.")
                .hasNoCause()
                .asString()
                .doesNotContain(keyFile.toString());
    }

    @Test
    void configuredCredentialsReachThePublisherBuilderAndNullKeepsAdc() throws Exception {
        Publisher.Builder builder = Publisher.newBuilder(TOPIC.toTopicPath());
        Object adc = field(builder, "credentialsProvider");

        PubSubDeadLetterQueue.configureCredentials(builder, null);

        assertThat(field(builder, "credentialsProvider")).isSameAs(adc);

        NoCredentialsProvider configured = NoCredentialsProvider.create();
        PubSubDeadLetterQueue.configureCredentials(builder, configured);

        assertThat(field(builder, "credentialsProvider")).isSameAs(configured);
    }

    @Test
    void refusesToOfferOrFlushBeforeItIsOpened() {
        PubSubDeadLetterQueue queue = PubSubDeadLetterQueue.builder().topic(TOPIC).build();

        assertThatThrownBy(() -> queue.offer(new StubElement(ByteString.EMPTY, "never opened")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not open");
        assertThatThrownBy(queue::flush)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not open");
    }

    @Test
    void closingBeforeOpeningIsANoOp() {
        assertThatCode(() -> PubSubDeadLetterQueue.builder().topic(TOPIC).build().close())
                .doesNotThrowAnyException();
    }

    @Test
    void closeShutsDownTheChannelEvenWhenThePublisherShutdownThrowsAnError() {
        // #276: Flink's IOUtils.closeAll rethrows an Error from inside its loop, so an emulator
        // channel was left running with its gRPC transport open. That the Error reaches the caller
        // as an Error is the other half: Flink halts the JVM on a fatal one, and only if it
        // arrives unwrapped.
        //
        // The two steps are substituted rather than driven through a real publisher: Publisher is
        // final, so this is the only seam, and opening one here would leave a gax executor behind
        // in the test JVM.
        List<String> ran = new ArrayList<>();
        PubSubDeadLetterQueue queue = PubSubDeadLetterQueue.builder().topic(TOPIC).build();
        queue.publisherShutdown =
                () -> {
                    throw new NoClassDefFoundError("publisher shutdown blew up");
                };
        queue.channelShutdown = () -> ran.add("channel");

        assertThatThrownBy(queue::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("publisher shutdown blew up");
        assertThat(ran).containsExactly("channel");
        // The steps are cleared whatever happened, so a second close is the no-op an unopened
        // queue's is: it neither reruns a step nor throws the same Error again.
        assertThatCode(queue::close).doesNotThrowAnyException();
        assertThat(ran).containsExactly("channel");
    }

    /**
     * The close goes through a {@link BoundedShutdown} carrying the configured budget, instead of
     * calling the SDK's own unbounded {@code shutdown()} inline on the task thread (#312). Opened
     * against a lazily-connecting channel, so this needs nothing listening — and closing it here is
     * safe precisely because the teardown is now bounded.
     */
    @Test
    void theCloseIsBoundedByTheConfiguredShutdownTimeout() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .shutdownTimeout(Duration.ofSeconds(7))
                        .build();
        queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            assertThat(queue.publisherShutdown)
                    .isInstanceOf(BoundedShutdown.class)
                    .extracting(step -> ((BoundedShutdown) step).timeout())
                    .isEqualTo(Duration.ofSeconds(7));
        } finally {
            queue.close();
        }
    }

    /**
     * The budget reaches the tasks, which {@code survivesJobGraphSerialization} cannot say: it
     * compares {@code toString()}, and this knob is deliberately not in it. A {@code transient}
     * slip would leave the restored instance with a null budget and fail at {@code open()} on a
     * TaskManager rather than here.
     */
    @Test
    void theShutdownTimeoutSurvivesJobGraphSerialization() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .shutdownTimeout(Duration.ofSeconds(12))
                        .build();

        PubSubDeadLetterQueue restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(queue), getClass().getClassLoader());

        restored.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            assertThat(((BoundedShutdown) restored.publisherShutdown).timeout())
                    .isEqualTo(Duration.ofSeconds(12));
        } finally {
            restored.close();
        }
    }

    @Test
    void theShutdownBudgetDefaultsToThirtySeconds() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .build();
        queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            // The value the hardcoded constant had before it became a knob, so a sink that never
            // configures one keeps exactly the budget it used to claim.
            assertThat(((BoundedShutdown) queue.publisherShutdown).timeout())
                    .isEqualTo(PubSubDeadLetterQueue.DEFAULT_SHUTDOWN_TIMEOUT)
                    .isEqualTo(Duration.ofSeconds(30));
        } finally {
            queue.close();
        }
    }

    /**
     * The queue's teardown is handed the dead-letter residue counter, <b>not</b> the sink
     * publishers' — these metrics register on whichever sink hosts the queue, and a Pub/Sub sink
     * has already registered {@code publisherShutdownsAbandoned} on that group, which Flink would
     * resolve by dropping this one with a warning. Identity rather than a driven give-up, for the
     * reason its sibling in {@code DefaultPublisherFactoryTest} records.
     */
    @Test
    void theTeardownIsHandedTheDeadLetterResidueCounter() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .build();
        queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            assertThat(((BoundedShutdown) queue.publisherShutdown).abandonedCounter())
                    .isSameAs(PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED)
                    .isNotSameAs(PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED);
        } finally {
            queue.close();
        }
    }

    // ---------------------------------------------------------------- the flush budget

    /**
     * The property #321 exists for: a wait that outlives its budget fails instead of blocking the
     * checkpoint for however long the SDK keeps retrying (600 s by default, which is also Flink's
     * default {@code execution.checkpointing.timeout}).
     */
    @Test
    void anExpiredFlushBudgetFailsRatherThanWaitingForTheSdk() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(SettableApiFuture.create());
        long startedAt = System.nanoTime();

        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofMillis(200),
                                        metrics))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(TOPIC.toString())
                .hasMessageContaining("flushTimeout")
                .hasMessageContaining("1 of 1 publishes unresolved")
                .hasCauseInstanceOf(TimeoutException.class);

        // Generous on both sides: the point is that it returned at all rather than waiting out an
        // SDK retry budget, and the exact wait is the scheduler's business.
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .isGreaterThanOrEqualTo(Duration.ofMillis(150))
                .isLessThan(Duration.ofSeconds(10));
    }

    /**
     * One deadline covers the whole list. A budget spent per future would be a thousandfold
     * multiple of the number it claims to be at the default {@code maxOutstandingMessages} — and
     * under that mutant all three of these fit their grant and nothing is thrown at all.
     */
    @Test
    void theBudgetCoversTheWholeListRatherThanEachPublish() {
        List<Duration> grants = new ArrayList<>();
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            outstanding.add(new RecordingFuture(Duration.ofMillis(400), grants));
        }

        // 400 ms of work each against a 1000 ms budget: two fit and the third does not, and the
        // first two fit by 600 ms and 200 ms of slack, so a stalled runner fails this test only
        // for the reason it is about.
        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofSeconds(1),
                                        metrics))
                .isInstanceOf(IOException.class)
                // At least one of the three fitted, so the count is what is left rather than the
                // whole list — how many fitted depends on the machine, that some did does not.
                .hasMessageContaining(" of 3 publishes unresolved")
                .hasMessageNotContaining("3 of 3");

        // The throw is what discriminates: a per-future budget grants every one of them the whole
        // second, all three fit, and nothing is thrown at all. The grants are the legibility half —
        // each future gets what the one budget had left, so they shrink.
        assertThat(grants).hasSize(3);
        for (int i = 1; i < grants.size(); i++) {
            assertThat(grants.get(i)).isLessThan(grants.get(i - 1));
        }
        // The last one, not the first: the first is a whole second under both the correct code and
        // the mutant, so asserting on it could not fail.
        assertThat(grants.get(2)).isLessThan(Duration.ofMillis(500));
    }

    @Test
    void theOutstandingListIsEmptiedEvenWhenTheBudgetExpired() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(SettableApiFuture.create());

        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofMillis(50),
                                        metrics))
                .isInstanceOf(IOException.class);

        // Re-awaiting a future that outlived one budget would only report the same thing again.
        assertThat(outstanding).isEmpty();
    }

    /**
     * A publish that is already done is never failed by the clock: the budget bounds the wait, not
     * the call. That rests on {@code Future.get(0, …)} returning a completed value rather than
     * throwing, which is measured here rather than argued.
     */
    @Test
    void resolvedPublishesReturnEvenWhenTheBudgetIsAlreadySpent() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            SettableApiFuture<String> future = SettableApiFuture.create();
            future.set("message-" + i);
            outstanding.add(future);
        }

        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {}, outstanding, TOPIC, Duration.ofNanos(1), metrics))
                .doesNotThrowAnyException();
        assertThat(outstanding).isEmpty();
    }

    /**
     * A spent budget is handed over as zero, never as the negative remainder it literally is:
     * {@code Future.get(long, TimeUnit)} does not define a negative timeout, and the futures here
     * come from an SDK whose implementation this connector does not choose.
     */
    @Test
    void theWaitIsNeverHandedANegativeBudget() {
        List<Duration> grants = new ArrayList<>();
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(new RecordingFuture(Duration.ZERO, grants));

        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {}, outstanding, TOPIC, Duration.ofNanos(1), metrics))
                .doesNotThrowAnyException();

        assertThat(grants).hasSize(1);
        assertThat(grants.get(0)).isGreaterThanOrEqualTo(Duration.ZERO);
    }

    /**
     * The largest budget the setter accepts really is a budget, which is worth pinning because the
     * arithmetic looks broken there and is not: the deadline stamp overflows and the subtraction
     * that reads it wraps a second time, the two cancelling to the true remainder. What this guards
     * is a later {@code Math.addExact} or clamp on that stamp, which would turn {@code
     * flushTimeout}'s documented way of saying "effectively unbounded" into a failed flush or the
     * setting that waited least (#334; ADR-0068).
     */
    @Test
    void theLargestExpressibleBudgetIsNotSpentTheInstantTheFlushStarts() {
        List<Duration> grants = new ArrayList<>();
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(new RecordingFuture(Duration.ZERO, grants));

        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofNanos(Long.MAX_VALUE),
                                        metrics))
                .doesNotThrowAnyException();

        assertThat(grants).hasSize(1);
        assertThat(grants.get(0)).isGreaterThan(Duration.ofDays(365L * 100));
    }

    // ---------------------------------------------------------------- the metrics

    /**
     * The counter names confirmations, not hand-offs: a wait that resolves two of three publishes
     * and then fails must report two. Counting at the offer instead — the shape the issue proposed
     * — would report three here, and would also duplicate {@code numRecordsSendErrors}, which every
     * sink increments on this same group immediately before calling its failure handler.
     */
    @Test
    void onlyConfirmedPublishesAreCounted() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            SettableApiFuture<String> resolved = SettableApiFuture.create();
            resolved.set("message-" + i);
            outstanding.add(resolved);
        }
        SettableApiFuture<String> failed = SettableApiFuture.create();
        failed.setException(new IllegalStateException("the service rejected it"));
        outstanding.add(failed);

        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofSeconds(30),
                                        metrics))
                .isInstanceOf(IOException.class);

        assertThat(metricGroup.counterValue("deadLettersPublished")).isEqualTo(2);
    }

    /**
     * The duration is recorded on the failing path too, which is the path worth having it on: the
     * wait that spent the whole budget is the one an operator sizing {@code flushTimeout} wants,
     * and recording only on success would skip exactly it. (That value is rarely scraped, since the
     * expiry fails the job — the reason it is a gauge to read <em>before</em> the failure, as a
     * series.)
     */
    @Test
    void theFlushDurationIsRecordedWhetherTheWaitResolvedOrExpired() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(SettableApiFuture.create());

        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofMillis(200),
                                        metrics))
                .isInstanceOf(IOException.class);

        // At least the budget, since the wait ran it out; the upper bound is loose because the
        // exact overshoot is the scheduler's business.
        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis"))
                .isGreaterThanOrEqualTo(150L)
                .isLessThan(10_000L);
    }

    /** And the successful path leaves the gauge holding that wait rather than the previous one. */
    @Test
    void aResolvedFlushReplacesTheDurationTheLastOneLeft() {
        List<ApiFuture<String>> slow = new ArrayList<>();
        slow.add(new RecordingFuture(Duration.ofMillis(300), new ArrayList<>()));
        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {}, slow, TOPIC, Duration.ofSeconds(30), metrics))
                .doesNotThrowAnyException();
        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis"))
                .isGreaterThanOrEqualTo(250L);

        SettableApiFuture<String> immediate = SettableApiFuture.create();
        immediate.set("message");
        List<ApiFuture<String>> quick = new ArrayList<>();
        quick.add(immediate);
        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {}, quick, TOPIC, Duration.ofSeconds(30), metrics))
                .doesNotThrowAnyException();

        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis")).isLessThan(250L);
        assertThat(metricGroup.counterValue("deadLettersPublished")).isEqualTo(2);
    }

    /**
     * The spike the last-wait gauge cannot keep (#405). Waits happen as often as the queue drains —
     * once per element under {@code WRITE_THROUGH} — so a slow one is overwritten long before a
     * reporter reads it, and only the maximum survives to say the budget was nearly spent.
     */
    @Test
    void theLongestWaitSurvivesTheFastWaitsThatFollowIt() {
        List<ApiFuture<String>> slow = new ArrayList<>();
        slow.add(new RecordingFuture(Duration.ofMillis(300), new ArrayList<>()));
        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {}, slow, TOPIC, Duration.ofSeconds(30), metrics))
                .doesNotThrowAnyException();
        long spike = metricGroup.<Long>gaugeValue("longestDeadLetterFlushMillis");
        assertThat(spike).isGreaterThanOrEqualTo(250L);

        // Several fast waits, as a steadily dead-lettering job makes between two scrapes.
        for (int i = 0; i < 3; i++) {
            SettableApiFuture<String> immediate = SettableApiFuture.create();
            immediate.set("message-" + i);
            List<ApiFuture<String>> quick = new ArrayList<>();
            quick.add(immediate);
            assertThatCode(
                            () ->
                                    PubSubDeadLetterQueue.flushOutstanding(
                                            () -> {},
                                            quick,
                                            TOPIC,
                                            Duration.ofSeconds(30),
                                            metrics))
                    .doesNotThrowAnyException();
        }

        // The last-wait gauge has forgotten the spike, which is the whole point of the second one.
        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis")).isLessThan(250L);
        assertThat(metricGroup.<Long>gaugeValue("longestDeadLetterFlushMillis")).isEqualTo(spike);
    }

    /** It is a maximum, not a last-write-wins under another name. */
    @Test
    void theLongestWaitRisesOnlyWhenAWaitBeatsIt() {
        metrics.flushCompleted(40L);
        assertThat(metricGroup.<Long>gaugeValue("longestDeadLetterFlushMillis")).isEqualTo(40L);

        metrics.flushCompleted(900L);
        assertThat(metricGroup.<Long>gaugeValue("longestDeadLetterFlushMillis")).isEqualTo(900L);

        metrics.flushCompleted(41L);
        assertThat(metricGroup.<Long>gaugeValue("longestDeadLetterFlushMillis")).isEqualTo(900L);
        // ... while the last-wait gauge does follow it down.
        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis")).isEqualTo(41L);
    }

    /** Both duration gauges start at zero, so a job that never dead-lettered reports no wait. */
    @Test
    void bothDurationGaugesStartAtZero() {
        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis")).isZero();
        assertThat(metricGroup.<Long>gaugeValue("longestDeadLetterFlushMillis")).isZero();
    }

    /**
     * A flush with nothing buffered is not a wait, and must not overwrite the last real one. {@code
     * flush()} runs at every checkpoint barrier, so on a job that dead-letters occasionally almost
     * every call is empty — recording those would erase the slow wait an operator is being told to
     * watch, within one checkpoint interval of it happening.
     */
    @Test
    void anEmptyFlushDoesNotEraseTheDurationOfTheLastRealOne() {
        List<ApiFuture<String>> slow = new ArrayList<>();
        slow.add(new RecordingFuture(Duration.ofMillis(300), new ArrayList<>()));
        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {}, slow, TOPIC, Duration.ofSeconds(30), metrics))
                .doesNotThrowAnyException();
        long afterRealWait = metricGroup.<Long>gaugeValue("deadLetterFlushMillis");
        assertThat(afterRealWait).isGreaterThanOrEqualTo(250L);

        assertThatCode(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        new ArrayList<>(),
                                        TOPIC,
                                        Duration.ofSeconds(30),
                                        metrics))
                .doesNotThrowAnyException();

        assertThat(metricGroup.<Long>gaugeValue("deadLetterFlushMillis")).isEqualTo(afterRealWait);
    }

    /**
     * The gauge reads the queue's live buffer rather than a number snapshotted when it opened, and
     * keeps answering after {@code close()} has discarded that buffer — a gauge is polled by the
     * reporter thread, which does not stop when the task's writer does.
     */
    @Test
    void theOutstandingGaugeTracksTheBufferAndSurvivesTheClose() throws Exception {
        StubWriterInitContext context = new StubWriterInitContext(0);
        TestSinkWriterMetricGroup group = context.getSinkWriterMetricGroup();
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        // The offer below leaves a publish the unreachable endpoint can never
                        // resolve, so the close waits out this budget: at the 30 s default this
                        // one test would cost thirty seconds of every build.
                        .shutdownTimeout(Duration.ofSeconds(2))
                        .build();
        queue.open(DefaultFailureHandlerContext.of(context));
        try {
            assertThat(group.<Integer>gaugeValue("outstandingDeadLetters")).isZero();
            assertThat(group.counterValue("deadLettersPublished")).isZero();

            queue.offer(new StubElement(ByteString.copyFromUtf8("row bytes"), "Rejected."));

            // Handed to the client library and unresolved — the endpoint is unreachable on
            // purpose, so nothing can confirm it and the confirmation counter must stay at zero.
            assertThat(group.<Integer>gaugeValue("outstandingDeadLetters")).isEqualTo(1);
            assertThat(group.counterValue("deadLettersPublished")).isZero();
        } finally {
            queue.close();
        }

        assertThat(group.<Integer>gaugeValue("outstandingDeadLetters")).isZero();
    }

    /**
     * The residue counter is registered under the dead-letter name and reads the dead-letter adder
     * — the discriminating half being that the sink publishers' adder does not move it, which is
     * what makes the two series answer "which publisher is stalling" rather than one number twice.
     */
    @Test
    void theResidueCounterReadsTheDeadLetterAdderAlone() throws Exception {
        PubSubShutdownResidue.resetForTests();
        StubWriterInitContext context = new StubWriterInitContext(0);
        TestSinkWriterMetricGroup group = context.getSinkWriterMetricGroup();
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .build();
        queue.open(DefaultFailureHandlerContext.of(context));
        try {
            assertThat(group.counterValue("deadLetterPublisherShutdownsAbandoned")).isZero();

            PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED.increment();
            assertThat(group.counterValue("deadLetterPublisherShutdownsAbandoned")).isZero();

            PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.increment();
            assertThat(group.counterValue("deadLetterPublisherShutdownsAbandoned")).isEqualTo(1);

            // The sink's own name is not registered by the queue: a host that is not a Pub/Sub
            // sink must not appear to report the sink publishers' residue.
            assertThat(group.hasMetric("publisherShutdownsAbandoned")).isFalse();
        } finally {
            queue.close();
            PubSubShutdownResidue.resetForTests();
        }
    }

    @Test
    void aFailedPublishStillReportsItsOwnCause() {
        SettableApiFuture<String> failed = SettableApiFuture.create();
        failed.setException(new IllegalStateException("the service rejected it"));
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(failed);

        // A real publish failure must not be reclassified as a budget expiry, which is what the
        // added catch could have swallowed.
        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {},
                                        outstanding,
                                        TOPIC,
                                        Duration.ofSeconds(30),
                                        metrics))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Publishing a dead letter")
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("the service rejected it");
    }

    @Test
    void anInterruptedAwaitLeavesTheFlagSetForTheRestOfTheTeardown() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(SettableApiFuture.create());
        Thread.currentThread().interrupt();
        try {
            // A short budget so a broken interrupt path fails here rather than parking this fork.
            assertThatThrownBy(
                            () ->
                                    PubSubDeadLetterQueue.flushOutstanding(
                                            () -> {},
                                            outstanding,
                                            TOPIC,
                                            Duration.ofSeconds(2),
                                            metrics))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while publishing");
            // Future.get clears the flag; without the restore the rest of the teardown would stop
            // honouring the cancellation.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Whatever happened, this fork is reused by other classes.
            Thread.interrupted();
        }
    }

    /**
     * The publisher hand-off is inside the guard too. {@code offer} already wrapped {@code
     * publisher.publish(...)}, while the drain two lines later and the one in {@code flush()} were
     * outside everything, so an unchecked SDK failure reached Flink raw with no topic named.
     */
    @Test
    void aPublishAllFailureNamesTheTopicAndEmptiesTheBuffer() {
        List<ApiFuture<String>> outstanding = new ArrayList<>();
        outstanding.add(SettableApiFuture.create());

        assertThatThrownBy(
                        () ->
                                PubSubDeadLetterQueue.flushOutstanding(
                                        () -> {
                                            throw new IllegalStateException("publisher blew up");
                                        },
                                        outstanding,
                                        TOPIC,
                                        Duration.ofSeconds(30),
                                        metrics))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(TOPIC.toString())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(outstanding).isEmpty();
    }

    /**
     * Needs no {@code open()}, unlike {@code theShutdownTimeoutSurvivesJobGraphSerialization}: this
     * budget is readable off the instance, while the shutdown one exists only inside the teardown
     * {@code open()} builds.
     */
    @Test
    void theFlushBudgetSurvivesJobGraphSerialization() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .flushTimeout(Duration.ofSeconds(12))
                        .build();

        PubSubDeadLetterQueue restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(queue), getClass().getClassLoader());

        assertThat(restored.flushTimeout()).isEqualTo(Duration.ofSeconds(12));
    }

    /**
     * The configured budget reaches the wait itself, which {@code
     * theFlushBudgetSurvivesJobGraphSerialization} cannot say: it reads the field. A call site
     * handing the wait the wrong budget — the shutdown one, or the default — is otherwise
     * invisible, since every other test here drives the static directly and the emulator ITs
     * configure none.
     *
     * <p>Opened against a lazily-connecting channel with nothing listening, so this needs no
     * server: a publish only enqueues, and the RPC then retries against a refused connection for
     * the SDK's whole 600 s budget, which is exactly the pending future the wait has to give up on.
     * The shutdown budget is pinned low because those publishes deliberately outlive the flush,
     * leaving the SDK's own shutdown an undrained waiter to sit on.
     */
    @Test
    void theConfiguredBudgetReachesTheFlushWait() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .flushTimeout(Duration.ofMillis(300))
                        .shutdownTimeout(Duration.ofSeconds(2))
                        .build();
        queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            queue.offer(new StubElement(ByteString.copyFromUtf8("row bytes"), "Rejected."));

            assertThatThrownBy(queue::flush)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("flushTimeout budget of PT0.3S")
                    .hasCauseInstanceOf(TimeoutException.class);
        } finally {
            queue.close();
        }
    }

    /** The same for the other call site, which {@code maxOutstandingMessages} drives. */
    @Test
    void theConfiguredBudgetReachesTheOutstandingDrain() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .maxOutstandingMessages(1)
                        .flushTimeout(Duration.ofMillis(300))
                        .shutdownTimeout(Duration.ofSeconds(2))
                        .build();
        queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            assertThatThrownBy(
                            () ->
                                    queue.offer(
                                            new StubElement(
                                                    ByteString.copyFromUtf8("row bytes"),
                                                    "Rejected.")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("flushTimeout budget of PT0.3S")
                    .hasCauseInstanceOf(TimeoutException.class);
        } finally {
            queue.close();
        }
    }

    @Test
    void theFlushBudgetDefaultsToSixtySeconds() {
        assertThat(PubSubDeadLetterQueue.builder().topic(TOPIC).build().flushTimeout())
                .isEqualTo(PubSubDeadLetterQueue.DEFAULT_FLUSH_TIMEOUT)
                .isEqualTo(Duration.ofSeconds(60));
    }

    private static String serviceAccountKeyJson() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        String encoded =
                Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(keyPair.getPrivate().getEncoded());
        String privateKey =
                "-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n";
        return "{"
                + "\"type\":\"service_account\","
                + "\"project_id\":\"test-project\","
                + "\"private_key_id\":\"credential-private-key-id-must-not-be-serialized\","
                + "\"private_key\":\""
                + privateKey.replace("\n", "\\n")
                + "\","
                + "\"client_email\":\"service-account@example.invalid\","
                + "\"client_id\":\"1234567890\","
                + "\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\","
                + "\"token_uri\":\"https://oauth2.googleapis.com/token\""
                + "}";
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    /**
     * An {@link ApiFuture} that records the budget it was granted and needs a scripted slice of it
     * to resolve. Recording the grant is what tells a whole-call deadline from a per-future one:
     * the first shrinks with every future awaited, the second does not.
     */
    private static final class RecordingFuture implements ApiFuture<String> {

        private final Duration work;
        private final List<Duration> grants;

        private RecordingFuture(Duration work, List<Duration> grants) {
            this.work = work;
            this.grants = grants;
        }

        @Override
        public String get(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException {
            Duration granted = Duration.ofNanos(unit.toNanos(timeout));
            grants.add(granted);
            if (granted.isNegative()) {
                // Rejected rather than absorbed, so the assertion below is the discriminating one
                // rather than a restatement of what this fake decided to tolerate.
                throw new IllegalArgumentException("negative grant: " + granted);
            }
            if (work.compareTo(granted) > 0) {
                Thread.sleep(granted.toMillis());
                throw new TimeoutException("the publish outlived its grant of " + granted);
            }
            Thread.sleep(work.toMillis());
            return "message-id";
        }

        @Override
        public String get() {
            throw new UnsupportedOperationException("the queue always waits with a budget");
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            throw new UnsupportedOperationException("not used by the await");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }
    }

    private static String repeat(char c, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(c);
        }
        return builder.toString();
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
