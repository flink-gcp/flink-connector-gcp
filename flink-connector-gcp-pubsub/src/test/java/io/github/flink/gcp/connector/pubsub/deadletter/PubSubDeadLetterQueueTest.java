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

package io.github.flink.gcp.connector.pubsub.deadletter;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
        // throwing an ArithmeticException out of the first flush on a TaskManager.
        assertThatThrownBy(() -> builder.flushTimeout(Duration.ofDays(400_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most");
        assertThatCode(() -> builder.flushTimeout(Duration.ofDays(1000)))
                .doesNotThrowAnyException();
        // The queue's other budget reaches BoundedShutdown.start(), which converts it the same way.
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofDays(400_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most");
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
     * The queue's teardown is handed the same counter the sink's publishers feed, so its
     * abandonments reach {@code publisherShutdownsAbandoned} too. Identity rather than a driven
     * give-up, for the reason its sibling in {@code DefaultPublisherFactoryTest} records.
     */
    @Test
    void theTeardownIsHandedTheConnectorsResidueCounter() throws Exception {
        PubSubDeadLetterQueue queue =
                PubSubDeadLetterQueue.builder()
                        .topic(TOPIC)
                        .emulatorEndpoint("localhost:1")
                        .build();
        queue.open(DefaultFailureHandlerContext.of(new StubWriterInitContext(0)));
        try {
            assertThat(((BoundedShutdown) queue.publisherShutdown).abandonedCounter())
                    .isSameAs(PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED);
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
                                        () -> {}, outstanding, TOPIC, Duration.ofMillis(200)))
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
                                        () -> {}, outstanding, TOPIC, Duration.ofSeconds(1)))
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
                                        () -> {}, outstanding, TOPIC, Duration.ofMillis(50)))
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
                                        () -> {}, outstanding, TOPIC, Duration.ofNanos(1)))
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
                                        () -> {}, outstanding, TOPIC, Duration.ofNanos(1)))
                .doesNotThrowAnyException();

        assertThat(grants).hasSize(1);
        assertThat(grants.get(0)).isGreaterThanOrEqualTo(Duration.ZERO);
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
                                        () -> {}, outstanding, TOPIC, Duration.ofSeconds(30)))
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
                                            () -> {}, outstanding, TOPIC, Duration.ofSeconds(2)))
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
                                        Duration.ofSeconds(30)))
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
