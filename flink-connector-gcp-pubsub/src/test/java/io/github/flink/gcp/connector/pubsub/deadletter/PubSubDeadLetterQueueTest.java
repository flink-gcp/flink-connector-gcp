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

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue.MAX_ATTRIBUTE_VALUE_BYTES;
import static io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue.TRUNCATION_MARKER;
import static io.github.flink.gcp.connector.pubsub.deadletter.PubSubDeadLetterQueue.truncateToAttributeLimit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests for the parts of {@link PubSubDeadLetterQueue} that need no publisher: the envelope, the
 * attribute-value truncation and the builder. The round trip through a topic is {@link
 * PubSubDeadLetterQueueITCase}.
 */
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
