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
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

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
