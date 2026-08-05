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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.FailedMessage;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubRealGcpITCase;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins, against real Cloud Pub/Sub, the end-to-end outcome the #264 isolation republish exists for:
 * a valid message co-batched with an invalid one is published, not dropped, and exactly the invalid
 * one reaches a dropping {@code failedMessageHandler} — with the flush completing, which is the
 * #269 half of the claim.
 *
 * <p>The unit suite encodes the measured vendor behaviour (#264's record: a mixed {@code Publish}
 * is rejected all-or-nothing with one request-level {@code INVALID_ARGUMENT}, and a solo republish
 * yields a true per-message verdict) by convention in {@code FakePublisherFactory} scripting; an
 * SDK or service change could invalidate that silently, and this class is what would catch it
 * (#303). Deliberately pinned at the outcome, not the wire: the outcome is identical whether the
 * service rejects all-or-nothing (measured) or were ever to accept the valid entries of a mixed
 * request — the fix is robust to both — so wire-level details like the shared {@code Throwable}
 * instance stay unasserted.
 *
 * <p>The sink is driven through the public builder and the production {@code createWriter} — with
 * no emulator endpoint that is the production path: application-default credentials, real service.
 * Batching thresholds are set so high that only {@code flush} sends, so the three messages travel
 * as one {@code Publish} request, and the invalid one carries a 1025-byte attribute value — the
 * measured deterministic {@code INVALID_ARGUMENT}, one byte over the documented 1024-byte limit.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubSinkRejectionRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /** Payloads as data; the {@code bad} element also gets the over-limit attribute value. */
    private static PubSubSerializationSchema<String> serializer() {
        return element -> {
            PubsubMessage.Builder message =
                    PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(element));
            if (element.equals("bad")) {
                message.putAttributes("k", "v".repeat(1025));
            }
            return message.build();
        };
    }

    /** Records what the sink hands it; dropping, because it returns. */
    private static final class RecordingHandler implements FailureHandler<FailedMessage> {

        private static final long serialVersionUID = 1L;

        private final transient List<FailedMessage> handled = new ArrayList<>();

        @Override
        public void handle(FailedMessage message) {
            handled.add(message);
        }
    }

    @Test
    void aValidMessageCoBatchedWithAnInvalidOneIsPublishedNotDropped() throws Exception {
        TopicDestination topic = createTopic("sink-rejection");
        SubscriptionDestination subscription = createSubscription(topic, "sink-rejection");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                PubSubSink.<String>builder()
                        .topic(topic)
                        .serializer(serializer())
                        // The topic is pre-created and registered for cleanup above; the sink must
                        // not create one the harness would not delete.
                        .createDisposition(CreateDisposition.CREATE_NEVER)
                        .publisherOptions(
                                PubSubPublisherOptions.builder()
                                        .batchElementCountThreshold(100)
                                        .batchRequestByteThreshold(10_000_000)
                                        .batchDelayThreshold(Duration.ofMinutes(10))
                                        .build())
                        .failedMessageHandler(handler)
                        .build()
                        .createWriter(new StubWriterInitContext(0));
        try {
            writer.write("m0", CONTEXT);
            writer.write("bad", CONTEXT);
            writer.write("m2", CONTEXT);
            // Completing at all is the #269 half: the drop must not fail the job.
            writer.flush(false);
        } finally {
            writer.close();
        }

        assertThat(pullAndAckUntil(subscription, 2, COLLECT_TIMEOUT))
                .containsExactlyInAnyOrder("m0", "m2");
        // One more pull, tolerating redelivery of the acknowledged survivors: whatever else the
        // service holds, the rejected message must not be among it.
        assertThat(pullMessagesAndAck(subscription, 10))
                .extracting(message -> message.getData().toStringUtf8())
                .doesNotContain("bad");
        assertThat(handler.handled).hasSize(1);
        FailedMessage failed = handler.handled.get(0);
        assertThat(failed.getPubsubMessage().getData().toStringUtf8()).isEqualTo("bad");
        assertThat(failed.describeDestination()).isEqualTo(topic.toTopicPath());
        assertThat(failed.getErrorMessage()).contains("INVALID_ARGUMENT");
        // The cause is the service's own verdict on the solo republish, not the batch report.
        assertThat(String.valueOf(failed.getCause())).contains("INVALID_ARGUMENT");
    }
}
