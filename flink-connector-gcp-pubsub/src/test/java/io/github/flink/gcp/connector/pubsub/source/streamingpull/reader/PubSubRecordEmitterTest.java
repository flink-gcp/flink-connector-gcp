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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubRecordEmitter}. */
class PubSubRecordEmitterTest {

    private static final SubscriptionSplit SPLIT =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub"), "0");

    private final PubSubAckTracker ackTracker = new PubSubAckTracker();
    private final CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

    @Test
    void emitsWithThePublishTimeAsEventTimeAndStagesTheAcknowledgement() throws Exception {
        RecordingAckReplyConsumer consumer = receive("m1");
        PubsubMessage message =
                message("m1", "payload").toBuilder()
                        .setPublishTime(
                                Timestamp.newBuilder()
                                        .setSeconds(1_700_000_000L)
                                        .setNanos(123_000_000))
                        .build();

        emitter(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .emitRecord(message, output, SPLIT);

        assertThat(output.records()).containsExactly("payload");
        assertThat(output.timestamps()).containsExactly(1_700_000_000_123L);

        // Staged, so the next checkpoint covers it.
        ackTracker.addCheckpoint(1L);
        ackTracker.notifyCheckpointComplete(1L);
        assertThat(consumer.isAcked()).isTrue();
    }

    @Test
    void emitsWithoutATimestampWhenTheMessageCarriesNoPublishTime() throws Exception {
        receive("m1");

        emitter(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .emitRecord(message("m1", "payload"), output, SPLIT);

        assertThat(output.records()).containsExactly("payload");
        assertThat(output.timestamps()).containsExactly(Long.MIN_VALUE);
    }

    @Test
    void aSchemaMayEmitSeveralRecordsForOneMessage() throws Exception {
        receive("m1");

        emitter(new SplittingSchema()).emitRecord(message("m1", "a,b,c"), output, SPLIT);

        assertThat(output.records()).containsExactly("a", "b", "c");
    }

    @Test
    void aSchemaMayEmitNothingWhichDropsTheMessageButStillAcknowledgesIt() throws Exception {
        RecordingAckReplyConsumer consumer = receive("m1");

        emitter(new DroppingSchema()).emitRecord(message("m1", "ignored"), output, SPLIT);

        assertThat(output.records()).isEmpty();
        ackTracker.addCheckpoint(1L);
        ackTracker.notifyCheckpointComplete(1L);
        assertThat(consumer.isAcked()).isTrue();
    }

    @Test
    void aMessageThatFailsOnTheWayOutIsNotStagedForAcknowledgement() {
        RecordingAckReplyConsumer consumer = receive("m1");
        output.failOnCollect(new IllegalStateException("downstream exploded"));

        assertThatThrownBy(
                        () ->
                                emitter(
                                                PubSubDeserializationSchema.dataOnly(
                                                        new SimpleStringSchema()))
                                        .emitRecord(message("m1", "payload"), output, SPLIT))
                .isInstanceOf(IllegalStateException.class);

        // Never staged, so a completing checkpoint must not acknowledge it; it stays pending and is
        // nacked when the reader closes.
        ackTracker.addCheckpoint(1L);
        ackTracker.notifyCheckpointComplete(1L);
        assertThat(consumer.isUnsettled()).isTrue();
    }

    private RecordingAckReplyConsumer receive(String messageId) {
        RecordingAckReplyConsumer consumer = new RecordingAckReplyConsumer(messageId);
        ackTracker.addPendingAck(SPLIT.splitId(), messageId, consumer);
        return consumer;
    }

    private PubSubRecordEmitter<String> emitter(PubSubDeserializationSchema<String> schema) {
        return new PubSubRecordEmitter<>(schema, ackTracker);
    }

    private static PubsubMessage message(String messageId, String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId(messageId)
                .setData(ByteString.copyFrom(payload, StandardCharsets.UTF_8))
                .build();
    }

    /** Splits a comma-separated payload into one record per element. */
    private static final class SplittingSchema implements PubSubDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(PubsubMessage message, Collector<String> out) {
            for (String part : message.getData().toString(StandardCharsets.UTF_8).split(",")) {
                out.collect(part);
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /** Emits nothing, which is how a schema drops a message. */
    private static final class DroppingSchema implements PubSubDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(PubsubMessage message, Collector<String> out) {}

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
