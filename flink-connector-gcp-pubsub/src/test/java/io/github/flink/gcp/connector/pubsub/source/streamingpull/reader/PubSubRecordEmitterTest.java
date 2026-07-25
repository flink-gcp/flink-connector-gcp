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
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubRecordEmitter}. */
class PubSubRecordEmitterTest {

    private static final SubscriptionSplit SPLIT =
            new SubscriptionSplit(SubscriptionDestination.of("project", "sub"), "0");

    private final TestReaderMetrics testMetrics = new TestReaderMetrics();
    private final PubSubAckTracker ackTracker = new PubSubAckTracker(testMetrics.metrics(), null);
    private final CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

    @Test
    void emitsWithThePublishTimeAsEventTimeAndStagesTheAcknowledgement() throws Exception {
        RecordingAckHandle consumer = receive("m1");
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
        assertThat(output.timestamps()).containsExactly((Long) null);
    }

    @Test
    void aSchemaMayEmitSeveralRecordsForOneMessage() throws Exception {
        receive("m1");

        emitter(new SplittingSchema()).emitRecord(message("m1", "a,b,c"), output, SPLIT);

        assertThat(output.records()).containsExactly("a", "b", "c");
    }

    @Test
    void aSchemaMayEmitNothingWhichDropsTheMessageButStillAcknowledgesIt() throws Exception {
        RecordingAckHandle consumer = receive("m1");

        emitter(new DroppingSchema()).emitRecord(message("m1", "ignored"), output, SPLIT);

        assertThat(output.records()).isEmpty();
        ackTracker.addCheckpoint(1L);
        ackTracker.notifyCheckpointComplete(1L);
        assertThat(consumer.isAcked()).isTrue();
    }

    @Test
    void aMessageThatFailsOnTheWayOutIsNackedAndTheFailureRethrown() throws Exception {
        RecordingAckHandle handle = receive("m1");
        output.failOnCollect(new IllegalStateException("downstream exploded"));

        assertThatThrownBy(
                        () ->
                                emitter(
                                                PubSubDeserializationSchema.dataOnly(
                                                        new SimpleStringSchema()))
                                        .emitRecord(message("m1", "payload"), output, SPLIT))
                // The downstream failure itself, not the marker the collector wraps it in.
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("downstream exploded");

        // Nacked at once rather than left to its acknowledgement deadline: the job is failing, and
        // the message is fine.
        assertThat(handle.isNacked()).isTrue();
        assertThat(testMetrics.counter("messagesNacked")).isEqualTo(1);

        // And never staged, so a completing checkpoint cannot acknowledge it either.
        ackTracker.addCheckpoint(1L);
        ackTracker.notifyCheckpointComplete(1L);
        assertThat(handle.isAcked()).isFalse();
    }

    @Test
    void theFailPolicyFailsTheJobAndLeavesTheMessagePending() throws Exception {
        RecordingAckHandle handle = receive("m1");

        assertThatThrownBy(
                        () ->
                                emitter(new UndeserializableSchema())
                                        .emitRecord(message("m1", "payload"), output, SPLIT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to deserialize Pub/Sub message m1")
                .hasMessageContaining("deserializationFailurePolicy(DROP)");

        assertThat(handle.isUnsettled()).isTrue();
        assertThat(testMetrics.numRecordsInErrors()).isEqualTo(1);
    }

    @Test
    void theDropPolicyAcknowledgesTheMessageImmediatelyAndCountsIt() throws Exception {
        RecordingAckHandle handle = receive("m1");

        emitter(new UndeserializableSchema(), DeserializationFailurePolicy.DROP)
                .emitRecord(message("m1", "payload"), output, SPLIT);

        assertThat(output.records()).isEmpty();
        // Acknowledged without waiting for a checkpoint: nothing was emitted, so no checkpoint
        // covers it.
        assertThat(handle.isAcked()).isTrue();
        assertThat(testMetrics.counter("messagesDropped")).isEqualTo(1);
        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(1);
        assertThat(testMetrics.numRecordsInErrors()).isEqualTo(1);
    }

    @Test
    void theDropPolicyKeepsRecordsTheSchemaEmittedBeforeFailing() throws Exception {
        receive("m1");

        emitter(new PartiallyFailingSchema(), DeserializationFailurePolicy.DROP)
                .emitRecord(message("m1", "payload"), output, SPLIT);

        // The prefix already reached the output and cannot be recalled.
        assertThat(output.records()).containsExactly("first");
    }

    @Test
    void anOutputFailureTheSchemaRewrapsIsStillTreatedAsAnOutputFailure() throws Exception {
        // The ignore-parse-errors idiom catches everything it throws and re-wraps it. Classifying
        // by the top-level type would call that a bad message and, under DROP, acknowledge a
        // perfectly good one while swallowing the downstream exception.
        RecordingAckHandle handle = receive("m1");
        output.failOnCollect(new IllegalStateException("downstream exploded"));

        assertThatThrownBy(
                        () ->
                                emitter(new RewrappingSchema(), DeserializationFailurePolicy.DROP)
                                        .emitRecord(message("m1", "payload"), output, SPLIT))
                .isInstanceOf(IOException.class)
                .hasMessage("Corrupt record");

        assertThat(handle.isNacked()).isTrue();
        assertThat(handle.isAcked()).isFalse();
        assertThat(testMetrics.counter("messagesDropped")).isZero();
    }

    /** A schema that emits and then re-wraps whatever comes back, including an output failure. */
    private static final class RewrappingSchema implements PubSubDeserializationSchema<String> {

        @Override
        public void deserialize(PubsubMessage message, Collector<String> out) throws IOException {
            try {
                out.collect("record");
            } catch (Throwable t) {
                throw new IOException("Corrupt record", t);
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    /** A schema that never succeeds. */
    private static final class UndeserializableSchema
            implements PubSubDeserializationSchema<String> {

        @Override
        public void deserialize(PubsubMessage message, Collector<String> out) throws IOException {
            throw new IOException("not my format");
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    /** A schema that emits one record and then fails. */
    private static final class PartiallyFailingSchema
            implements PubSubDeserializationSchema<String> {

        @Override
        public void deserialize(PubsubMessage message, Collector<String> out) throws IOException {
            out.collect("first");
            throw new IOException("truncated");
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private RecordingAckHandle receive(String messageId) {
        RecordingAckHandle consumer = new RecordingAckHandle(messageId);
        ackTracker.addPendingAck(SPLIT.splitId(), messageId, consumer);
        return consumer;
    }

    private PubSubRecordEmitter<String> emitter(PubSubDeserializationSchema<String> schema) {
        return emitter(schema, DeserializationFailurePolicy.FAIL);
    }

    private PubSubRecordEmitter<String> emitter(
            PubSubDeserializationSchema<String> schema, DeserializationFailurePolicy policy) {
        return new PubSubRecordEmitter<>(schema, ackTracker, policy, testMetrics.metrics());
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
