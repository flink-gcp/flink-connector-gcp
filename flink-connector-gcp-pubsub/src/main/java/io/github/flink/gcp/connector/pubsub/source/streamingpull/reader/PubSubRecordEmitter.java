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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Deserializes received messages and emits them, using the Pub/Sub publish time as the event
 * timestamp.
 *
 * <p>Runs on the task thread. A message is staged for acknowledgement only <em>after</em> its
 * records have reached the output, so a message that fails on the way out is never acknowledged by
 * a later checkpoint.
 *
 * <p>Two failures are distinguished, because they call for opposite handling:
 *
 * <ul>
 *   <li><b>The schema failed.</b> The message is bad, so {@link DeserializationFailurePolicy}
 *       decides between failing the job, dropping the message, and returning it to Pub/Sub for its
 *       dead-letter policy to deal with.
 *   <li><b>The output failed.</b> The message is fine and the job is about to fail anyway, so it is
 *       nacked for immediate redelivery rather than left to its acknowledgement deadline.
 * </ul>
 *
 * <p><b>Only inline downstream failures are visible here.</b> {@code SourceOutput.collect} runs the
 * chained operators synchronously, so an exception from one of them propagates back into this
 * method — but a failure past a shuffle boundary happens on another task entirely and cannot be
 * observed. Those messages are covered by the nack the reader performs when it closes.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class PubSubRecordEmitter<T> implements RecordEmitter<PubsubMessage, T, SubscriptionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubRecordEmitter.class);

    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final AckTracker ackTracker;
    private final DeserializationFailurePolicy failurePolicy;
    private final PubSubSourceReaderMetrics metrics;

    /**
     * Drives the decreasing log rate of messages the failure policy handled; task-thread confined.
     */
    private long handledFailureCount;

    /**
     * Creates the emitter.
     *
     * @param deserializationSchema converts messages into records
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     * @param failurePolicy what to do with a message the schema cannot convert
     * @param metrics counts drops and deserialization failures
     */
    public PubSubRecordEmitter(
            PubSubDeserializationSchema<T> deserializationSchema,
            AckTracker ackTracker,
            DeserializationFailurePolicy failurePolicy,
            PubSubSourceReaderMetrics metrics) {
        this.deserializationSchema = deserializationSchema;
        this.ackTracker = ackTracker;
        this.failurePolicy = failurePolicy;
        this.metrics = metrics;
    }

    @Override
    public void emitRecord(
            PubsubMessage message, SourceOutput<T> sourceOutput, SubscriptionSplit split)
            throws Exception {
        Consumer<T> timestampedOutput = timestampedOutput(sourceOutput, message);
        try {
            long emittedCount =
                    SynchronousDeserializationCollector.<T, Exception>deserialize(
                            record -> collectOrMarkFailure(timestampedOutput, record),
                            out ->
                                    deserializationSchema.deserialize(
                                            message, split.getSubscription(), out));
            if (emittedCount == 0) {
                metrics.recordSkipped();
            }
        } catch (Exception e) {
            CollectFailure collectFailure = findCollectFailure(e);
            if (collectFailure == null) {
                handleDeserializationFailure(message, split, e);
                return;
            }
            ackTracker.nackPendingImmediately(split.splitId(), message.getMessageId());
            // Unwrap only our own marker; a schema that caught and re-wrapped it added context
            // worth keeping.
            throw e == collectFailure ? collectFailure.getCause() : e;
        }
        ackTracker.stagePendingAck(split.splitId(), message.getMessageId());
    }

    /** Finds a downstream-output marker in the exception chain. */
    @Nullable
    private static CollectFailure findCollectFailure(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof CollectFailure) {
                return (CollectFailure) current;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return null;
    }

    private void handleDeserializationFailure(
            PubsubMessage message, SubscriptionSplit split, Exception failure) throws IOException {
        if (failurePolicy == DeserializationFailurePolicy.FAIL) {
            metrics.deserializationFailed();
            throw new IOException(
                    "Failed to deserialize Pub/Sub message "
                            + message.getMessageId()
                            + " from "
                            + split.getSubscription()
                            + ". Set"
                            + " PubSubSource.builder().deserializationFailurePolicy(DROP) to"
                            + " discard messages the schema cannot convert, or (NACK) to return"
                            + " them to Pub/Sub for dead-lettering, instead of failing the job.",
                    failure);
        }
        if (failurePolicy == DeserializationFailurePolicy.NACK) {
            // The tracker counts the nack itself; this records that a deserialization failure is
            // what caused it, which is what numRecordsInErrors reports.
            metrics.deserializationFailed();
            ackTracker.nackPendingImmediately(split.splitId(), message.getMessageId());
            logHandledFailure(message, split, failure, "Nacked");
            return;
        }
        metrics.messageDropped();
        ackTracker.ackPendingImmediately(split.splitId(), message.getMessageId());
        logHandledFailure(message, split, failure, "Dropped");
    }

    /**
     * Logs the first few handled failures, then progressively fewer, so a bad batch cannot flood
     * the log.
     */
    private void logHandledFailure(
            PubsubMessage message, SubscriptionSplit split, Exception failure, String action) {
        handledFailureCount++;
        if (handledFailureCount <= 10 || Long.bitCount(handledFailureCount) == 1) {
            LOG.warn(
                    "{} Pub/Sub message {} from {}: the deserialization schema could not convert it"
                            + " ({} handled so far on this reader).",
                    action,
                    message.getMessageId(),
                    split.getSubscription(),
                    handledFailureCount,
                    failure);
        }
    }

    private static <T> Consumer<T> timestampedOutput(
            SourceOutput<T> output, PubsubMessage message) {
        if (!message.hasPublishTime()) {
            // Pub/Sub always stamps delivered messages, so this only happens for synthetic
            // messages; emitting without a timestamp beats emitting the epoch.
            return output::collect;
        }
        long timestamp = toEpochMillis(message.getPublishTime());
        return record -> output.collect(record, timestamp);
    }

    private static <T> void collectOrMarkFailure(Consumer<T> output, T record) {
        try {
            output.accept(record);
        } catch (Exception failure) {
            throw new CollectFailure(failure);
        }
    }

    private static long toEpochMillis(Timestamp publishTime) {
        return publishTime.getSeconds() * 1_000L + publishTime.getNanos() / 1_000_000L;
    }

    /** Marks an exception thrown by the source output rather than by the schema. */
    private static final class CollectFailure extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private CollectFailure(Exception cause) {
            super(cause);
        }

        @Override
        public synchronized Exception getCause() {
            return (Exception) super.getCause();
        }
    }
}
