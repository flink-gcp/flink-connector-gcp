/*
 * Copyright 2023 Google LLC
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
import org.apache.flink.util.Collector;

import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

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
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which emits exactly one possibly-null record per message and has no failure policy.
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

    /** Reused across records; the emitter is confined to the task thread. */
    private final SourceOutputCollector<T> collector = new SourceOutputCollector<>();

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
        collector.bind(sourceOutput, message);
        try {
            deserializationSchema.deserialize(message, split.getSubscription(), collector);
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
        } finally {
            collector.unbind();
        }
        ackTracker.stagePendingAck(split.splitId(), message.getMessageId());
    }

    /**
     * Finds the output failure inside a thrown exception, or returns {@code null} when the schema
     * itself failed.
     *
     * <p>The chain is walked rather than the top-level type checked: a schema that follows the
     * common {@code ignore-parse-errors} shape catches everything it throws and re-wraps it, which
     * would otherwise make a downstream failure look like a bad message — and under {@code DROP}
     * that would acknowledge a perfectly good message and swallow the downstream exception.
     */
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

    /**
     * Adapts a {@link SourceOutput} to the {@link Collector} the deserialization schema writes to,
     * marking failures that came from the output rather than from the schema.
     */
    private static final class SourceOutputCollector<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;
        private boolean hasTimestamp;
        private long timestamp;

        private void bind(SourceOutput<T> sourceOutput, PubsubMessage message) {
            this.sourceOutput = sourceOutput;
            this.hasTimestamp = message.hasPublishTime();
            this.timestamp = hasTimestamp ? toEpochMillis(message.getPublishTime()) : 0L;
        }

        private void unbind() {
            this.sourceOutput = null;
        }

        @Override
        public void collect(T record) {
            try {
                if (hasTimestamp) {
                    sourceOutput.collect(record, timestamp);
                } else {
                    // Pub/Sub always stamps delivered messages, so this only happens for synthetic
                    // messages; emitting without a timestamp beats emitting the epoch.
                    sourceOutput.collect(record);
                }
            } catch (Exception e) {
                // Wrapped so the emitter can tell an output failure from a schema failure. A schema
                // that catches broadly would swallow it, but such a schema already swallows the
                // downstream failure itself — the wrapper does not make that worse.
                throw new CollectFailure(e);
            }
        }

        @Override
        public void close() {}

        private static long toEpochMillis(Timestamp publishTime) {
            return publishTime.getSeconds() * 1_000L + publishTime.getNanos() / 1_000_000L;
        }
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
