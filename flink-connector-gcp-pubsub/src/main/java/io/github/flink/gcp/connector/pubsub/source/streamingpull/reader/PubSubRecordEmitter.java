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
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

/**
 * Deserializes received messages and emits them, using the Pub/Sub publish time as the event
 * timestamp.
 *
 * <p>Runs on the task thread. A message is staged for acknowledgement only <em>after</em> its
 * records have reached the output, so a message that fails on the way out stays pending and is
 * nacked when the reader closes rather than being acknowledged by the next checkpoint.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which emits exactly one possibly-null record per message.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class PubSubRecordEmitter<T> implements RecordEmitter<PubsubMessage, T, SubscriptionSplit> {

    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final AckTracker ackTracker;

    /** Reused across records; the emitter is confined to the task thread. */
    private final SourceOutputCollector<T> collector = new SourceOutputCollector<>();

    /**
     * Creates the emitter.
     *
     * @param deserializationSchema converts messages into records
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     */
    public PubSubRecordEmitter(
            PubSubDeserializationSchema<T> deserializationSchema, AckTracker ackTracker) {
        this.deserializationSchema = deserializationSchema;
        this.ackTracker = ackTracker;
    }

    @Override
    public void emitRecord(
            PubsubMessage message, SourceOutput<T> sourceOutput, SubscriptionSplit split)
            throws Exception {
        collector.bind(sourceOutput, message);
        try {
            deserializationSchema.deserialize(message, collector);
        } finally {
            collector.unbind();
        }
        ackTracker.stagePendingAck(split.splitId(), message.getMessageId());
    }

    /**
     * Adapts a {@link SourceOutput} to the {@link Collector} the deserialization schema writes to.
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
            if (hasTimestamp) {
                sourceOutput.collect(record, timestamp);
            } else {
                // Pub/Sub always stamps delivered messages, so this only happens for synthetic
                // messages; emitting without a timestamp beats emitting the epoch.
                sourceOutput.collect(record);
            }
        }

        @Override
        public void close() {}

        private static long toEpochMillis(Timestamp publishTime) {
            return publishTime.getSeconds() * 1_000L + publishTime.getNanos() / 1_000_000L;
        }
    }
}
