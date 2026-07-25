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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Reader subtask consuming its assigned subscription splits.
 *
 * <p>The split itself is the split state: splits carry no progress, so there is nothing to mutate
 * while reading and nothing to convert when snapshotting.
 *
 * <p>The reader owns the two ends of the acknowledgement lifecycle that only it can see — binding
 * staged messages to the checkpoint being taken, and acknowledging them once that checkpoint
 * completes.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which wraps the split in a separate state class and passes a fresh {@code Configuration} instead
 * of the job's.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                PubsubMessage, T, SubscriptionSplit, SubscriptionSplit> {

    private final AckTracker ackTracker;

    /**
     * Creates the reader.
     *
     * @param splitReaderSupplier supplies the split reader multiplexing the subscribers
     * @param recordEmitter deserializes and emits received messages
     * @param config the job configuration, which carries the source reader options
     * @param context the reader context
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     */
    public PubSubSourceReader(
            Supplier<SplitReader<PubsubMessage, SubscriptionSplit>> splitReaderSupplier,
            RecordEmitter<PubsubMessage, T, SubscriptionSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            AckTracker ackTracker) {
        super(splitReaderSupplier, recordEmitter, config, context);
        this.ackTracker = ackTracker;
    }

    @Override
    public List<SubscriptionSplit> snapshotState(long checkpointId) {
        // Everything emitted before the barrier belongs to this checkpoint; anything emitted after
        // it belongs to the next one.
        ackTracker.addCheckpoint(checkpointId);
        return super.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        ackTracker.notifyCheckpointComplete(checkpointId);
    }

    @Override
    protected SubscriptionSplit initializedState(SubscriptionSplit split) {
        return split;
    }

    @Override
    protected SubscriptionSplit toSplitType(String splitId, SubscriptionSplit splitState) {
        return splitState;
    }

    @Override
    protected void onSplitFinished(Map<String, SubscriptionSplit> finishedSplits) {
        throw new IllegalStateException(
                "A Pub/Sub subscription split never finishes, but the reader was told that "
                        + finishedSplits.keySet()
                        + " did.");
    }
}
