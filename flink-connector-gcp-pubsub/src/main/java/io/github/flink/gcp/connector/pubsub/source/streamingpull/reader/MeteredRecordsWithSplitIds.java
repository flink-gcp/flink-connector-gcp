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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import com.google.pubsub.v1.PubsubMessage;

import javax.annotation.Nullable;

import java.util.Set;

/** Counts a fetched batch until the source reader takes each message from it. */
@Internal
final class MeteredRecordsWithSplitIds implements RecordsWithSplitIds<PubsubMessage> {

    private final RecordsWithSplitIds<PubsubMessage> delegate;
    private final PubSubSourceReaderMetrics metrics;
    private long remainingMessages;
    private long remainingBytes;
    private boolean recycled;

    MeteredRecordsWithSplitIds(
            RecordsWithSplitIds<PubsubMessage> delegate,
            long messages,
            long bytes,
            PubSubSourceReaderMetrics metrics) {
        this.delegate = delegate;
        this.remainingMessages = messages;
        this.remainingBytes = bytes;
        this.metrics = metrics;
        metrics.recordsEnteredFetcher(messages, bytes);
    }

    @Override
    @Nullable
    public String nextSplit() {
        return delegate.nextSplit();
    }

    @Override
    @Nullable
    public PubsubMessage nextRecordFromSplit() {
        PubsubMessage message = delegate.nextRecordFromSplit();
        if (message != null) {
            long messageBytes = message.getSerializedSize();
            remainingMessages--;
            remainingBytes -= messageBytes;
            metrics.recordLeftFetcher(messageBytes);
        }
        return message;
    }

    @Override
    public Set<String> finishedSplits() {
        return delegate.finishedSplits();
    }

    @Override
    public void recycle() {
        if (recycled) {
            return;
        }
        recycled = true;
        metrics.recordsLeftFetcher(remainingMessages, remainingBytes);
        remainingMessages = 0;
        remainingBytes = 0;
        delegate.recycle();
    }
}
