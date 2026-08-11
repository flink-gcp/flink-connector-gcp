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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

/** Pulls change-stream records for one assigned partition at a time. */
@Internal
public final class BigtableChangeStreamSplitReader
        implements SplitReader<ChangeStreamRecord, ChangeStreamPartitionSplit> {

    private static final int MAX_RECORDS_PER_FETCH = 1000;

    private final TableDestination table;
    private final ChangeStreamOpener opener;
    @Nullable private final Instant endTime;
    private final Deque<ChangeStreamPartitionSplit> queued = new ArrayDeque<>();
    @Nullable private volatile Active active;
    private volatile boolean pendingWakeUp;

    public BigtableChangeStreamSplitReader(
            TableDestination table, ChangeStreamOpener opener, @Nullable Instant endTime) {
        this.table = table;
        this.opener = opener;
        this.endTime = endTime;
    }

    @Override
    public RecordsWithSplitIds<ChangeStreamRecord> fetch() throws IOException {
        RecordsBySplits.Builder<ChangeStreamRecord> batch = new RecordsBySplits.Builder<>();
        if (active == null) {
            ChangeStreamPartitionSplit split = queued.poll();
            if (split == null) {
                return batch.build();
            }
            active = new Active(split, opener.open(table, split, endTime));
        }
        Active current = active;
        if (pendingWakeUp) {
            pendingWakeUp = false;
            return batch.build();
        }
        if (current.needsOpen()) {
            current.reopen(opener.open(table, current.split, endTime));
        }
        for (int i = 0; i < MAX_RECORDS_PER_FETCH; i++) {
            ChangeStreamRecord record;
            try {
                record = current.next();
            } catch (RuntimeException e) {
                if (current.cancelled) {
                    current.prepareReopen();
                    return batch.build();
                }
                throw new IOException(
                        "Failed to read Bigtable Change Streams for " + table + ".", e);
            }
            if (record == null) {
                if (current.cancelled) {
                    current.prepareReopen();
                    return batch.build();
                }
                throw new IOException(
                        "Bigtable Change Streams ended without a CloseStream record for split "
                                + current.split.splitId()
                                + ".");
            }
            batch.add(current.split.splitId(), record);
            current.advance(record);
            if (record instanceof CloseStream) {
                batch.addFinishedSplit(current.split.splitId());
                current.close();
                active = null;
                break;
            }
        }
        return batch.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<ChangeStreamPartitionSplit> changes) {
        if (!(changes instanceof SplitsAddition)) {
            throw new IllegalArgumentException("Unsupported split change " + changes + ".");
        }
        queued.addAll(changes.splits());
    }

    @Override
    public void wakeUp() {
        Active current = active;
        if (current == null) {
            pendingWakeUp = true;
        } else {
            current.cancel();
        }
    }

    @Override
    public void close() {
        Active current = active;
        if (current != null) {
            current.cancel();
            current.close();
            active = null;
        }
    }

    private static final class Active {
        private ChangeStreamPartitionSplit split;
        @Nullable private ChangeStream stream;
        private volatile boolean cancelled;

        private Active(ChangeStreamPartitionSplit split, ChangeStream stream) {
            this.split = split;
            this.stream = stream;
        }

        @Nullable
        private ChangeStreamRecord next() {
            ChangeStream current = stream;
            return current == null ? null : current.next();
        }

        private void cancel() {
            cancelled = true;
            ChangeStream current = stream;
            if (current != null) {
                current.cancel();
            }
        }

        private void prepareReopen() {
            cancelled = false;
            close();
            stream = null;
        }

        private void close() {
            ChangeStream current = stream;
            if (current != null) {
                current.close();
            }
        }

        private boolean needsOpen() {
            return stream == null;
        }

        private void reopen(ChangeStream reopened) {
            stream = reopened;
        }

        private void advance(ChangeStreamRecord record) {
            ChangeStreamContinuationToken token;
            Instant watermark;
            if (record instanceof ChangeStreamMutation) {
                ChangeStreamMutation mutation = (ChangeStreamMutation) record;
                token =
                        ChangeStreamContinuationToken.create(
                                split.getPartition(), mutation.getToken());
                watermark = mutation.getEstimatedLowWatermarkTime();
            } else if (record instanceof Heartbeat) {
                Heartbeat heartbeat = (Heartbeat) record;
                token = heartbeat.getChangeStreamContinuationToken();
                watermark = heartbeat.getEstimatedLowWatermarkTime();
            } else {
                return;
            }
            split =
                    new ChangeStreamPartitionSplit(
                            split.splitId(),
                            split.getPartition(),
                            java.util.Collections.singletonList(token),
                            watermark);
        }
    }
}
