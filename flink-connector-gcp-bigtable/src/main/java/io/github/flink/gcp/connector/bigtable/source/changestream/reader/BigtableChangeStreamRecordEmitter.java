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
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Emits mutations with commit timestamps and advances token/watermark state. */
@Internal
public final class BigtableChangeStreamRecordEmitter<T>
        implements RecordEmitter<ChangeStreamRecord, T, ChangeStreamPartitionSplitState> {

    private final BigtableChangeStreamDeserializationSchema<T> deserializer;
    private final SourceReaderContext context;
    private final BigtableChangeStreamReaderMetrics metrics;
    private final TimestampCollector collector = new TimestampCollector();

    public BigtableChangeStreamRecordEmitter(
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            SourceReaderContext context,
            BigtableChangeStreamReaderMetrics metrics) {
        this.deserializer = deserializer;
        this.context = context;
        this.metrics = metrics;
    }

    @Override
    public void emitRecord(
            ChangeStreamRecord record,
            SourceOutput<T> output,
            ChangeStreamPartitionSplitState state)
            throws Exception {
        if (record instanceof ChangeStreamMutation) {
            ChangeStreamMutation mutation = (ChangeStreamMutation) record;
            collector.retarget(output, mutation.getCommitTime().toEpochMilli());
            try {
                deserializer.deserialize(mutation, collector);
                if (collector.emitted == 0) {
                    metrics.skipped();
                }
            } finally {
                collector.retarget(null, 0);
            }
            state.advance(
                    ChangeStreamContinuationToken.create(
                            state.toSplit().getPartition(), mutation.getToken()),
                    mutation.getEstimatedLowWatermarkTime());
            metrics.mutation(mutation.getEstimatedLowWatermarkTime());
            return;
        }
        if (record instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) record;
            state.advance(
                    heartbeat.getChangeStreamContinuationToken(),
                    heartbeat.getEstimatedLowWatermarkTime());
            metrics.heartbeat(heartbeat.getEstimatedLowWatermarkTime());
            return;
        }
        if (record instanceof CloseStream) {
            CloseStream close = (CloseStream) record;
            List<PartitionTransitionEvent.Successor> successors = new ArrayList<>();
            List<ChangeStreamContinuationToken> tokens = close.getChangeStreamContinuationTokens();
            List<ByteStringRange> partitions = close.getNewPartitions();
            Preconditions.checkState(
                    partitions.isEmpty() || partitions.size() == tokens.size(),
                    "Bigtable CloseStream returned %s continuation token(s) and %s successor"
                            + " partition(s); successor partitions must be absent or paired with"
                            + " every token.",
                    tokens.size(),
                    partitions.size());
            for (int i = 0; i < tokens.size(); i++) {
                ByteStringRange target =
                        partitions.isEmpty() ? tokens.get(i).getPartition() : partitions.get(i);
                successors.add(new PartitionTransitionEvent.Successor(target, tokens.get(i)));
            }
            context.sendSourceEventToCoordinator(
                    new PartitionTransitionEvent(
                            state.toSplit().splitId(), state.getLowWatermark(), successors));
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported Bigtable change-stream record " + record + ".");
    }

    private final class TimestampCollector implements Collector<T> {
        @Nullable private SourceOutput<T> output;
        private long timestamp;
        private int emitted;

        private void retarget(@Nullable SourceOutput<T> output, long timestamp) {
            this.output = output;
            this.timestamp = timestamp;
            this.emitted = 0;
        }

        @Override
        public void collect(T record) {
            Preconditions.checkState(output != null, "The change-stream collector is not active.");
            output.collect(record, timestamp);
            emitted++;
        }

        @Override
        public void close() {}
    }
}
