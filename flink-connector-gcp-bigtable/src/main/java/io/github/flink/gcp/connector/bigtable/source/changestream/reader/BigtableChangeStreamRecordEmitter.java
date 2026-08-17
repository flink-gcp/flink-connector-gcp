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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitions;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import java.util.ArrayList;
import java.util.List;

/** Emits mutations with commit timestamps and advances token/watermark state. */
@Internal
public final class BigtableChangeStreamRecordEmitter<T>
        implements RecordEmitter<ChangeStreamRecord, T, ChangeStreamPartitionSplitState> {

    private final BigtableChangeStreamDeserializationSchema<T> deserializer;
    private final BigtableChangeStreamMutationFilter mutationFilter;
    private final SourceReaderContext context;
    private final BigtableChangeStreamReaderMetrics metrics;

    public BigtableChangeStreamRecordEmitter(
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            SourceReaderContext context,
            BigtableChangeStreamReaderMetrics metrics) {
        this(deserializer, BigtableChangeStreamMutationFilter.none(), context, metrics);
    }

    public BigtableChangeStreamRecordEmitter(
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            BigtableChangeStreamMutationFilter mutationFilter,
            SourceReaderContext context,
            BigtableChangeStreamReaderMetrics metrics) {
        this.deserializer = deserializer;
        this.mutationFilter = mutationFilter;
        this.context = context;
        this.metrics = metrics;
    }

    @Override
    public void emitRecord(
            ChangeStreamRecord record,
            SourceOutput<T> output,
            ChangeStreamPartitionSplitState state)
            throws Exception {
        if (record instanceof com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation) {
            com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation sdkMutation =
                    (com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation) record;
            long removedEntries = 0;
            if (mutationFilter.hasEntryFilters()) {
                ChangeStreamMutationConverter.Result result =
                        ChangeStreamMutationConverter.convertFiltered(sdkMutation, mutationFilter);
                removedEntries = result.getRemovedEntries();
                if (result.isSkipped()) {
                    metrics.skippedWithoutChange();
                } else {
                    emitMutation(result.getMutation(), output);
                }
            } else {
                emitMutation(ChangeStreamMutationConverter.convert(sdkMutation), output);
            }
            state.advance(
                    ChangeStreamContinuationToken.create(
                            ChangeStreamPartitions.sdkRange(state.toSplit().getPartition()),
                            sdkMutation.getToken()),
                    sdkMutation.getEstimatedLowWatermarkTime());
            if (removedEntries > 0) {
                metrics.entriesFiltered(removedEntries);
            }
            metrics.mutation(sdkMutation);
            return;
        }
        if (record instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) record;
            state.advance(
                    heartbeat.getChangeStreamContinuationToken(),
                    heartbeat.getEstimatedLowWatermarkTime());
            context.sendSourceEventToCoordinator(
                    new PartitionProgressEvent(
                            state.toSplit().splitId(),
                            heartbeat.getChangeStreamContinuationToken(),
                            heartbeat.getEstimatedLowWatermarkTime()));
            metrics.heartbeat();
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

    private void emitMutation(ChangeStreamMutation mutation, SourceOutput<T> output)
            throws Exception {
        long timestamp = mutation.getCommitTime().toEpochMilli();
        long emittedCount =
                SynchronousDeserializationCollector.<T, Exception>deserialize(
                        emitted -> output.collect(emitted, timestamp),
                        out -> deserializer.deserialize(mutation, out));
        if (emittedCount == 0) {
            metrics.skipped();
        }
    }
}
