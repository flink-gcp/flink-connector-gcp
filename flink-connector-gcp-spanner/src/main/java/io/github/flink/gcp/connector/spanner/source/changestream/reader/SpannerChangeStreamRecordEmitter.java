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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import java.util.ArrayList;
import java.util.List;

/** Emits decoded Spanner changes and advances partition progress on the task thread. */
@Internal
final class SpannerChangeStreamRecordEmitter<T>
        implements RecordEmitter<SpannerChangeStreamRecord, T, ChangeStreamPartitionSplitState> {

    private final SpannerChangeStreamDeserializationSchema<T> deserializer;
    private final SpannerChangeStreamRecordFilter recordFilter;
    private final boolean filtersActive;
    private final SourceReaderContext context;
    private final SpannerChangeStreamReaderMetrics metrics;

    @VisibleForTesting
    SpannerChangeStreamRecordEmitter(
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            SpannerChangeStreamRecordFilter recordFilter,
            SourceReaderContext context,
            SpannerChangeStreamReaderMetrics metrics) {
        this(
                deserializer,
                recordFilter,
                Preconditions.checkNotNull(recordFilter, "recordFilter must not be null")
                        .hasFilters(),
                context,
                metrics);
    }

    SpannerChangeStreamRecordEmitter(
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            SpannerChangeStreamRecordFilter recordFilter,
            boolean filtersActive,
            SourceReaderContext context,
            SpannerChangeStreamReaderMetrics metrics) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.recordFilter =
                Preconditions.checkNotNull(recordFilter, "recordFilter must not be null");
        this.filtersActive = filtersActive;
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public void emitRecord(
            SpannerChangeStreamRecord record,
            SourceOutput<T> output,
            ChangeStreamPartitionSplitState state)
            throws Exception {
        Preconditions.checkNotNull(record, "record must not be null");
        Preconditions.checkNotNull(output, "output must not be null");
        Preconditions.checkNotNull(state, "state must not be null");

        if (record instanceof SpannerChangeStreamRecord.Data) {
            emitData(((SpannerChangeStreamRecord.Data) record).record, output);
        } else if (record instanceof SpannerChangeStreamRecord.Heartbeat) {
            // The heartbeat advances the watermark below together with the current position.
        } else if (record instanceof SpannerChangeStreamRecord.Children) {
            context.sendSourceEventToCoordinator(
                    childrenEvent(
                            state.splitId(),
                            state.isInitialPartition(),
                            (SpannerChangeStreamRecord.Children) record));
        } else {
            throw new IllegalArgumentException("Unsupported Spanner Change Streams record.");
        }

        state.advance(
                record.position(),
                record instanceof SpannerChangeStreamRecord.Heartbeat
                        ? record.position()
                        : state.getWatermark());
        context.sendSourceEventToCoordinator(
                new PartitionProgressEvent(
                        state.splitId(), state.getCurrentPosition(), state.getWatermark()));
    }

    private void emitData(DataChangeRecord record, SourceOutput<T> output) throws Exception {
        if (!filtersActive) {
            deserialize(record, output);
            return;
        }

        SpannerChangeStreamRecordFilter.Result filtered = recordFilter.filter(record);
        switch (filtered.getDisposition()) {
            case TABLE_FILTERED:
                metrics.filteredByTable();
                break;
            case SKIPPED_WITHOUT_CHANGE:
                metrics.skippedWithoutChange();
                break;
            case DELIVER:
                metrics.columnOccurrencesFiltered(filtered.getRemovedColumnOccurrences());
                deserialize(filtered.getRecord(), output);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Spanner Change Streams filter disposition.");
        }
    }

    private void deserialize(DataChangeRecord record, SourceOutput<T> output) throws Exception {
        long timestamp = record.getCommitTimestamp().toEpochMilli();
        long emittedCount =
                SynchronousDeserializationCollector.<T, Exception>deserialize(
                        emitted -> output.collect(emitted, timestamp),
                        out -> deserializer.deserialize(record, out));
        if (emittedCount == 0) {
            metrics.skipped();
        }
    }

    private static ChildPartitionsEvent childrenEvent(
            String parentSplitId,
            boolean initialPartition,
            SpannerChangeStreamRecord.Children record) {
        List<ChildPartitionsEvent.ChildPartition> children = new ArrayList<>();
        for (SpannerChangeStreamRecord.Child child : record.children) {
            List<String> parentIds = new ArrayList<>();
            if (child.initialParent) {
                parentIds.add(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
            }
            for (String token : child.parentTokens) {
                parentIds.add(ChangeStreamPartitionSplit.idForToken(token));
            }
            if (parentIds.isEmpty() && initialPartition) {
                parentIds.add(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
            }
            children.add(new ChildPartitionsEvent.ChildPartition(child.token, parentIds));
        }
        return new ChildPartitionsEvent(parentSplitId, record.startTimestamp, children);
    }
}
