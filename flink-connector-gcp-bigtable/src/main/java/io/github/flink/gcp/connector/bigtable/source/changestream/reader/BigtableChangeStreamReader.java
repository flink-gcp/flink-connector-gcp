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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Source reader for Bigtable Change Streams. */
@Internal
public final class BigtableChangeStreamReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                ChangeStreamRecord,
                T,
                ChangeStreamPartitionSplit,
                ChangeStreamPartitionSplitState> {

    private final ChangeStreamOpener opener;
    private final ChangeStreamRestoreResolver restoreResolver;
    private final java.util.Optional<io.github.flink.gcp.connector.base.source.StartPosition>
            resumeFallback;
    private boolean started;

    public BigtableChangeStreamReader(
            SourceReaderContext context, BigtableChangeStreamSourceConfig<T> config) {
        this(context, config, new BigtableChangeStreamReaderMetrics(context.metricGroup()));
    }

    private BigtableChangeStreamReader(
            SourceReaderContext context,
            BigtableChangeStreamSourceConfig<T> config,
            BigtableChangeStreamReaderMetrics metrics) {
        super(
                () ->
                        new BigtableChangeStreamSplitReader(
                                config.getTable(), config.getOpener(), config.getEndTime()),
                new BigtableChangeStreamRecordEmitter<>(config.getDeserializer(), context, metrics),
                context.getConfiguration(),
                context);
        this.opener = config.getOpener();
        this.restoreResolver = config.getRestoreResolver();
        this.resumeFallback = config.getResumeFallback();
    }

    @Override
    public void addSplits(List<ChangeStreamPartitionSplit> splits) {
        if (started) {
            super.addSplits(splits);
            return;
        }
        List<ChangeStreamPartitionSplit> resolved = new ArrayList<>(splits.size());
        for (ChangeStreamPartitionSplit split : splits) {
            try {
                resolved.add(restoreResolver.resolve(split, resumeFallback));
            } catch (Exception e) {
                throw new org.apache.flink.util.FlinkRuntimeException(
                        "Failed to validate restored Bigtable Change Streams split "
                                + split.splitId()
                                + ".",
                        e);
            }
        }
        super.addSplits(resolved);
    }

    @Override
    public void start() {
        started = true;
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected ChangeStreamPartitionSplitState initializedState(ChangeStreamPartitionSplit split) {
        return new ChangeStreamPartitionSplitState(split);
    }

    @Override
    protected ChangeStreamPartitionSplit toSplitType(
            String splitId, ChangeStreamPartitionSplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, ChangeStreamPartitionSplitState> finishedSplits) {
        context.sendSplitRequest();
    }

    @Override
    public void close() throws Exception {
        Closers.closeAll(super::close, opener);
    }
}
