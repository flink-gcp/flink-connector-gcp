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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplit;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplitState;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Reads the partitions this subtask is assigned, asking the enumerator for the next one each time
 * it finishes one.
 *
 * @param <T> the record type produced
 */
@Internal
public class SpannerSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Struct, T, BatchReadSplit, BatchReadSplitState> {

    private final StructStreamOpener opener;

    /**
     * Creates the reader.
     *
     * @param splitReaderSupplier supplies a split reader per fetcher
     * @param recordEmitter deserializes rows
     * @param config the Flink configuration
     * @param context the reader context
     * @param opener the read opener this reader owns and closes; the split readers share it
     */
    public SpannerSourceReader(
            Supplier<SplitReader<Struct, BatchReadSplit>> splitReaderSupplier,
            RecordEmitter<Struct, T, BatchReadSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            StructStreamOpener opener) {
        super(splitReaderSupplier, recordEmitter, config, context);
        this.opener = opener;
    }

    @Override
    public void start() {
        // A restored reader is given its splits before it is started, so an empty assignment here
        // means there is nothing to work through and the first split has to be asked for.
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected BatchReadSplitState initializedState(BatchReadSplit split) {
        return new BatchReadSplitState(split);
    }

    @Override
    protected BatchReadSplit toSplitType(String splitId, BatchReadSplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, BatchReadSplitState> finishedSplits) {
        context.sendSplitRequest();
    }

    @Override
    public void close() throws Exception {
        // The opener is closed after the fetchers are down, which super.close() waits for: a
        // fetcher still reading through a released client would fail a job that was shutting down
        // cleanly.
        Closers.closeAll(super::close, opener);
    }
}
