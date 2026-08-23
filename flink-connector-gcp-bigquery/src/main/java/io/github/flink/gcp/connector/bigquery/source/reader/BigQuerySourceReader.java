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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplitState;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Reader subtask consuming the read streams it is assigned, one at a time.
 *
 * <p>Splits are pulled rather than pushed: this reader asks for one when it starts with none, and
 * asks for the next one as soon as it finishes one. A subtask that draws a small stream therefore
 * goes back for more work instead of idling, which is what replaces the stream-splitting the
 * Storage Read API offers and FLIP-27 has no hook for.
 *
 * <p>Splits <em>are</em> checkpointed here, unlike the Pub/Sub source's: a read stream's progress
 * is the reader's alone, and the enumerator cannot recompute it.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class BigQuerySourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                GenericRecord, T, ReadStreamSplit, BigQueryReadStreamSplitState> {

    private final RowStreamOpener opener;

    /**
     * Creates the reader.
     *
     * @param splitReaderSupplier supplies the split readers decoding the assigned streams
     * @param recordEmitter converts rows and advances their stream's offset
     * @param config the TaskManager configuration, which carries the source reader options
     * @param context the reader context
     * @param opener the stream opener shared by this subtask's split readers; closed here, once
     */
    public BigQuerySourceReader(
            Supplier<SplitReader<GenericRecord, ReadStreamSplit>> splitReaderSupplier,
            RecordEmitter<GenericRecord, T, BigQueryReadStreamSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            RowStreamOpener opener) {
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
    protected BigQueryReadStreamSplitState initializedState(ReadStreamSplit split) {
        return new BigQueryReadStreamSplitState(split);
    }

    @Override
    protected ReadStreamSplit toSplitType(String splitId, BigQueryReadStreamSplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, BigQueryReadStreamSplitState> finishedSplits) {
        context.sendSplitRequest();
    }

    @Override
    public void close() throws Exception {
        // super.close() shuts the fetchers down, so every split reader has released its stream by
        // the time the client behind them is closed.
        Closers.closeAll(super::close, opener);
    }
}
