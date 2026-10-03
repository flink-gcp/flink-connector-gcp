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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.datastore.source.batch.FetchedEntity;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplitState;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Reads the splits this subtask is assigned, asking the enumerator for the next one each time it
 * finishes one.
 *
 * @param <T> the record type produced
 */
@Internal
public class DatastoreSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                FetchedEntity, T, QuerySplit, QuerySplitState> {

    private final QueryPageReader pageReader;

    /**
     * Creates the reader.
     *
     * @param splitReaderSupplier supplies a split reader per fetcher
     * @param recordEmitter deserializes entities and advances the split state
     * @param config the Flink configuration
     * @param context the reader context
     * @param pageReader the page reader this reader owns and closes; the split readers share it
     */
    public DatastoreSourceReader(
            Supplier<SplitReader<FetchedEntity, QuerySplit>> splitReaderSupplier,
            RecordEmitter<FetchedEntity, T, QuerySplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            QueryPageReader pageReader) {
        super(splitReaderSupplier, recordEmitter, config, context);
        this.pageReader = pageReader;
    }

    @Override
    public void start() {
        // A restored reader is given its splits before it is started, so an empty assignment here
        // means the first split has to be asked for.
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected QuerySplitState initializedState(QuerySplit split) {
        return new QuerySplitState(split);
    }

    @Override
    protected QuerySplit toSplitType(String splitId, QuerySplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, QuerySplitState> finishedSplits) {
        context.sendSplitRequest();
    }

    @Override
    public void close() throws Exception {
        // The page reader is closed after super.close() has waited for the fetchers, up to Flink's
        // source.reader.close.timeout; a page call still running past it, which an interrupt does
        // not end, then fails against the closed client.
        Closers.closeAll(super::close, pageReader);
    }
}
