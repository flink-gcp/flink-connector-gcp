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
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
import io.github.flink.gcp.connector.datastore.source.batch.FetchedEntity;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplitState;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;

/**
 * Deserializes each entity and records that the split has passed it.
 *
 * <p>Runs on the task thread, one entity at a time, as {@code pollNext} drains the element queue.
 *
 * @param <T> the record type produced
 */
@Internal
public class DatastoreRecordEmitter<T> implements RecordEmitter<FetchedEntity, T, QuerySplitState> {

    private final DatastoreEntityDeserializationSchema<T> deserializer;
    private final DatastoreSourceReaderMetrics metrics;

    /**
     * Creates the emitter.
     *
     * @param deserializer the deserializer turning entities into records
     * @param metrics the reader's metrics
     */
    public DatastoreRecordEmitter(
            DatastoreEntityDeserializationSchema<T> deserializer,
            DatastoreSourceReaderMetrics metrics) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public void emitRecord(FetchedEntity entity, SourceOutput<T> output, QuerySplitState splitState)
            throws Exception {
        long emittedCount =
                SynchronousDeserializationCollector.<T, Exception>deserialize(
                        output::collect, out -> deserializer.deserialize(entity.getEntity(), out));
        if (emittedCount == 0) {
            metrics.recordSkipped();
        }
        // Outside the branch above: the split resumes at a cursor, so an entity that produced no
        // record has still been passed. A deserializer that threw never gets here, and the job
        // fails rather than advancing past an entity nobody saw.
        splitState.recordEmitted(entity);
    }
}
