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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;

/**
 * The batch reader's own counters, beside the ones Flink registers for every source.
 *
 * <p>Entities are counted by the split reader on a fetcher thread, skipped entities by the record
 * emitter on the task thread, so only the first counter needs to be thread-safe.
 */
@Internal
public class DatastoreSourceReaderMetrics {

    private final Counter entitiesRead;
    private final Counter recordsSkipped;

    /**
     * Registers the counters.
     *
     * @param metricGroup the reader's metric group
     */
    public DatastoreSourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        Preconditions.checkNotNull(metricGroup, "metricGroup must not be null");
        this.entitiesRead =
                metricGroup.counter(
                        DatastoreMetricNames.ENTITIES_READ, new ThreadSafeSimpleCounter());
        this.recordsSkipped = metricGroup.counter(DatastoreMetricNames.RECORDS_SKIPPED);
    }

    /** Counts one entity a page returned. Called from a fetcher thread. */
    public void entityRead() {
        entitiesRead.inc();
    }

    /** Counts one entity the deserializer produced no record for. Called from the task thread. */
    public void recordSkipped() {
        recordsSkipped.inc();
    }
}
