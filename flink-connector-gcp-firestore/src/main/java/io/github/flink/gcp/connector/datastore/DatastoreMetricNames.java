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

package io.github.flink.gcp.connector.datastore;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name the Datastore-mode connector registers itself, in one place so that this file
 * is the connector's inventory: what it reports can be read here without opening the writer.
 *
 * <p>Counters name the event that happened ({@code recordsSkipped}), gauges name the state they
 * read ({@code bufferedMutations}), and no name takes Flink's {@code num} prefix — a name meaning
 * the same thing in another connector of this project is spelled the same way there.
 *
 * <p>What is <em>not</em> here: Flink's standard sink and source names, which come from {@code
 * SinkWriterMetricGroup} and {@code SourceReaderMetricGroup} accessors rather than from a name, and
 * the subgroup leaves {@code base.metrics} registers on this connector's behalf ({@code
 * errorClass.CODE.errors}). The user-facing meaning of each name is on the connector's
 * documentation page, not duplicated here.
 */
@Internal
public final class DatastoreMetricNames {

    // Registered by the sink writer (DatastoreWriterMetrics). A mutation is one record the
    // serializer returned, one entity operation.
    public static final String BUFFERED_MUTATIONS = "bufferedMutations";
    public static final String BUFFERED_BYTES = "bufferedBytes";
    public static final String RECORDS_SKIPPED = "recordsSkipped";
    public static final String MUTATIONS_RETRIED = "mutationsRetried";
    public static final String BATCHES_SENT = "batchesSent";
    public static final String MUTATIONS_CONFIRMED_ALONE = "mutationsConfirmedAlone";
    public static final String THROTTLED_MILLIS = "throttledMillis";

    // Registered by the batch source's split enumerator (DatastoreBatchSplitEnumerator).
    public static final String SPLITS_ASSIGNED = "splitsAssigned";
    public static final String SPLITS_RETURNED = "splitsReturned";
    public static final String READS_PLANNED = "readsPlanned";

    // Registered by the batch source's reader (DatastoreSourceReaderMetrics). RECORDS_SKIPPED above
    // is registered there too, for an entity the deserializer produced no record for.
    public static final String ENTITIES_READ = "entitiesRead";

    private DatastoreMetricNames() {}
}
