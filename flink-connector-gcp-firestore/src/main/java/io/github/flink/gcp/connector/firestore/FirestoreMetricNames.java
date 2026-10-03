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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers itself, in one place so that this file is the
 * connector's inventory: what it reports can be read here without opening the writer.
 *
 * <p>Counters name the event that happened ({@code recordsSkipped}), gauges name the state they
 * read ({@code inFlightWrites}), and no name takes Flink's {@code num} prefix — a name meaning the
 * same thing in another connector of this project is spelled the same way there.
 *
 * <p>What is <em>not</em> here: Flink's standard sink and source names, which come from {@code
 * SinkWriterMetricGroup} and {@code SourceReaderMetricGroup} accessors rather than from a name, and
 * the subgroup leaves {@code base.metrics} registers on this connector's behalf ({@code
 * errorClass.CODE.errors}). The user-facing meaning of each name is on the connector's
 * documentation page, not duplicated here.
 */
@Internal
public final class FirestoreMetricNames {

    // Registered by the sink writer (FirestoreWriterMetrics). A write is one record the serializer
    // returned, one document operation.
    public static final String IN_FLIGHT_WRITES = "inFlightWrites";
    public static final String IN_FLIGHT_BYTES = "inFlightBytes";
    public static final String PARKED_WRITES = "parkedWrites";
    public static final String RECORDS_SKIPPED = "recordsSkipped";
    public static final String WRITES_RETRIED = "writesRetried";
    public static final String BULK_WRITERS_REPLACED = "bulkWritersReplaced";
    public static final String WRITES_CONFIRMED_ALONE = "writesConfirmedAlone";

    // Registered by the batch source's split enumerator (FirestoreBatchSplitEnumerator).
    public static final String SPLITS_ASSIGNED = "splitsAssigned";
    public static final String SPLITS_RETURNED = "splitsReturned";
    public static final String READS_PLANNED = "readsPlanned";

    // Registered by the batch source's reader (FirestoreSourceReaderMetrics). RECORDS_SKIPPED above
    // is registered there too, for a document the deserializer produced no record for.
    public static final String DOCUMENTS_READ = "documentsRead";

    private FirestoreMetricNames() {}
}
