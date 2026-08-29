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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers itself, in one place so that this file is the
 * connector's inventory: what it reports can be read here without opening the writer.
 *
 * <p>Each connector has one of these, and comparing them is how the repository's metric naming
 * convention is held across connectors — a name that means the same thing in two connectors should
 * be spelled the same way, and a diff of these files is what shows it. The convention itself (a
 * counter names the event, a gauge names the state, and neither takes Flink's {@code num} prefix)
 * is recorded in the base module's detailed agent guidance.
 *
 * <p>What is <em>not</em> here: Flink's standard sink names, which come from {@code
 * SinkWriterMetricGroup} accessors rather than from a name, and the subgroup leaves {@code
 * base.metrics} registers on this connector's behalf ({@code errorClass.CODE.errors}). The
 * user-facing meaning of each name is on the connector's documentation page, not duplicated here.
 */
@Internal
public final class BigtableMetricNames {

    // Registered by the sink writer (BigtableWriterMetrics). The two counts are of entries — one
    // per record written — rather than of the mutations each carries, and say so, because the
    // knobs they are read against count entries too and Bigtable's own limit does not.
    public static final String IN_FLIGHT_ENTRIES = "inFlightEntries";
    public static final String IN_FLIGHT_BYTES = "inFlightBytes";
    public static final String PARKED_ENTRIES = "parkedEntries";
    public static final String ACTIVE_CLIENTS = "activeClients";
    public static final String CAPACITY_EVICTIONS = "capacityEvictions";
    public static final String IDLE_EVICTIONS = "idleEvictions";

    /** Registered by the sink writer and by the source readers, on their own groups. */
    public static final String RECORDS_SKIPPED = "recordsSkipped";

    public static final String TABLES_CREATED = "tablesCreated";
    public static final String COLUMN_FAMILIES_ADDED = "columnFamiliesAdded";

    // Registered by the scan source's reader (BigtableSourceReaderMetrics) or the Change Streams
    // reader (BigtableChangeStreamReaderMetrics), as named below.
    public static final String ROWS_READ = "rowsRead";
    public static final String CHANGE_STREAM_MUTATIONS_READ = "changeStreamMutationsRead";
    public static final String CHANGE_STREAM_HEARTBEATS_READ = "changeStreamHeartbeatsRead";
    public static final String CHANGE_STREAM_READS_STARTED = "changeStreamReadsStarted";
    public static final String ACTIVE_CHANGE_STREAM_READS = "activeChangeStreamReads";
    public static final String QUEUED_CHANGE_STREAM_PARTITIONS = "queuedChangeStreamPartitions";
    public static final String QUEUED_CHANGE_STREAM_PARTITION_LAG_MILLIS =
            "queuedChangeStreamPartitionLagMillis";
    public static final String MISSED_HEARTBEAT_INTERVALS = "missedHeartbeatIntervals";
    public static final String CHANGE_STREAM_CLOSE_STREAMS_READ = "changeStreamCloseStreamsRead";
    public static final String CHANGE_STREAM_USER_MUTATIONS_READ = "changeStreamUserMutationsRead";
    public static final String CHANGE_STREAM_GARBAGE_COLLECTION_MUTATIONS_READ =
            "changeStreamGarbageCollectionMutationsRead";
    public static final String CHANGE_STREAM_MUTATION_ENTRIES_FILTERED =
            "changeStreamMutationEntriesFiltered";
    public static final String CHANGE_STREAM_RECORDS_SKIPPED_WITHOUT_CHANGE =
            "changeStreamRecordsSkippedWithoutChange";
    public static final String PARTITION_LOW_WATERMARK_MILLIS = "partitionLowWatermarkMillis";

    // Registered by the Change Streams split enumerator.
    public static final String CHANGE_STREAM_PARTITIONS_DISCOVERED =
            "changeStreamPartitionsDiscovered";
    public static final String CHANGE_STREAM_PARTITION_SPLITS = "changeStreamPartitionSplits";
    public static final String CHANGE_STREAM_PARTITION_MERGES = "changeStreamPartitionMerges";
    public static final String UNASSIGNED_CHANGE_STREAM_PARTITION_LAG_MILLIS =
            "unassignedChangeStreamPartitionLagMillis";
    public static final String CHANGE_STREAM_PARTITIONS_RECONCILED =
            "changeStreamPartitionsReconciled";
    public static final String CHANGE_STREAM_TOKENLESS_RESTARTS = "changeStreamTokenlessRestarts";

    // Registered by the source split enumerators, on their own groups.
    public static final String SPLITS_ASSIGNED = "splitsAssigned";
    public static final String SPLITS_RETURNED = "splitsReturned";

    // Registered by an enumerator that plans by sampling, which the Change Streams enumerator does
    // not: it takes its partitions from the service.
    public static final String ROW_KEY_SAMPLES_TAKEN = "rowKeySamplesTaken";

    private BigtableMetricNames() {}
}
