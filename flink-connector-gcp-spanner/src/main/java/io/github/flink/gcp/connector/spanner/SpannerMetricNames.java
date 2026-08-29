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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers, in one place.
 *
 * <p>Counters name the event that happened ({@code recordsSkipped}), gauges name the state they
 * read ({@code bufferedCells}), and no name takes Flink's {@code num} prefix — a name meaning the
 * same thing in another connector of this project is spelled the same way there.
 *
 * <p>Deliberately absent: the names Flink itself provides through {@code SinkWriterMetricGroup}
 * ({@code numRecordsSend}, {@code numBytesSend}, {@code numRecordsSendErrors}) and through the
 * source's own groups ({@code numRecordsIn}, {@code unassignedSplits}), and the templated leaves of
 * the shared {@code base.metrics} subgroups ({@code errorClass.CODE.errors}) — none of them is this
 * connector's to name.
 */
@Internal
public final class SpannerMetricNames {

    // Registered by the sink writer (SpannerWriterMetrics).

    /** Mutations held in the writer's batch, waiting for the next flush. */
    public static final String BUFFERED_MUTATIONS = "bufferedMutations";

    /**
     * Mutation cells held in the writer's batch, counted the way Spanner counts a mutation — index
     * entries included.
     */
    public static final String BUFFERED_CELLS = "bufferedCells";

    /** Estimated bytes of the mutations held in the writer's batch. */
    public static final String BUFFERED_BYTES = "bufferedBytes";

    /** Records the sink serializer skipped or source inputs whose deserializer emitted nothing. */
    public static final String RECORDS_SKIPPED = "recordsSkipped";

    /**
     * Mutations re-sent after a transient failure. Counted per re-send, so one mutation retried
     * three times contributes three — the question it answers is how much work the retry loop is
     * doing, not how many mutations were unlucky.
     */
    public static final String MUTATIONS_RETRIED = "mutationsRetried";

    /** Batch write requests the writer sent, first attempts and re-sends alike. */
    public static final String BATCHES_SENT = "batchesSent";

    // Registered by both split enumerators (SpannerBatchReadSplitEnumerator and
    // SpannerChangeStreamSplitEnumerator).

    /** Partition splits handed to a reader. */
    public static final String SPLITS_ASSIGNED = "splitsAssigned";

    /** Partition splits a failed reader gave back, to be handed out again. */
    public static final String SPLITS_RETURNED = "splitsReturned";

    // Registered by the Change Streams coordinator (SpannerChangeStreamSplitEnumerator).

    /** Child partition tokens first accepted into the Change Streams coordinator ledger. */
    public static final String CHANGE_STREAM_PARTITIONS_DISCOVERED =
            "changeStreamPartitionsDiscovered";

    /** Lag of the oldest scheduled Change Streams partition no reader owns yet. */
    public static final String UNASSIGNED_CHANGE_STREAM_PARTITION_LAG_MILLIS =
            "unassignedChangeStreamPartitionLagMillis";

    /** Unfinished partition entries held in the Change Streams coordinator ledger. */
    public static final String CHANGE_STREAM_PARTITION_LEDGER_ENTRIES =
            "changeStreamPartitionLedgerEntries";

    /** Finished parent IDs retained while a Change Streams child awaits another parent. */
    public static final String CHANGE_STREAM_FINISHED_PARENT_PROOFS =
            "changeStreamFinishedParentProofs";

    // Registered by the batch source's enumerator (SpannerBatchReadSplitEnumerator).

    /**
     * Reads planned into partitions. One per job at most: a restored enumerator plans nothing, so
     * this reads {@code 1} on a fresh run and {@code 0} on a restored one.
     */
    public static final String READS_PLANNED = "readsPlanned";

    // Registered by the batch source's readers (SpannerSourceReaderMetrics).

    /** Input rows accepted from a partition into fetch batches. */
    public static final String ROWS_READ = "rowsRead";

    /**
     * Partitions opened again from their start after a wake-up cancelled them part-way, delivering
     * the rows they had already handed on a second time.
     */
    public static final String PARTITIONS_REREAD = "partitionsReread";

    // Registered by the Change Streams source readers (SpannerChangeStreamReaderMetrics).

    /** Change Streams TVF partition queries opened, including restored reopens. */
    public static final String CHANGE_STREAM_QUERIES_STARTED = "changeStreamQueriesStarted";

    /** Change Streams TVF partition queries currently open in one reader subtask. */
    public static final String ACTIVE_CHANGE_STREAM_QUERIES = "activeChangeStreamQueries";

    /** Assigned Change Streams partitions waiting for a query slot in one reader subtask. */
    public static final String QUEUED_CHANGE_STREAM_PARTITIONS = "queuedChangeStreamPartitions";

    /** Lag of the oldest assigned Change Streams partition waiting for a query slot. */
    public static final String QUEUED_CHANGE_STREAM_PARTITION_LAG_MILLIS =
            "queuedChangeStreamPartitionLagMillis";

    /** Maximum whole heartbeat intervals missed by any active non-initial partition query. */
    public static final String MISSED_HEARTBEAT_INTERVALS = "missedHeartbeatIntervals";

    /** Wait for the most recently returned non-heartbeat Change Streams result. */
    public static final String LAST_CHANGE_STREAM_RECORD_WAIT_MILLIS =
            "lastChangeStreamRecordWaitMillis";

    /** Longest wait for a returned non-heartbeat Change Streams result in this task attempt. */
    public static final String LONGEST_CHANGE_STREAM_RECORD_WAIT_MILLIS =
            "longestChangeStreamRecordWaitMillis";

    /** Data-change records removed by a Change Streams table filter. */
    public static final String CHANGE_STREAM_RECORDS_FILTERED_BY_TABLE =
            "changeStreamRecordsFilteredByTable";

    /** Data-change records skipped after column projection left no reported non-key values. */
    public static final String CHANGE_STREAM_RECORDS_SKIPPED_WITHOUT_CHANGE =
            "changeStreamRecordsSkippedWithoutChange";

    /** Column metadata and value occurrences removed from records delivered to the deserializer. */
    public static final String CHANGE_STREAM_COLUMN_OCCURRENCES_FILTERED =
            "changeStreamColumnOccurrencesFiltered";

    private SpannerMetricNames() {}
}
