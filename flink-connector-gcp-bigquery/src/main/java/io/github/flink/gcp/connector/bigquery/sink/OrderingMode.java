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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Experimental;

import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;

/**
 * Whether the {@link WriteMethod#STORAGE_API_AT_LEAST_ONCE} writer commits a table's append
 * requests in the order a sink subtask sent them, so that no row commits ahead of a row the subtask
 * received earlier and sent in an earlier request.
 *
 * <p>The order is observable to a reader that depends on the commit order: a query over {@code
 * APPENDS} that keeps the row with the latest {@code _CHANGE_TIMESTAMP} per key, a continuous query
 * that forwards rows as they commit, or a reader that resumes after the highest value it has seen
 * and never returns for a row that lands behind it. It does not decide the value a CDC table
 * settles on: every CDC row carries a {@code _CHANGE_SEQUENCE_NUMBER}, and BigQuery applies one
 * primary key's mutations by it (ADR-0193), except where two of a key's mutations carry equal
 * sequences, which BigQuery resolves by ingestion time.
 *
 * <p>BigQuery's CDC documentation states no order among the rows of one append request, and,
 * measured without a sequence, the first of a key's changes in one request won. Equal sequences
 * come from a source that can repeat one, or from Flink's {@code ChangelogNormalize}, which
 * completes a key-only delete from the last row it holds and so gives it the sequence of the change
 * it deletes. For a CDC write, this mode therefore also keeps one primary key out of a request
 * twice: the writer sends the pending request before a row whose key it already holds, and a key
 * changed in quick succession costs a request per change. The key is read from the bytes the
 * serializer wrote, and a key column written in an encoding with several spellings of one value,
 * such as a {@code TIMESTAMP} or {@code NUMERIC} written as text, makes every row of the table its
 * own request.
 *
 * <p>Each subtask orders only what it receives, so the order of rows that reach different subtasks
 * stays undefined whatever this mode says. A key's rows in different requests keep their order only
 * when every row of the key reaches the same subtask: a Table API changelog that Flink shuffles by
 * the sink's primary key, or a stream keyed before the sink.
 *
 * @see DefaultStreamOptions.Builder#orderingMode(OrderingMode)
 */
@Experimental
public enum OrderingMode {

    /**
     * No order between the append requests of one table (default). The writer keeps several
     * requests in flight, and re-sends a failed request on a rebuilt stream writer after later
     * requests may have committed, so a table's rows can commit out of the order they arrived.
     */
    NONE("none"),

    /**
     * Commits a table's append requests in the order the subtask sent them. The writer sends a
     * table's next append request only after the previous one has committed, or after its failed
     * rows have been re-sent or handed to the failure handler; different tables still have their
     * requests in flight together. A table therefore has one request in flight at a time, so its
     * throughput is bounded by one round trip per request of up to {@code maxAppendRequestBytes},
     * and an attempt that timed out on the client can still be committed by the service after a
     * later request. For a CDC write, a request also ends before a row whose primary key it already
     * holds.
     */
    PER_TABLE("per-table");

    private final String value;

    OrderingMode(String value) {
        this.value = value;
    }

    /** Returns the spelling a {@code WITH} clause uses. */
    @Override
    public String toString() {
        return value;
    }
}
