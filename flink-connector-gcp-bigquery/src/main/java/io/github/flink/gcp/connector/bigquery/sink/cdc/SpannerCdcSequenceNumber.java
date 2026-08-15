/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.PublicEvolving;

import java.time.Instant;

/**
 * Builds BigQuery CDC sequence numbers from typed Spanner change-stream coordinates.
 *
 * <p>A DataStream application reading Spanner change streams calls this from its own {@link
 * CdcSequenceNumberProvider}, passing the commit timestamp, record sequence, and mod number of the
 * mod it is writing:
 * <!-- javadoc-example file="JavadocBigQueryExamples.java" tag="spanner-cdc-sequence" -->
 *
 * <pre>{@code
 * CdcOptions.<SpannerChange>builder(
 *                 change ->
 *                         change.isDeletion()
 *                                 ? CdcChangeType.DELETE
 *                                 : CdcChangeType.UPSERT)
 *         .sequenceNumberProvider(
 *                 change ->
 *                         SpannerCdcSequenceNumber.of(
 *                                 change.commitTimestamp(),
 *                                 change.recordSequence(),
 *                                 change.modNumber()))
 *         .build();
 * }</pre>
 *
 * <p>The three coordinates become three fixed-width unsigned 64-bit hexadecimal sections, so an
 * equivalent record read through a Debezium Spanner envelope encodes to the same sequence through
 * {@link DebeziumSpannerCdcSequenceNumberProvider}. A record sequence is compared numerically,
 * which makes the zero-padded form Spanner emits and the unpadded form Debezium emits
 * interchangeable.
 *
 * <p>These sections order every record of one transaction, and order two transactions whose commit
 * timestamps differ. Spanner gives distinct commit timestamps to transactions that write
 * overlapping fields, so repeated changes to one set of columns are ordered. Two transactions that
 * write <em>disjoint</em> fields may share a commit timestamp, and a record sequence counts within
 * its own transaction, so such a pair can encode to one sequence and is then resolved by BigQuery's
 * ingestion order. Supply an application-provided total order through another {@link
 * CdcSequenceNumberProvider} when that pair must be ordered.
 *
 * @see <a href="https://cloud.google.com/spanner/docs/change-streams/details">Spanner change stream
 *     record contents</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/change-data-capture">BigQuery change data
 *     capture ordering</a>
 */
@PublicEvolving
public final class SpannerCdcSequenceNumber {

    private SpannerCdcSequenceNumber() {}

    /**
     * Encodes one Spanner mod as a BigQuery CDC sequence number.
     *
     * @param commitTimestamp the commit timestamp of the change record, not before the epoch
     * @param recordSequence the change record's sequence within its transaction and partition
     * @param modNumber the zero-based position of the mod within its change record
     */
    public static String of(Instant commitTimestamp, String recordSequence, int modNumber) {
        return SpannerCdcSequenceNumberEncoder.sequenceNumber(
                commitTimestamp, recordSequence, modNumber);
    }
}
