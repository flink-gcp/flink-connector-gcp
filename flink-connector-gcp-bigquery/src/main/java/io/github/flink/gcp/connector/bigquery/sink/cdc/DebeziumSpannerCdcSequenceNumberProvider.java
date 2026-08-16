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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.Public;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Derives BigQuery CDC sequence numbers from Debezium Spanner source properties.
 *
 * <p>The source properties must contain {@code connector=spanner} together with {@code ts_ns},
 * {@code sequence}, and {@code mod_number}. The provider encodes the Spanner commit timestamp in
 * nanoseconds, the record sequence within the transaction, and the mod number within the record as
 * three fixed-width unsigned 64-bit hexadecimal sections.
 *
 * <p>{@code source.ts_ns} is Debezium's rendering of the Spanner commit timestamp. Debezium also
 * writes {@code ts_ns} beside {@code source} in the event payload, where it means the connector's
 * own processing time; a source-properties map never carries that sibling.
 *
 * <p>Replaying the same change record returns the same sequence, and {@link
 * SpannerCdcSequenceNumber} returns the same sequence for the equivalent record read through the
 * native Spanner change-stream source. The three sections order every record of one transaction,
 * and order two transactions whose commit timestamps differ. Two transactions that write disjoint
 * fields may share a commit timestamp, and a record sequence counts within its own transaction, so
 * such a pair can encode to one sequence that BigQuery then resolves by ingestion order.
 *
 * @see <a href="https://cloud.google.com/spanner/docs/change-streams/details">Spanner change stream
 *     record contents</a>
 * @see <a
 *     href="https://debezium.io/documentation/reference/stable/connectors/spanner.html">Debezium
 *     Spanner source metadata</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/change-data-capture">BigQuery change data
 *     capture ordering</a>
 */
@Public
public final class DebeziumSpannerCdcSequenceNumberProvider
        implements CdcSequenceNumberProvider<Map<String, String>> {

    private static final long serialVersionUID = 1L;

    /** Creates a Spanner source-properties provider. */
    public DebeziumSpannerCdcSequenceNumberProvider() {}

    @Override
    public String getSequenceNumber(Map<String, String> sourceProperties) {
        requireNonNull(sourceProperties, "sourceProperties must not be null");
        return SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                sourceProperties.get("connector"),
                sourceProperties.get("ts_ns"),
                sourceProperties.get("sequence"),
                sourceProperties.get("mod_number"));
    }
}
