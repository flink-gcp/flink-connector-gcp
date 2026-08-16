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

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Derives BigQuery CDC sequence numbers from Debezium PostgreSQL source properties.
 *
 * <p>The source properties must contain {@code connector=postgresql} and Debezium's two-element
 * {@code sequence} JSON array. The provider encodes the nullable last committed LSN and required
 * current LSN as fixed-width unsigned hexadecimal sections. This transcodes Debezium's decimal
 * representation; it does not parse PostgreSQL's customary {@code X/Y} display form. When {@code
 * lsn} is non-null, it must identify the same current position.
 *
 * <p>Replaying the same Debezium event returns the same sequence. Sequence continuity across a
 * PostgreSQL primary failover requires Debezium to continue from the same preserved or synchronized
 * logical replication slot. Debezium can emit multiple events with the same LSN, so this provider
 * does not promise a unique total order for every event.
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype-pg-lsn.html">PostgreSQL {@code
 *     pg_lsn}</a>
 * @see <a
 *     href="https://debezium.io/documentation/reference/stable/connectors/postgresql.html">Debezium
 *     PostgreSQL source metadata</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/change-data-capture">BigQuery change data
 *     capture ordering</a>
 */
@PublicEvolving
public final class DebeziumPostgreSqlCdcSequenceNumberProvider
        implements CdcSequenceNumberProvider<Map<String, String>> {

    private static final long serialVersionUID = 1L;

    /** Creates a PostgreSQL source-properties provider. */
    public DebeziumPostgreSqlCdcSequenceNumberProvider() {}

    @Override
    public String getSequenceNumber(Map<String, String> sourceProperties) {
        requireNonNull(sourceProperties, "sourceProperties must not be null");
        return DebeziumPostgreSqlCdcSequenceNumberEncoder.sequenceNumber(
                sourceProperties.get("connector"),
                sourceProperties.get("sequence"),
                sourceProperties.get("lsn"));
    }
}
