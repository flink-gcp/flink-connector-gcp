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
 * Derives BigQuery CDC sequence numbers from TiCDC's Debezium-protocol source properties.
 *
 * <p>The source properties must contain {@code connector=TiCDC}, the numeric {@code commit_ts}, and
 * the {@code cluster_id} matching the configured TiCDC cluster. TiCDC emits both fields from v8.0.0
 * onwards. The provider encodes the commit timestamp oracle value as one fixed-width unsigned
 * hexadecimal section. It never substitutes the source object's {@code ts_ms}, which truncates that
 * timestamp oracle value to milliseconds and so cannot order two transactions committed within one
 * millisecond.
 *
 * <p>The commit TSO orders every transaction within one TiDB cluster and is read from the change
 * log rather than assigned by the process encoding it, so it survives a TiCDC process or node
 * failover and this profile needs no failover epoch. Rejecting an event whose {@code cluster_id}
 * differs from the configured one keeps two changefeeds from sharing one ordering domain, where
 * independent oracles would interleave. That identifier is TiCDC's own cluster ID, which defaults
 * to {@code default}, so a deployment routing several TiCDC clusters into one table has to give
 * each its own. Replaying an event returns the same sequence.
 *
 * <p>Row changes of one transaction share its commit TSO and therefore its sequence, so the commit
 * TSO is not a unique total order over events. TiDB writes each key at most once per transaction,
 * but TiCDC splits an UPDATE that modifies a primary or unique key into a DELETE and an INSERT. A
 * transaction that moves one key's value onto another key, such as a primary-key swap, therefore
 * emits both a DELETE and an INSERT for one BigQuery primary key at one sequence, and BigQuery
 * resolves that pair by ingestion time rather than by TiCDC's emission order. Applications that
 * admit such transactions must supply their own tie-breaker through the formatted {@code
 * change-sequence-number} route or a custom {@link CdcSequenceNumberProvider}.
 *
 * <p>This profile covers row-change events only. TiCDC's classic architecture emits nothing else in
 * this protocol; its new architecture, which is the only one from TiDB v9.0.0, adds DDL events and,
 * under {@code enable-tidb-extension}, watermark events. Neither carries a row to write, and the
 * protocol provides no initial snapshot stream.
 *
 * @see <a href="https://docs.pingcap.com/tidb/stable/ticdc-debezium/">TiCDC Debezium protocol</a>
 * @see <a href="https://docs.pingcap.com/tidb/stable/ticdc-split-update-behavior/">TiCDC UPDATE
 *     event splitting</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/change-data-capture">BigQuery change data
 *     capture ordering</a>
 */
@PublicEvolving
public final class TiCdcSequenceNumberProvider
        implements CdcSequenceNumberProvider<Map<String, String>> {

    private static final long serialVersionUID = 1L;

    private final TiCdcSequenceNumberEncoder encoder;

    /** Creates a provider ordering the changefeeds of one TiCDC cluster ID. */
    public TiCdcSequenceNumberProvider(String clusterId) {
        this.encoder = new TiCdcSequenceNumberEncoder(clusterId);
    }

    @Override
    public String getSequenceNumber(Map<String, String> sourceProperties) {
        requireNonNull(sourceProperties, "sourceProperties must not be null");
        return encoder.sequenceNumber(
                sourceProperties.get("connector"),
                sourceProperties.get("snapshot"),
                sourceProperties.get("commit_ts"),
                sourceProperties.get("cluster_id"));
    }
}
