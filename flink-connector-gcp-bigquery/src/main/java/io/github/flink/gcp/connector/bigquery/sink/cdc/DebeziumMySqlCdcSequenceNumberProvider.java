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

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Derives BigQuery CDC sequence numbers from Debezium MySQL source properties.
 *
 * <p>The configured source UUIDs define one-based epochs in causal order. Existing entries are an
 * immutable ordering contract: a failover appends its new SID, while editing, removing or
 * reordering an earlier entry can make a replay compare differently. Initial snapshot rows use
 * epoch zero and are safe only when the destination is empty before that snapshot starts.
 *
 * <p>Streaming records must contain {@code connector=mysql}, an untagged {@code
 * UUID:transaction_id} GTID, {@code pos}, and {@code row}. The provider encodes the SID epoch,
 * transaction ID, binlog event position, and row within that event as four fixed-width unsigned
 * hexadecimal sections. Tagged GTIDs and interleaved multi-source histories are not supported.
 * MySQL Group Replication primary changes and multi-primary histories are also unsupported because
 * its fixed group UUID does not identify the member-specific GTID assignment block. This profile
 * can support them only if MySQL and Debezium expose a durable group-wide ordering coordinate in
 * source metadata.
 *
 * @see <a href="https://debezium.io/documentation/reference/stable/connectors/mysql.html">Debezium
 *     MySQL source metadata</a>
 * @see <a href="https://dev.mysql.com/doc/refman/8.4/en/group-replication-gtids.html">MySQL Group
 *     Replication GTID assignment</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/change-data-capture">BigQuery change data
 *     capture ordering</a>
 */
@PublicEvolving
public final class DebeziumMySqlCdcSequenceNumberProvider
        implements CdcSequenceNumberProvider<Map<String, String>> {

    private static final long serialVersionUID = 1L;

    private final DebeziumMySqlCdcSequenceNumberEncoder encoder;

    /** Creates a provider with source UUIDs listed in causal order. */
    public DebeziumMySqlCdcSequenceNumberProvider(List<String> sourceUuids) {
        this.encoder = new DebeziumMySqlCdcSequenceNumberEncoder(sourceUuids);
    }

    @Override
    public String getSequenceNumber(Map<String, String> sourceProperties) {
        requireNonNull(sourceProperties, "sourceProperties must not be null");
        return encoder.sequenceNumber(
                sourceProperties.get("connector"),
                sourceProperties.get("snapshot"),
                sourceProperties.get("gtid"),
                sourceProperties.get("pos"),
                sourceProperties.get("row"));
    }
}
