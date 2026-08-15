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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TiCdcSequenceNumberProviderTest {

    private final TiCdcSequenceNumberProvider provider =
            new TiCdcSequenceNumberProvider("test_cluster");

    @Test
    void encodesTheSameStrictTiCdcSourcePropertiesUsedByTheTableProfile() {
        assertThat(provider.getSequenceNumber(sourceProperties("449574614268182531")))
                .isEqualTo("063D35BACF7D0003");
    }

    @Test
    void readsTheCommitTsoRatherThanTheMessageTimestamp() {
        Map<String, String> properties = sourceProperties("449574614268182531");
        properties.put("ts_ms", "1714991051743");

        assertThat(provider.getSequenceNumber(properties)).isEqualTo("063D35BACF7D0003");
    }

    /**
     * The documented primary-key swap: one transaction whose split UPDATEs delete and rewrite the
     * same keys. Every row of it carries one commit TSO, so the profile cannot order the DELETE
     * against the INSERT that share a BigQuery primary key.
     */
    @Test
    void everyRowOfOneTransactionSharesItsSequenceUntilALaterTransactionSupersedesIt() {
        Map<String, String> deleteOfKeyOne = rowEvent("orders", "449574614268182531", 0);
        Map<String, String> insertOfKeyOne = rowEvent("orders", "449574614268182531", 3);
        Map<String, String> otherTable = rowEvent("audit", "449574614268182531", 0);
        Map<String, String> laterTransaction = rowEvent("orders", "449574614268182532", 0);

        assertThat(provider.getSequenceNumber(deleteOfKeyOne))
                .isEqualTo(provider.getSequenceNumber(insertOfKeyOne))
                .isEqualTo(provider.getSequenceNumber(otherTable))
                .isEqualTo("063D35BACF7D0003");
        assertThat(provider.getSequenceNumber(laterTransaction))
                .isGreaterThan(provider.getSequenceNumber(deleteOfKeyOne));
    }

    @Test
    void rejectsATiCdcSnapshotStateTheProtocolNeverEmits() {
        Map<String, String> properties = sourceProperties("449574614268182531");
        properties.put("snapshot", "true");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'snapshot'");
    }

    @Test
    void rejectsACommitTsoShapeThatTheTableProfileRejects() {
        Map<String, String> properties = sourceProperties("not-a-tso");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'commit_ts'");
    }

    @Test
    void rejectsAnEventFromAnotherCluster() {
        Map<String, String> properties = sourceProperties("449574614268182531");
        properties.put("cluster_id", "other_cluster");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("other_cluster");
    }

    @Test
    void rejectsAbsentSourceProperties() {
        assertThatThrownBy(() -> provider.getSequenceNumber(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sourceProperties");
    }

    /** One row event's complete source object, as TiCDC documents it. */
    private static Map<String, String> rowEvent(String table, String commitTs, int row) {
        Map<String, String> properties = sourceProperties(commitTs);
        properties.put("version", "2.4.0.Final");
        properties.put("name", "test_cluster");
        properties.put("ts_ms", "0");
        properties.put("db", "test");
        properties.put("table", table);
        properties.put("server_id", "0");
        properties.put("file", "");
        properties.put("pos", "0");
        properties.put("row", String.valueOf(row));
        properties.put("thread", "0");
        return properties;
    }

    private static Map<String, String> sourceProperties(String commitTs) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "TiCDC");
        properties.put("snapshot", "false");
        properties.put("commit_ts", commitTs);
        properties.put("cluster_id", "test_cluster");
        return properties;
    }
}
