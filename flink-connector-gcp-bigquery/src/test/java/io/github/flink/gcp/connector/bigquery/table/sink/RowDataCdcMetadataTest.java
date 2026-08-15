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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Table API adapters for BigQuery CDC metadata. */
class RowDataCdcMetadataTest {

    @Test
    void mapsTheFlinkUpsertChangelogOntoBigQueryMutations() {
        assertThat(changeType(RowKind.INSERT)).isEqualTo(CdcChangeType.UPSERT);
        assertThat(changeType(RowKind.UPDATE_AFTER)).isEqualTo(CdcChangeType.UPSERT);
        assertThat(changeType(RowKind.DELETE)).isEqualTo(CdcChangeType.DELETE);
        assertThatThrownBy(() -> changeType(RowKind.UPDATE_BEFORE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UPDATE_BEFORE is not part of the BigQuery CDC sink");
    }

    @Test
    void readsAnAlreadyFormattedSequenceFromThePlannerAppendedColumn() {
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("physical"),
                        StringData.fromString("0001/0000000000000002"));
        RowDataCdcSequenceNumberProvider provider =
                new RowDataCdcSequenceNumberProvider(
                        WritableMetadata.CHANGE_SEQUENCE_NUMBER,
                        1,
                        resolver(Collections.emptyList(), null));

        assertThat(provider.getSequenceNumber(row)).isEqualTo("0001/0000000000000002");
    }

    @Test
    void forwardsDebeziumSourcePropertiesToThePostgreSqlEncoder() {
        assertThat(
                        debeziumSequence(
                                properties(
                                        "connector",
                                        "postgresql",
                                        "sequence",
                                        "[\"16\",\"17\"]",
                                        "lsn",
                                        "17")))
                .isEqualTo("0000000000000010/0000000000000011");
    }

    @Test
    void forwardsAContradictoryDebeziumLsnToThePostgreSqlEncoder() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties(
                                                "connector",
                                                "postgresql",
                                                "sequence",
                                                "[\"16\",\"17\"]",
                                                "lsn",
                                                "18")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'lsn' does not match");
    }

    @Test
    void routesDebeziumSourcePropertiesByConnectorName() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties(
                                                "connector",
                                                "mysql",
                                                "sequence",
                                                "[\"16\",\"17\"]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires sink.cdc.debezium-mysql.source-uuids");
    }

    @Test
    void forwardsDebeziumSourcePropertiesToTheMySqlEncoder() {
        String sid = "24bc7850-2c16-11e6-a073-0242ac110002";
        assertThat(
                        debeziumSequence(
                                properties(
                                        "connector",
                                        "mysql",
                                        "snapshot",
                                        "false",
                                        "gtid",
                                        sid + ":16",
                                        "pos",
                                        "1081",
                                        "row",
                                        "2"),
                                Collections.singletonList(sid)))
                .isEqualTo("0000000000000001/0000000000000010/0000000000000439/0000000000000002");
    }

    @Test
    void forwardsMySqlSnapshotStateWithoutRequiringStreamingCoordinates() {
        String sid = "24bc7850-2c16-11e6-a073-0242ac110002";
        assertThat(
                        debeziumSequence(
                                properties("connector", "mysql", "snapshot", "true"),
                                Collections.singletonList(sid)))
                .isEqualTo("0000000000000000/0000000000000000/0000000000000000/0000000000000000");
    }

    @Test
    void forwardsDebeziumSourcePropertiesToTheTiCdcEncoder() {
        assertThat(
                        debeziumSequence(
                                properties(
                                        "connector",
                                        "TiCDC",
                                        "snapshot",
                                        "false",
                                        "commit_ts",
                                        "449574614268182531",
                                        "cluster_id",
                                        "test_cluster",
                                        "ts_ms",
                                        "0"),
                                "test_cluster"))
                .isEqualTo("063D35BACF7D0003");
    }

    @Test
    void rejectsATiCdcEventFromAnotherClusterThanTheConfiguredOne() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties(
                                                "connector",
                                                "TiCDC",
                                                "commit_ts",
                                                "449574614268182531",
                                                "cluster_id",
                                                "other_cluster"),
                                        "test_cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("other_cluster");
    }

    @Test
    void forwardsTiCdcSnapshotStateToTheEncoderThatRejectsIt() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties(
                                                "connector",
                                                "TiCDC",
                                                "snapshot",
                                                "true",
                                                "commit_ts",
                                                "449574614268182531",
                                                "cluster_id",
                                                "test_cluster"),
                                        "test_cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'snapshot'");
    }

    /** A TiCDC release before v8.0.0 sends neither field, which must not order by anything else. */
    @Test
    void rejectsATiCdcEnvelopeWithoutTheTiDbSourceFields() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties("connector", "TiCDC", "ts_ms", "1714991051743"),
                                        "test_cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty 'cluster_id'");
    }

    @Test
    void rejectsATiCdcEnvelopeWhoseCommitTsoTheMapDoesNotCarry() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties(
                                                "connector",
                                                "TiCDC",
                                                "cluster_id",
                                                "test_cluster",
                                                "ts_ms",
                                                "1714991051743"),
                                        "test_cluster"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'commit_ts'");
    }

    @Test
    void requiresAConfiguredClusterIdBeforeOrderingTiCdcEvents() {
        assertThatThrownBy(
                        () ->
                                debeziumSequence(
                                        properties(
                                                "connector",
                                                "TiCDC",
                                                "commit_ts",
                                                "449574614268182531",
                                                "cluster_id",
                                                "test_cluster")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires sink.cdc.ticdc.cluster-id");
    }

    @Test
    void aNullSequenceMetadataValueRemainsNullForTheCdcEncoderToReject() {
        GenericRowData row = GenericRowData.of((Object) null);
        RowDataCdcSequenceNumberProvider provider =
                new RowDataCdcSequenceNumberProvider(
                        WritableMetadata.CHANGE_SEQUENCE_NUMBER,
                        0,
                        resolver(Collections.emptyList(), null));

        assertThat(provider.getSequenceNumber(row)).isNull();
    }

    private static CdcChangeType changeType(RowKind kind) {
        GenericRowData row = GenericRowData.of();
        row.setRowKind(kind);
        return RowDataCdcChangeTypeProvider.INSTANCE.getChangeType(row);
    }

    private static String debeziumSequence(Map<StringData, StringData> properties) {
        return debeziumSequence(properties, Collections.emptyList());
    }

    private static String debeziumSequence(
            Map<StringData, StringData> properties, List<String> mysqlSourceUuids) {
        return debeziumSequence(properties, mysqlSourceUuids, null);
    }

    private static String debeziumSequence(
            Map<StringData, StringData> properties, String tiCdcClusterId) {
        return debeziumSequence(properties, Collections.emptyList(), tiCdcClusterId);
    }

    private static String debeziumSequence(
            Map<StringData, StringData> properties,
            List<String> mysqlSourceUuids,
            String tiCdcClusterId) {
        GenericRowData row = GenericRowData.of(new GenericMapData(properties));
        RowDataCdcSequenceNumberProvider provider =
                new RowDataCdcSequenceNumberProvider(
                        WritableMetadata.DEBEZIUM_SOURCE_PROPERTIES,
                        0,
                        resolver(mysqlSourceUuids, tiCdcClusterId));
        return provider.getSequenceNumber(row);
    }

    private static DebeziumCdcSequenceNumberResolver resolver(
            List<String> mysqlSourceUuids, String tiCdcClusterId) {
        return new DebeziumCdcSequenceNumberResolver(mysqlSourceUuids, tiCdcClusterId);
    }

    private static Map<StringData, StringData> properties(String... entries) {
        Map<StringData, StringData> properties = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            properties.put(
                    StringData.fromString(entries[i]),
                    entries[i + 1] == null ? null : StringData.fromString(entries[i + 1]));
        }
        return properties;
    }
}
