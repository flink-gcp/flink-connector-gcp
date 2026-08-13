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

import java.util.LinkedHashMap;
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
                new RowDataCdcSequenceNumberProvider(WritableMetadata.CHANGE_SEQUENCE_NUMBER, 1);

        assertThat(provider.getSequenceNumber(row)).isEqualTo("0001/0000000000000002");
    }

    @Test
    void rejectsDebeziumMapsWithoutAProfileInsteadOfUsingATimestampFallback() {
        Map<StringData, StringData> properties = new LinkedHashMap<>();
        properties.put(StringData.fromString("connector"), StringData.fromString("postgresql"));
        properties.put(StringData.fromString("ts_ms"), StringData.fromString("1723593600000"));
        GenericRowData row = GenericRowData.of(new GenericMapData(properties));
        RowDataCdcSequenceNumberProvider provider =
                new RowDataCdcSequenceNumberProvider(
                        WritableMetadata.DEBEZIUM_SOURCE_PROPERTIES, 0);

        assertThatThrownBy(() -> provider.getSequenceNumber(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector 'postgresql' is not supported")
                .hasMessageNotContaining("ts_ms");
    }

    @Test
    void aNullSequenceMetadataValueRemainsNullForTheCdcEncoderToReject() {
        GenericRowData row = GenericRowData.of((Object) null);
        RowDataCdcSequenceNumberProvider provider =
                new RowDataCdcSequenceNumberProvider(WritableMetadata.CHANGE_SEQUENCE_NUMBER, 0);

        assertThat(provider.getSequenceNumber(row)).isNull();
    }

    private static CdcChangeType changeType(RowKind kind) {
        GenericRowData row = GenericRowData.of();
        row.setRowKind(kind);
        return RowDataCdcChangeTypeProvider.INSTANCE.getChangeType(row);
    }
}
