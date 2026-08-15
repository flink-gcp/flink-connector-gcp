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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;

import io.github.flink.gcp.connector.bigquery.sink.cdc.DebeziumMySqlCdcSequenceNumberEncoder;
import io.github.flink.gcp.connector.bigquery.sink.cdc.DebeziumPostgreSqlCdcSequenceNumberEncoder;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/** Routes Debezium source properties to a connector-specific sequence encoder. */
@Internal
final class DebeziumCdcSequenceNumberResolver implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final StringData CONNECTOR_KEY = StringData.fromString("connector");
    private static final StringData SEQUENCE_KEY = StringData.fromString("sequence");
    private static final StringData LSN_KEY = StringData.fromString("lsn");
    private static final StringData SNAPSHOT_KEY = StringData.fromString("snapshot");
    private static final StringData GTID_KEY = StringData.fromString("gtid");
    private static final StringData POSITION_KEY = StringData.fromString("pos");
    private static final StringData ROW_KEY = StringData.fromString("row");

    @Nullable private final DebeziumMySqlCdcSequenceNumberEncoder mySqlEncoder;

    DebeziumCdcSequenceNumberResolver(List<String> mySqlSourceUuids) {
        this.mySqlEncoder =
                mySqlSourceUuids.isEmpty()
                        ? null
                        : new DebeziumMySqlCdcSequenceNumberEncoder(mySqlSourceUuids);
    }

    String sequenceNumber(MapData properties) {
        ArrayData keys = properties.keyArray();
        ArrayData values = properties.valueArray();
        String connector = null;
        String sequence = null;
        String lsn = null;
        String snapshot = null;
        String gtid = null;
        String position = null;
        String row = null;
        for (int i = 0; i < properties.size(); i++) {
            if (keys.isNullAt(i)) {
                continue;
            }
            StringData key = keys.getString(i);
            if (CONNECTOR_KEY.equals(key)) {
                connector = stringValue(values, i);
            } else if (SEQUENCE_KEY.equals(key)) {
                sequence = stringValue(values, i);
            } else if (LSN_KEY.equals(key)) {
                lsn = stringValue(values, i);
            } else if (SNAPSHOT_KEY.equals(key)) {
                snapshot = stringValue(values, i);
            } else if (GTID_KEY.equals(key)) {
                gtid = stringValue(values, i);
            } else if (POSITION_KEY.equals(key)) {
                position = stringValue(values, i);
            } else if (ROW_KEY.equals(key)) {
                row = stringValue(values, i);
            }
        }

        if (connector == null || connector.isEmpty()) {
            throw new IllegalArgumentException(
                    "The 'debezium-source-properties' metadata column must contain a non-empty"
                            + " 'connector' property");
        }
        switch (connector) {
            case "postgresql":
                return DebeziumPostgreSqlCdcSequenceNumberEncoder.sequenceNumber(
                        connector, sequence, lsn);
            case "mysql":
                if (mySqlEncoder == null) {
                    throw new IllegalArgumentException(
                            "Debezium MySQL sequence generation requires"
                                    + " sink.cdc.debezium-mysql.source-uuids");
                }
                return mySqlEncoder.sequenceNumber(connector, snapshot, gtid, position, row);
            default:
                throw new IllegalArgumentException(
                        "Debezium connector '"
                                + connector
                                + "' is not supported for BigQuery CDC sequence generation");
        }
    }

    private static String stringValue(ArrayData values, int position) {
        return values.isNullAt(position) ? null : values.getString(position).toString();
    }
}
