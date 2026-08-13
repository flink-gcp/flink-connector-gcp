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

/** Routes Debezium source properties to a connector-specific sequence encoder. */
@Internal
final class DebeziumCdcSequenceNumberProvider {

    private static final StringData CONNECTOR_KEY = StringData.fromString("connector");

    private DebeziumCdcSequenceNumberProvider() {}

    static String sequenceNumber(MapData properties) {
        String connector = property(properties, CONNECTOR_KEY);
        if (connector == null || connector.isEmpty()) {
            throw new IllegalArgumentException(
                    "The 'debezium-source-properties' metadata column must contain a non-empty"
                            + " 'connector' property");
        }

        // Connector-specific ordering fields are deliberately added by #629, #631 and #633.
        // Falling back to source.ts_ms here would collapse distinct changes from the same
        // millisecond and let BigQuery resolve them by ingestion order instead.
        throw new IllegalArgumentException(
                "Debezium connector '"
                        + connector
                        + "' is not supported for BigQuery CDC sequence generation");
    }

    private static String property(MapData properties, StringData wanted) {
        ArrayData keys = properties.keyArray();
        ArrayData values = properties.valueArray();
        for (int i = 0; i < properties.size(); i++) {
            if (keys.isNullAt(i)) {
                continue;
            }
            StringData key = keys.getString(i);
            if (wanted.equals(key)) {
                return values.isNullAt(i) ? null : values.getString(i).toString();
            }
        }
        return null;
    }
}
