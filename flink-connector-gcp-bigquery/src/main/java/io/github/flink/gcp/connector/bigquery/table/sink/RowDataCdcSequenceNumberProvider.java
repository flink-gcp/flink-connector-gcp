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
import org.apache.flink.table.data.RowData;

import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcSequenceNumberProvider;

/** Reads one selected writable metadata column as a BigQuery CDC sequence number. */
@Internal
final class RowDataCdcSequenceNumberProvider implements CdcSequenceNumberProvider<RowData> {

    private static final long serialVersionUID = 1L;

    private final WritableMetadata source;
    private final int position;
    private final DebeziumCdcSequenceNumberResolver debeziumResolver;

    RowDataCdcSequenceNumberProvider(
            WritableMetadata source, int position, DebeziumCdcSequenceNumberResolver resolver) {
        this.source = source;
        this.position = position;
        this.debeziumResolver = resolver;
    }

    @Override
    public String getSequenceNumber(RowData row) {
        if (row.isNullAt(position)) {
            return null;
        }
        switch (source) {
            case CHANGE_SEQUENCE_NUMBER:
                return row.getString(position).toString();
            case DEBEZIUM_SOURCE_PROPERTIES:
                return debeziumResolver.sequenceNumber(row.getMap(position));
            default:
                throw new AssertionError("Unhandled writable metadata source " + source);
        }
    }
}
