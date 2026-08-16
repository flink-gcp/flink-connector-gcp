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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;

/** Maps a Flink table changelog onto BigQuery CDC mutation types. */
@Internal
enum RowDataCdcChangeTypeProvider implements CdcChangeTypeProvider<RowData> {
    INSTANCE;

    @Override
    public CdcChangeType getChangeType(RowData row) {
        RowKind kind = row.getRowKind();
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                return CdcChangeType.UPSERT;
            case DELETE:
                return CdcChangeType.DELETE;
            case UPDATE_BEFORE:
                throw new IllegalArgumentException(
                        "UPDATE_BEFORE is not part of the BigQuery CDC sink changelog");
            default:
                throw new IllegalArgumentException("Unsupported row kind " + kind);
        }
    }
}
