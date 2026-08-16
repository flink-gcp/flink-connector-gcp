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
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.bigquery.sink.cdc.SpannerCdcSequenceNumberEncoder;

/** Reads the typed Spanner change-sequence metadata row as a BigQuery CDC sequence number. */
@Internal
final class SpannerChangeSequenceResolver {

    private static final int COMMIT_TIMESTAMP_FIELD = 0;
    private static final int RECORD_SEQUENCE_FIELD = 1;
    private static final int MOD_NUMBER_FIELD = 2;

    /**
     * The arity and commit-timestamp precision come from the declared metadata type rather than
     * from literals, because {@code RowData} decodes a timestamp differently either side of the
     * compact-representation boundary and a silently mismatched precision reads the wrong bytes.
     */
    private static final RowType DECLARED_TYPE =
            (RowType) WritableMetadata.SPANNER_CHANGE_SEQUENCE.getDataType().getLogicalType();

    private static final int FIELD_COUNT = DECLARED_TYPE.getFieldCount();

    private static final int TIMESTAMP_PRECISION = commitTimestampPrecision(DECLARED_TYPE);

    private SpannerChangeSequenceResolver() {}

    static String sequenceNumber(RowData row, int position) {
        RowData changeSequence = row.getRow(position, FIELD_COUNT);
        requireField(changeSequence, COMMIT_TIMESTAMP_FIELD, "commit_timestamp");
        requireField(changeSequence, RECORD_SEQUENCE_FIELD, "record_sequence");
        requireField(changeSequence, MOD_NUMBER_FIELD, "mod_number");
        return SpannerCdcSequenceNumberEncoder.sequenceNumber(
                changeSequence
                        .getTimestamp(COMMIT_TIMESTAMP_FIELD, TIMESTAMP_PRECISION)
                        .toInstant(),
                changeSequence.getString(RECORD_SEQUENCE_FIELD).toString(),
                changeSequence.getInt(MOD_NUMBER_FIELD));
    }

    private static int commitTimestampPrecision(RowType declaredType) {
        LogicalType field = declaredType.getTypeAt(COMMIT_TIMESTAMP_FIELD);
        if (!(field instanceof LocalZonedTimestampType)) {
            throw new IllegalStateException(
                    "The 'spanner-change-sequence' commit timestamp must be a local-zoned timestamp"
                            + " but is "
                            + field);
        }
        return ((LocalZonedTimestampType) field).getPrecision();
    }

    private static void requireField(RowData changeSequence, int field, String name) {
        if (changeSequence.isNullAt(field)) {
            throw new IllegalArgumentException(
                    "The 'spanner-change-sequence' metadata column must contain a non-null '"
                            + name
                            + "' field");
        }
    }
}
