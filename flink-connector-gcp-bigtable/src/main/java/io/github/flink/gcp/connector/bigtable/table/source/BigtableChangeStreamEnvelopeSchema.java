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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

/** The exact physical schema of the generic Bigtable Change Streams mutation envelope. */
@Internal
public final class BigtableChangeStreamEnvelopeSchema {

    private static final DataType GENERIC_VALUE =
            DataTypes.ROW(
                    DataTypes.FIELD("value_type", DataTypes.STRING()),
                    DataTypes.FIELD("bytes_value", DataTypes.BYTES()),
                    DataTypes.FIELD("long_value", DataTypes.BIGINT()));

    private static final DataType DELETE_RANGE =
            DataTypes.ROW(
                    DataTypes.FIELD("start_bound", DataTypes.STRING()),
                    DataTypes.FIELD("start_micros", DataTypes.BIGINT()),
                    DataTypes.FIELD("end_bound", DataTypes.STRING()),
                    DataTypes.FIELD("end_micros", DataTypes.BIGINT()));

    /** The physical row type accepted by {@code scan.mode = change-stream}. */
    public static final DataType DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("row_key", DataTypes.BYTES()),
                    DataTypes.FIELD(
                            "entries",
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD("entry_index", DataTypes.INT()),
                                            DataTypes.FIELD("kind", DataTypes.STRING()),
                                            DataTypes.FIELD("family", DataTypes.STRING()),
                                            DataTypes.FIELD("qualifier", GENERIC_VALUE),
                                            DataTypes.FIELD("timestamp", GENERIC_VALUE),
                                            DataTypes.FIELD("value", GENERIC_VALUE),
                                            DataTypes.FIELD("delete_range", DELETE_RANGE)))));

    private BigtableChangeStreamEnvelopeSchema() {}

    /** Rejects every physical shape other than {@link #DATA_TYPE}. */
    public static void validate(DataType actual) {
        // Flink marks the top-level physical row NOT NULL even though DDL columns and every nested
        // field retain their declared nullability. The envelope contract starts at those columns,
        // so normalize only that planner-owned root bit before the exact comparison.
        if (!actual.getLogicalType().copy(true).equals(DATA_TYPE.getLogicalType())) {
            throw new ValidationException(
                    "A 'bigtable' Change Streams envelope source requires exactly this physical"
                            + " schema: "
                            + DATA_TYPE
                            + ". The declared physical schema was: "
                            + actual
                            + ".");
        }
    }
}
