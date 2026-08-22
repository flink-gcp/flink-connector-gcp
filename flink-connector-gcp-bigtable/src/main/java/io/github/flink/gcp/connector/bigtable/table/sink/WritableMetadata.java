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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The non-cell-value field a Bigtable table row may supply to its mutation.
 *
 * <p>A constant added here is offered to the planner on its own, because {@link #listAll()} derives
 * its listing from {@code values()}. Being offered is not enough to make it work: {@code
 * BigtableDynamicSink.applyWritableMetadata} reduces the planner's selection to one boolean, and
 * {@code RowDataSerializationSchema} reads the metadata column at the first position after the
 * physical columns, so a second constant needs both of those changed too.
 */
@Internal
enum WritableMetadata {

    /** One timestamp applied to every cell the row writes; {@code null} keeps the writer clock. */
    TIMESTAMP(
            "timestamp", DataTypes.TIMESTAMP_LTZ(WritableMetadata.TIMESTAMP_PRECISION).nullable());

    /**
     * The precision {@link #TIMESTAMP} is declared at, and the one the serializer must read it back
     * at: {@code RowData.getTimestamp} is told how many fractional digits to expect, and a reader
     * disagreeing with the declared type is not a mismatch anything reports.
     *
     * <p>Qualified above rather than named plainly because an enum constant precedes every static
     * field, and a simple-name forward reference to one does not compile.
     */
    static final int TIMESTAMP_PRECISION = 6;

    private final String key;
    private final DataType dataType;

    WritableMetadata(String key, DataType dataType) {
        this.key = key;
        this.dataType = dataType;
    }

    String getKey() {
        return key;
    }

    /**
     * Returns the metadata this connector can write, in declaration order.
     *
     * <p>Derived from {@code values()} so that a constant added to the enum is offered rather than
     * silently withheld.
     */
    static Map<String, DataType> listAll() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        for (WritableMetadata value : values()) {
            metadata.put(value.key, value.dataType);
        }
        return metadata;
    }
}
