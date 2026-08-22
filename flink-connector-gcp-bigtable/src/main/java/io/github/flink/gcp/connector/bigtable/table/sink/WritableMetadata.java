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
 * its listing from {@code values()}. Being offered is not being applied: {@code
 * RowDataSerializationSchema} names {@link #TIMESTAMP} and nothing else, so a second constant needs
 * a read site written there. Finding its column is the one part that is already answered — {@link
 * #position(int, WritableMetadata[])} reads the position out of the selection rather than assuming
 * one, so the read site calls it instead of deriving arithmetic that can disagree with the
 * planner's layout. What that read site should <em>do</em> depends on the field: whether it applies
 * to every cell as the timestamp does or to the row as a whole, and whether it may be combined with
 * an explicit timestamp. That is why nothing further is prepared for it here.
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
     * Returns the index of this metadata's column in the consumed row, or {@code -1} when the
     * planner did not select it.
     *
     * <p>Flink builds the key list it hands to {@code applyWritableMetadata} and the metadata
     * suffix of the consumed row from one call each to the same function, ordered by {@link
     * #listAll()}'s iteration rather than by the DDL — so a position read out of the selection is
     * right whatever order the selection is in, where "the first column after the physical ones" is
     * right only while this enum has one constant.
     *
     * <p>Position is also the <em>only</em> correlation available: the consumed row's field carries
     * the column's name, which under {@code ts TIMESTAMP_LTZ(6) METADATA FROM 'timestamp'} is
     * {@code ts} and not {@code timestamp}, so matching {@link #getKey()} against the row type
     * would find nothing.
     */
    int position(int physicalArity, WritableMetadata[] metadata) {
        for (int i = 0; i < metadata.length; i++) {
            if (metadata[i] == this) {
                return physicalArity + i;
            }
        }
        return -1;
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

    /**
     * Returns the constant with the given metadata key.
     *
     * <p>No DDL reaches the throw: {@code DynamicSinkUtils.validateAndApplyMetadata} rejects a key
     * outside {@link #listAll()} first, and with a better message. A restored compiled plan does —
     * its {@code WritingMetadataSpec} applies the keys it was serialized with and no validation
     * runs — so a plan compiled against a build that offered a key this one does not is what the
     * throw is for. Naming the key beats writing the row without the column.
     */
    static WritableMetadata of(String key) {
        for (WritableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown Bigtable writable metadata key '" + key + "'.");
    }
}
