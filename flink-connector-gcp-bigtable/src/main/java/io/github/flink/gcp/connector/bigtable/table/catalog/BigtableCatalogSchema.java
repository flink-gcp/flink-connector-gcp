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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.bigtable.table.CatalogKeyType;

import java.util.Map;
import java.util.TreeMap;

/**
 * The Flink schema the {@code bigtable} catalog gives a table: a {@code _key} row key column that
 * is the primary key, then one {@code MAP} column per column family in name order (docs/adr/0178).
 *
 * <p>Bigtable's metadata names a table's families and their value types and nothing inside them, so
 * each family is a map from qualifier to its latest cell, the form ADR-0172 added for this catalog
 * and the one GoogleSQL for Bigtable presents. The value is {@code BIGINT} for an {@code int64}
 * sum, min or max aggregate family whose cells hold the eight big-endian bytes the codec's {@code
 * BIGINT} reads, and {@code BYTES} for every other family, since {@code BYTES} returns a cell's
 * stored bytes without decoding them. Every catalog map value is nullable, so an empty cell reads
 * as {@code NULL}, the connector's null convention for a nullable value.
 */
@Internal
final class BigtableCatalogSchema {

    /** The row key column's name, GoogleSQL for Bigtable's. */
    static final String ROW_KEY_COLUMN = "_key";

    private BigtableCatalogSchema() {}

    /**
     * Builds the schema of a table with the given families.
     *
     * @param families each family's value type, as the admin API reports it
     * @param keyType the type of the row key and of every map's key
     * @return the schema
     * @throws IllegalArgumentException if a family has the row key column's name
     */
    static Schema of(Map<String, Type> families, CatalogKeyType keyType) {
        if (families.containsKey(ROW_KEY_COLUMN)) {
            throw new IllegalArgumentException(
                    "its column family '"
                            + ROW_KEY_COLUMN
                            + "' has the name the catalog gives the row key column. Declare the"
                            + " table with CREATE TABLE and another name for the row key.");
        }
        Schema.Builder schema =
                Schema.newBuilder().column(ROW_KEY_COLUMN, keyType(keyType).notNull());
        for (Map.Entry<String, Type> family : new TreeMap<>(families).entrySet()) {
            schema.column(
                    family.getKey(), DataTypes.MAP(keyType(keyType), valueType(family.getValue())));
        }
        return schema.primaryKey(ROW_KEY_COLUMN).build();
    }

    private static DataType keyType(CatalogKeyType keyType) {
        return keyType == CatalogKeyType.STRING ? DataTypes.STRING() : DataTypes.BYTES();
    }

    /**
     * The Flink type that reads a family's cells: {@code BIGINT} where every cell is an eight-byte
     * big-endian integer, {@code BYTES} otherwise.
     *
     * <p>An aggregate family's cells hold its state. Only a sum, min or max family can be {@code
     * BIGINT}: its state is its input type, which the service also reports as the state type, and
     * both must be an {@code int64} in big-endian bytes. An {@code int64} with no encoding set is
     * big-endian, as the sink's family-type check reads it. An HLL family is {@code BYTES} whatever
     * state type it reports: its cells are sketches, and the admin API names {@code int64} only as
     * a conversion of the sketch to its count estimate, which a read of the cell does not make. An
     * {@code int64} in ordered-code bytes or in an encoding newer than the pinned protobuf, or an
     * aggregator or type this mapping does not know, reads as {@code BYTES}, which decodes nothing
     * and so cannot misread a non-empty cell.
     */
    static DataType valueType(Type type) {
        if (!type.hasAggregateType()) {
            return DataTypes.BYTES();
        }
        Type.Aggregate aggregate = type.getAggregateType();
        switch (aggregate.getAggregatorCase()) {
            case SUM:
            case MIN:
            case MAX:
                boolean bigint =
                        isBigEndianInt64(aggregate.getInputType())
                                && (!aggregate.hasStateType()
                                        || isBigEndianInt64(aggregate.getStateType()));
                return bigint ? DataTypes.BIGINT() : DataTypes.BYTES();
            default:
                return DataTypes.BYTES();
        }
    }

    /**
     * Whether the type is an {@code int64} in big-endian bytes. An unset encoding counts only when
     * nothing unknown was parsed in its place: the pinned protobuf reports an encoding newer than
     * itself as unset and keeps it among the unknown fields, which the sink's family-type check
     * preserves for the same reason.
     */
    private static boolean isBigEndianInt64(Type type) {
        if (!type.hasInt64Type()) {
            return false;
        }
        Type.Int64.Encoding encoding = type.getInt64Type().getEncoding();
        switch (encoding.getEncodingCase()) {
            case BIG_ENDIAN_BYTES:
                return true;
            case ENCODING_NOT_SET:
                return encoding.getUnknownFields().asMap().isEmpty();
            default:
                return false;
        }
    }
}
