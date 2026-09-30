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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The DDL schema of a {@code bigtable} table: which column is the row key, and which column family
 * and qualifier every other column addresses.
 *
 * <p>The model is the HBase connector's, so that a table definition moves between the two with its
 * schema intact:
 *
 * <p><b>Abbreviated, not compiled:</b> deployment-specific connector options are omitted.
 * <!-- javadoc-example partial="deployment-specific connector options" -->
 *
 * <pre>{@code
 * CREATE TABLE bt (
 *   rowkey STRING,
 *   cf1 ROW<qual1 STRING, qual2 BIGINT>,
 *   cf2 ROW<metric DOUBLE>,
 *   PRIMARY KEY (rowkey) NOT ENFORCED
 * ) WITH ('connector' = 'bigtable', ...)
 * }</pre>
 *
 * <p>Exactly one column is neither a {@code ROW} nor a {@code MAP}, and that column is the row key;
 * every {@code ROW} column is a column family whose nested field names are the qualifiers. Upstream
 * google/flink-connector-gcp's alternative — a {@code value.format} per family — was weighed and
 * declined on <a href="https://github.com/flink-gcp/flink-connector-gcp/issues/34">#34</a>: it
 * cannot give a single qualifier its own type, and it ties a family to a format.
 *
 * <p>A {@code MAP<STRING, V>} or {@code MAP<BYTES, V>} column is a column family too, declared
 * without its qualifiers: each entry is one qualifier and its latest cell, every value has the one
 * type {@code V}, and the family is read and written whole. This is how GoogleSQL for Bigtable
 * presents a family, and it is the form a schema derived from table metadata alone can take, since
 * Bigtable stores families but neither qualifiers nor cell types (ADR-0172). The HBase connector
 * has no such form, so a DDL using one does not move to it.
 *
 * <p>Column order is preserved, because it is what a projection's indexes refer to.
 */
@Internal
public final class BigtableTableSchema {

    private final int rowKeyIndex;
    private final String rowKeyName;
    private final LogicalType rowKeyType;
    private final List<Family> families;

    private BigtableTableSchema(
            int rowKeyIndex, String rowKeyName, LogicalType rowKeyType, List<Family> families) {
        this.rowKeyIndex = rowKeyIndex;
        this.rowKeyName = rowKeyName;
        this.rowKeyType = rowKeyType;
        this.families = Collections.unmodifiableList(families);
    }

    /**
     * Reads the model out of a table's physical row type.
     *
     * @param rowType the physical columns, in DDL order
     * @return the parsed schema
     * @throws ValidationException if the columns do not describe one row key and zero or more
     *     column families, or if a column's type, or a map's key or value type, has no cell
     *     encoding
     */
    public static BigtableTableSchema of(RowType rowType) {
        int rowKeyIndex = -1;
        List<Family> families = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String name = rowType.getFieldNames().get(i);
            LogicalType type = rowType.getTypeAt(i);
            if (type.is(LogicalTypeRoot.ROW)) {
                families.add(family(name, i, (RowType) type));
                continue;
            }
            if (type.is(LogicalTypeRoot.MAP)) {
                families.add(mapFamily(name, i, (MapType) type));
                continue;
            }
            if (rowKeyIndex >= 0) {
                throw new ValidationException(
                        String.format(
                                "Columns '%s' and '%s' are both atomic, so neither can be the row"
                                        + " key. A 'bigtable' table has exactly one atomic column,"
                                        + " which is the row key; every other column is a"
                                        + " ROW<...> naming one column family's qualifiers, or a"
                                        + " MAP<...> holding a whole column family.",
                                rowType.getFieldNames().get(rowKeyIndex), name));
            }
            CellValueCodec.checkSupported(name, type);
            rowKeyIndex = i;
        }
        if (rowKeyIndex < 0) {
            throw new ValidationException(
                    "A 'bigtable' table needs one atomic column to be its row key, and this one"
                            + " has none: every column is a ROW<...> or a MAP<...>, which is how a"
                            + " column family is declared.");
        }
        return new BigtableTableSchema(
                rowKeyIndex,
                rowType.getFieldNames().get(rowKeyIndex),
                rowType.getTypeAt(rowKeyIndex),
                families);
    }

    private static Family family(String name, int index, RowType familyType) {
        checkFamilyName(name);
        List<Qualifier> qualifiers = new ArrayList<>(familyType.getFieldCount());
        for (int i = 0; i < familyType.getFieldCount(); i++) {
            String qualifier = familyType.getFieldNames().get(i);
            LogicalType type = familyType.getTypeAt(i);
            if (type.is(LogicalTypeRoot.ROW)) {
                throw new ValidationException(
                        String.format(
                                "Column '%s.%s' is a ROW, but a column family's fields are"
                                        + " qualifiers holding one cell each, and a Bigtable cell"
                                        + " is a byte string. Nested rows have no encoding here.",
                                name, qualifier));
            }
            CellValueCodec.checkSupported(name + "." + qualifier, type);
            qualifiers.add(new Qualifier(qualifier, type));
        }
        return new Family(name, index, qualifiers, null);
    }

    private static Family mapFamily(String name, int index, MapType mapType) {
        checkFamilyName(name);
        LogicalType keyType = mapType.getKeyType();
        if (!keyType.is(LogicalTypeRoot.VARCHAR) && !keyType.is(LogicalTypeRoot.VARBINARY)) {
            // A key is a qualifier's bytes, which have no fixed length and no other layout: STRING
            // reads them as UTF-8, BYTES as they are. CHAR and BINARY would pad or truncate them.
            throw new ValidationException(
                    String.format(
                            "Column '%s' is a MAP with key type %s, but a column family's map key"
                                    + " is a qualifier, which is a STRING or BYTES of any length.",
                            name, keyType));
        }
        LogicalType valueType = mapType.getValueType();
        if (valueType.is(LogicalTypeRoot.ARRAY)
                && ((ArrayType) valueType).getElementType().is(LogicalTypeRoot.ROW)) {
            // GoogleSQL's with_history shape, MAP<key, ARRAY<STRUCT<timestamp, value>>>, is a
            // later form rather than a nested value (ADR-0172); say so rather than calling it
            // merely unencodable.
            throw new ValidationException(
                    String.format(
                            "Column '%s' is a MAP whose values are %s. A column family's map holds"
                                    + " the latest cell of each qualifier, one value per entry;"
                                    + " reading every version of a cell is not supported.",
                            name, valueType));
        }
        CellValueCodec.checkMapValueSupported(name, valueType);
        return new Family(name, index, Collections.emptyList(), mapType);
    }

    private static void checkFamilyName(String name) {
        if (name.indexOf(':') >= 0) {
            // Not a general Bigtable identifier check — the service's own message covers the rest.
            // This one is here because a colon survives escaping: a family filter is a
            // familyNameRegexFilter, which RE2 refuses a ':' in even when it is backslash-escaped,
            // so a projection over such a family would fail at read time rather than here.
            throw new ValidationException(
                    String.format(
                            "Column family '%s' contains ':', which Bigtable's family filter"
                                    + " cannot express. Rename the family.",
                            name));
        }
    }

    /** Returns the position of the row-key column in the physical row. */
    public int getRowKeyIndex() {
        return rowKeyIndex;
    }

    /** Returns the name of the row-key column. */
    public String getRowKeyName() {
        return rowKeyName;
    }

    /** Returns the declared type of the row-key column. */
    public LogicalType getRowKeyType() {
        return rowKeyType;
    }

    /** Returns the column families, in DDL order. */
    public List<Family> getFamilies() {
        return families;
    }

    /** Returns the physical row's field count: its row key and its column families. */
    public int getFieldCount() {
        return families.size() + 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableTableSchema that = (BigtableTableSchema) o;
        return rowKeyIndex == that.rowKeyIndex
                && rowKeyName.equals(that.rowKeyName)
                && rowKeyType.equals(that.rowKeyType)
                && families.equals(that.families);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKeyIndex, rowKeyName, rowKeyType, families);
    }

    /**
     * One column family: a {@code ROW} column whose nested fields are its qualifiers, or a {@code
     * MAP} column whose entries are.
     */
    @Internal
    public static final class Family {

        private final String name;
        private final int index;
        private final List<Qualifier> qualifiers;
        @Nullable private final MapType mapType;

        private Family(
                String name, int index, List<Qualifier> qualifiers, @Nullable MapType mapType) {
            this.name = name;
            this.index = index;
            this.qualifiers = Collections.unmodifiableList(qualifiers);
            this.mapType = mapType;
        }

        /** Returns the family name, which is the column name. */
        public String getName() {
            return name;
        }

        /** Returns the position of the family's column in the physical row. */
        public int getIndex() {
            return index;
        }

        /**
         * Returns the family's qualifiers, in DDL order; empty for a {@code MAP} family, whose
         * qualifiers are its entries' keys and so are known only per row.
         */
        public List<Qualifier> getQualifiers() {
            return qualifiers;
        }

        /** Returns whether the family is declared as a {@code MAP} rather than a {@code ROW}. */
        public boolean isMap() {
            return mapType != null;
        }

        /**
         * Returns the declared type of a {@code MAP} family's keys, which are its qualifiers.
         *
         * @throws IllegalStateException if the family is a {@code ROW}
         */
        public LogicalType getMapKeyType() {
            return mapType().getKeyType();
        }

        /**
         * Returns the declared type of a {@code MAP} family's values, which are its cells.
         *
         * @throws IllegalStateException if the family is a {@code ROW}
         */
        public LogicalType getMapValueType() {
            return mapType().getValueType();
        }

        private MapType mapType() {
            if (mapType == null) {
                throw new IllegalStateException("Column family '" + name + "' is not a MAP.");
            }
            return mapType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Family that = (Family) o;
            return index == that.index
                    && name.equals(that.name)
                    && qualifiers.equals(that.qualifiers)
                    && Objects.equals(mapType, that.mapType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, index, qualifiers, mapType);
        }
    }

    /** One qualifier: a nested field of a column family's {@code ROW}. */
    @Internal
    public static final class Qualifier {

        private final String name;
        private final LogicalType type;

        private Qualifier(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }

        /** Returns the qualifier, which is the nested field's name. */
        public String getName() {
            return name;
        }

        /** Returns the declared type of the cell. */
        public LogicalType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Qualifier that = (Qualifier) o;
            return name.equals(that.name) && type.equals(that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }
    }
}
