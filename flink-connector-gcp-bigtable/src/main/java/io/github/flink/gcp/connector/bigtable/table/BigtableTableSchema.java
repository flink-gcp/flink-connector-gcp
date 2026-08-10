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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

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
 * <pre>{@code
 * CREATE TABLE bt (
 *   rowkey STRING,
 *   cf1 ROW<qual1 STRING, qual2 BIGINT>,
 *   cf2 ROW<metric DOUBLE>,
 *   PRIMARY KEY (rowkey) NOT ENFORCED
 * ) WITH ('connector' = 'bigtable', ...)
 * }</pre>
 *
 * <p>Exactly one column is not a {@code ROW} and that column is the row key; every {@code ROW}
 * column is a column family whose nested field names are the qualifiers. Upstream
 * google/flink-connector-gcp's alternative — a {@code value.format} per family — was weighed and
 * declined on <a href="https://github.com/laughingman7743/flink-connector-gcp/issues/34">#34</a>:
 * it cannot give a single qualifier its own type, and it ties a family to a format.
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
     *     column families, or if a column's type has no cell encoding
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
            if (rowKeyIndex >= 0) {
                throw new ValidationException(
                        String.format(
                                "Columns '%s' and '%s' are both atomic, so neither can be the row"
                                        + " key. A 'bigtable' table has exactly one atomic column,"
                                        + " which is the row key; every other column is a"
                                        + " ROW<...> naming one column family's qualifiers.",
                                rowType.getFieldNames().get(rowKeyIndex), name));
            }
            CellValueCodec.checkSupported(name, type);
            rowKeyIndex = i;
        }
        if (rowKeyIndex < 0) {
            throw new ValidationException(
                    "A 'bigtable' table needs one atomic column to be its row key, and this one"
                            + " has none: every column is a ROW<...>, which is how a column family"
                            + " is declared.");
        }
        return new BigtableTableSchema(
                rowKeyIndex,
                rowType.getFieldNames().get(rowKeyIndex),
                rowType.getTypeAt(rowKeyIndex),
                families);
    }

    private static Family family(String name, int index, RowType familyType) {
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
        return new Family(name, index, qualifiers);
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

    /** One column family: a {@code ROW} column whose nested fields are its qualifiers. */
    @Internal
    public static final class Family {

        private final String name;
        private final int index;
        private final List<Qualifier> qualifiers;

        private Family(String name, int index, List<Qualifier> qualifiers) {
            this.name = name;
            this.index = index;
            this.qualifiers = Collections.unmodifiableList(qualifiers);
        }

        /** Returns the family name, which is the column name. */
        public String getName() {
            return name;
        }

        /** Returns the position of the family's column in the physical row. */
        public int getIndex() {
            return index;
        }

        /** Returns the family's qualifiers, in DDL order. */
        public List<Qualifier> getQualifiers() {
            return qualifiers;
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
                    && qualifiers.equals(that.qualifiers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, index, qualifiers);
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
