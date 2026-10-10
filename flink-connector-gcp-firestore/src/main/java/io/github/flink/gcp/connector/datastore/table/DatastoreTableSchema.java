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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * A {@code datastore} table's physical schema, checked against what a Datastore entity can hold:
 * which column is the key, and what Datastore value each column and nested field is.
 *
 * <p>The PRIMARY KEY column, when there is one, is the key's name (STRING) or its numeric id
 * (BIGINT) under the table's kind; it is never stored as a property. Every other column is a
 * property, a ROW an embedded entity whose fields are its properties.
 */
@Internal
public final class DatastoreTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The longest kind or property name Datastore stores, in UTF-8 bytes. */
    static final int MAX_NAME_BYTES = 1500;

    private final RowType rowType;
    private final int keyIndex;
    private final Set<String> unindexedColumns;

    private DatastoreTableSchema(RowType rowType, int keyIndex, Set<String> unindexedColumns) {
        this.rowType = rowType;
        this.keyIndex = keyIndex;
        this.unindexedColumns = unindexedColumns;
    }

    /**
     * Checks a table's physical row type, its primary key and its unindexed columns.
     *
     * @param rowType the physical row type
     * @param primaryKeyIndexes the declared primary key's column indexes, empty when none
     * @param unindexedColumns the {@code sink.unindexed-columns} value
     * @return the schema
     * @throws ValidationException if a column has no Datastore form, the key is not one STRING or
     *     BIGINT column, or an unindexed column is not a property of the table
     */
    public static DatastoreTableSchema of(
            RowType rowType, int[] primaryKeyIndexes, List<String> unindexedColumns) {
        int keyIndex = keyIndex(rowType, primaryKeyIndexes);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i == keyIndex) {
                continue;
            }
            String name = rowType.getFieldNames().get(i);
            checkPropertyName(name, name);
            check(name, rowType.getTypeAt(i));
        }
        Set<String> unindexed = new LinkedHashSet<>(unindexedColumns);
        if (keyIndex >= 0 && unindexed.contains(rowType.getFieldNames().get(keyIndex))) {
            throw new ValidationException(
                    DatastoreConnectorOptions.SINK_UNINDEXED_COLUMNS.key()
                            + " names the PRIMARY KEY column '"
                            + rowType.getFieldNames().get(keyIndex)
                            + "', which is the entity's key and never stored as a property.");
        }
        Set<String> unknown = new LinkedHashSet<>(unindexed);
        unknown.removeAll(rowType.getFieldNames());
        if (!unknown.isEmpty()) {
            throw new ValidationException(
                    DatastoreConnectorOptions.SINK_UNINDEXED_COLUMNS.key()
                            + " names columns the table does not declare: "
                            + quoted(unknown)
                            + ". It names top-level columns; an unindexed ROW leaves every"
                            + " value nested in it unindexed.");
        }
        return new DatastoreTableSchema(rowType, keyIndex, unindexed);
    }

    private static int keyIndex(RowType rowType, int[] primaryKeyIndexes) {
        if (primaryKeyIndexes.length == 0) {
            return -1;
        }
        if (primaryKeyIndexes.length > 1) {
            throw new ValidationException(
                    "A datastore table's PRIMARY KEY is the entity key's name or id, one STRING or"
                            + " BIGINT column, but this one has "
                            + primaryKeyIndexes.length
                            + " columns.");
        }
        int index = primaryKeyIndexes[0];
        LogicalType type = rowType.getTypeAt(index);
        if (!type.isAnyOf(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.BIGINT)) {
            throw new ValidationException(
                    "The PRIMARY KEY column '"
                            + rowType.getFieldNames().get(index)
                            + "' is the entity key's name or id and must be STRING or BIGINT,"
                            + " but it is "
                            + type.asSummaryString()
                            + ".");
        }
        return index;
    }

    private static void check(String path, LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BIGINT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return;
            case ARRAY:
                LogicalType element = ((ArrayType) type).getElementType();
                if (element.is(LogicalTypeRoot.ARRAY)) {
                    throw unsupported(
                            path, type, "Datastore does not store an array directly in an array.");
                }
                check(path, element);
                return;
            case ROW:
                for (RowType.RowField field : ((RowType) type).getFields()) {
                    checkPropertyName(path + "." + field.getName(), field.getName());
                    check(path + "." + field.getName(), field.getType());
                }
                return;
            default:
                throw unsupported(
                        path,
                        type,
                        "A datastore table holds STRING, BIGINT, DOUBLE, BOOLEAN, BYTES,"
                                + " TIMESTAMP_LTZ, ARRAY and ROW; write an INT as a BIGINT and a"
                                + " FLOAT as a DOUBLE.");
        }
    }

    /**
     * Returns whether {@code name} has the form {@code __…__}, at least one character between the
     * underscores, which the emulator refuses as a kind, a namespace, a key name or a property name
     * on every write. {@code entity.proto} reserves {@code __.*__}, which also matches {@code
     * ____}, and Datastore's documentation every kind beginning with {@code __}; the emulator
     * stores both, so they are left to the service.
     */
    static boolean isReserved(String name) {
        return name.length() > 4 && name.startsWith("__") && name.endsWith("__");
    }

    /** Returns whether {@code name} is longer than {@value #MAX_NAME_BYTES} UTF-8 bytes. */
    static boolean isTooLong(String name) {
        return name.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES;
    }

    /**
     * Refuses a property name Datastore refuses for every entity the table writes, so the name is
     * checked once here rather than failing each record (ADR-0127): a reserved one, and one longer
     * than {@value #MAX_NAME_BYTES} bytes.
     */
    private static void checkPropertyName(String path, String name) {
        if (isReserved(name)) {
            throw new ValidationException(
                    "Field '"
                            + path
                            + "' has a name Datastore reserves: a property name of the form"
                            + " '__…__', with at least one character between the underscores,"
                            + " is refused for every entity. Rename the column or field.");
        }
        if (isTooLong(name)) {
            throw new ValidationException(
                    "Field '"
                            + path
                            + "' has a name longer than "
                            + MAX_NAME_BYTES
                            + " bytes, which Datastore refuses as a property name for every"
                            + " entity. Rename the column or field.");
        }
    }

    private static String quoted(Set<String> names) {
        StringJoiner joined = new StringJoiner(", ");
        for (String name : names) {
            joined.add("'" + name + "'");
        }
        return joined.toString();
    }

    private static ValidationException unsupported(String path, LogicalType type, String reason) {
        return new ValidationException(
                "Field '" + path + "' has the type " + type.asSummaryString() + ". " + reason);
    }

    /** Returns the physical row type. */
    public RowType getRowType() {
        return rowType;
    }

    /** Returns whether the table declares a PRIMARY KEY, the entity key's name or id. */
    public boolean hasPrimaryKey() {
        return keyIndex >= 0;
    }

    /** Returns the index of the key column, or {@code -1} when there is none. */
    public int getKeyIndex() {
        return keyIndex;
    }

    /** Returns whether the key column is the key's numeric id rather than its name. */
    public boolean isKeyId() {
        return keyIndex >= 0 && rowType.getTypeAt(keyIndex).is(LogicalTypeRoot.BIGINT);
    }

    /** Returns whether the top-level column {@code name} is written excluded from indexes. */
    public boolean isUnindexed(String name) {
        return unindexedColumns.contains(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatastoreTableSchema that = (DatastoreTableSchema) o;
        return keyIndex == that.keyIndex
                && rowType.equals(that.rowType)
                && unindexedColumns.equals(that.unindexedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, keyIndex, unindexedColumns);
    }
}
