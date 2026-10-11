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
 *
 * <p>A field path names a column, then a ROW field by {@code .name}; an ARRAY is transparent, so a
 * path through an ARRAY names its elements. The two schema markers name such paths: {@code
 * geo-point-field-paths} for a ROW of the DOUBLE fields {@code latitude} and {@code longitude} that
 * is a geographical point, and {@code reference-field-paths} for a ROW that is a key value: the
 * fields {@code kind} (STRING), {@code name} (STRING) and {@code id} (BIGINT) of the key's last
 * path element, optionally followed by {@code ancestors}, an ARRAY of ROWs of the same three
 * fields, root first. A key value is in the table's own project, database and namespace.
 */
@Internal
public final class DatastoreTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The longest kind, key name or property name Datastore stores, in UTF-8 bytes. */
    public static final int MAX_NAME_BYTES = 1500;

    /** The two fields, in this order, of a ROW a geo-point marker may name. */
    private static final List<String> GEO_POINT_FIELDS = List.of("latitude", "longitude");

    /** The three fields, in this order, of a key path element in a ROW a reference marker names. */
    private static final List<String> PATH_ELEMENT_FIELDS = List.of("kind", "name", "id");

    /** The optional fourth field of a ROW a reference marker names: the key's ancestors. */
    private static final String ANCESTORS_FIELD = "ancestors";

    private final RowType rowType;
    private final int keyIndex;
    private final Set<String> unindexedColumns;
    private final Set<String> geoPointPaths;
    private final Set<String> referencePaths;

    private DatastoreTableSchema(
            RowType rowType,
            int keyIndex,
            Set<String> unindexedColumns,
            Set<String> geoPointPaths,
            Set<String> referencePaths) {
        this.rowType = rowType;
        this.keyIndex = keyIndex;
        this.unindexedColumns = unindexedColumns;
        this.geoPointPaths = geoPointPaths;
        this.referencePaths = referencePaths;
    }

    /**
     * Checks a table's physical row type, its primary key, its unindexed columns and its markers.
     *
     * @param rowType the physical row type
     * @param primaryKeyIndexes the declared primary key's column indexes, empty when none
     * @param unindexedColumns the {@code sink.unindexed-columns} value
     * @param geoPointPaths the {@code geo-point-field-paths} value
     * @param referencePaths the {@code reference-field-paths} value
     * @return the schema
     * @throws ValidationException if a column has no Datastore form, the key is not one STRING or
     *     BIGINT column, an unindexed column is not a property of the table, or a marker names a
     *     path that is not of its type
     */
    public static DatastoreTableSchema of(
            RowType rowType,
            int[] primaryKeyIndexes,
            List<String> unindexedColumns,
            List<String> geoPointPaths,
            List<String> referencePaths) {
        Set<String> geoPoints = new LinkedHashSet<>(geoPointPaths);
        Set<String> references = new LinkedHashSet<>(referencePaths);
        Set<String> both = new LinkedHashSet<>(geoPoints);
        both.retainAll(references);
        if (!both.isEmpty()) {
            throw new ValidationException(
                    DatastoreConnectorOptions.GEO_POINT_FIELD_PATHS.key()
                            + " and "
                            + DatastoreConnectorOptions.REFERENCE_FIELD_PATHS.key()
                            + " both name "
                            + quoted(both)
                            + "; a field is one or the other.");
        }
        int keyIndex = keyIndex(rowType, primaryKeyIndexes);
        Markers markers = new Markers(geoPoints, references);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String name = rowType.getFieldNames().get(i);
            if (i == keyIndex) {
                if (geoPoints.contains(name) || references.contains(name)) {
                    throw new ValidationException(
                            "The PRIMARY KEY column '"
                                    + name
                                    + "' is the entity's key, which is never stored as a property,"
                                    + " so no schema marker can name it.");
                }
                continue;
            }
            checkPropertyName(name, name);
            check(name, rowType.getTypeAt(i), markers);
        }
        markers.ambiguous.retainAll(markers.consumed);
        if (!markers.ambiguous.isEmpty()) {
            throw new ValidationException(
                    "Schema markers name field paths that more than one field has: "
                            + quoted(markers.ambiguous)
                            + ". A path joins names with '.', so a column whose name contains a"
                            + " dot can share it with a ROW field; rename one of them.");
        }
        Set<String> unmarked = new LinkedHashSet<>(geoPoints);
        unmarked.addAll(references);
        unmarked.removeAll(markers.consumed);
        if (!unmarked.isEmpty()) {
            throw new ValidationException(
                    "Schema markers name field paths the table does not declare: "
                            + quoted(unmarked)
                            + ". A path names a column, then a ROW field by '.name'; an ARRAY is"
                            + " passed through.");
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
        return new DatastoreTableSchema(rowType, keyIndex, unindexed, geoPoints, references);
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

    /** The markers being matched to the schema's paths while it is checked. */
    private static final class Markers {
        private final Set<String> geoPoints;
        private final Set<String> references;
        private final Set<String> consumed = new LinkedHashSet<>();
        private final Set<String> seen = new LinkedHashSet<>();
        private final Set<String> ambiguous = new LinkedHashSet<>();

        private Markers(Set<String> geoPoints, Set<String> references) {
            this.geoPoints = geoPoints;
            this.references = references;
        }
    }

    private static void check(String path, LogicalType type, Markers markers) {
        if (!type.is(LogicalTypeRoot.ARRAY) && !markers.seen.add(path)) {
            markers.ambiguous.add(path);
        }
        if (markers.geoPoints.contains(path) && !type.is(LogicalTypeRoot.ARRAY)) {
            markers.consumed.add(path);
            if (!isGeoPointRow(type)) {
                throw unsupported(
                        path,
                        type,
                        DatastoreConnectorOptions.GEO_POINT_FIELD_PATHS.key()
                                + " names it, so it must be a ROW of the DOUBLE fields latitude"
                                + " and longitude, in that order.");
            }
            return;
        }
        if (markers.references.contains(path) && !type.is(LogicalTypeRoot.ARRAY)) {
            markers.consumed.add(path);
            if (!isReferenceRow(type)) {
                throw unsupported(
                        path,
                        type,
                        DatastoreConnectorOptions.REFERENCE_FIELD_PATHS.key()
                                + " names it, so it must be a ROW of the fields kind STRING, name"
                                + " STRING and id BIGINT, in that order, optionally followed by"
                                + " ancestors, an ARRAY of ROWs of the same three fields.");
            }
            return;
        }
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
                check(path, element, markers);
                return;
            case ROW:
                for (RowType.RowField field : ((RowType) type).getFields()) {
                    checkPropertyName(path + "." + field.getName(), field.getName());
                    check(path + "." + field.getName(), field.getType(), markers);
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

    private static boolean isGeoPointRow(LogicalType type) {
        if (!type.is(LogicalTypeRoot.ROW)) {
            return false;
        }
        RowType row = (RowType) type;
        return row.getFieldNames().equals(GEO_POINT_FIELDS)
                && row.getTypeAt(0).is(LogicalTypeRoot.DOUBLE)
                && row.getTypeAt(1).is(LogicalTypeRoot.DOUBLE);
    }

    /**
     * Returns whether {@code type} is a key value's ROW: a path element's three fields, optionally
     * followed by an ARRAY of path elements named {@code ancestors}.
     */
    private static boolean isReferenceRow(LogicalType type) {
        if (!type.is(LogicalTypeRoot.ROW)) {
            return false;
        }
        RowType row = (RowType) type;
        List<String> names = row.getFieldNames();
        if (names.size() == PATH_ELEMENT_FIELDS.size()) {
            return isPathElementRow(row);
        }
        if (names.size() != PATH_ELEMENT_FIELDS.size() + 1
                || !names.get(PATH_ELEMENT_FIELDS.size()).equals(ANCESTORS_FIELD)
                || !isPathElementRow(row)) {
            return false;
        }
        LogicalType ancestors = row.getTypeAt(PATH_ELEMENT_FIELDS.size());
        return ancestors.is(LogicalTypeRoot.ARRAY)
                && ((ArrayType) ancestors).getElementType().is(LogicalTypeRoot.ROW)
                && ((RowType) ((ArrayType) ancestors).getElementType()).getFieldCount()
                        == PATH_ELEMENT_FIELDS.size()
                && isPathElementRow((RowType) ((ArrayType) ancestors).getElementType());
    }

    /** Returns whether the first three fields of {@code row} are a key path element's. */
    private static boolean isPathElementRow(RowType row) {
        return row.getFieldNames()
                        .subList(0, PATH_ELEMENT_FIELDS.size())
                        .equals(PATH_ELEMENT_FIELDS)
                && row.getTypeAt(0).isAnyOf(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR)
                && row.getTypeAt(1).isAnyOf(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR)
                && row.getTypeAt(2).is(LogicalTypeRoot.BIGINT);
    }

    /**
     * Returns whether {@code name} has the form {@code __…__}, at least one character between the
     * underscores, which the emulator refuses as a kind, a namespace, a key name or a property name
     * on every write. {@code entity.proto} reserves {@code __.*__}, which also matches {@code
     * ____}, and Datastore's documentation every kind beginning with {@code __}; the emulator
     * stores both, so they are left to the service.
     */
    public static boolean isReserved(String name) {
        return name.length() > 4 && name.startsWith("__") && name.endsWith("__");
    }

    /** Returns whether {@code name} is longer than {@value #MAX_NAME_BYTES} UTF-8 bytes. */
    public static boolean isTooLong(String name) {
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

    /** Returns whether the field at {@code path} is a geographical point. */
    public boolean isGeoPoint(String path) {
        return geoPointPaths.contains(path);
    }

    /** Returns whether the field at {@code path} is a key value. */
    public boolean isReference(String path) {
        return referencePaths.contains(path);
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
                && unindexedColumns.equals(that.unindexedColumns)
                && geoPointPaths.equals(that.geoPointPaths)
                && referencePaths.equals(that.referencePaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, keyIndex, unindexedColumns, geoPointPaths, referencePaths);
    }
}
