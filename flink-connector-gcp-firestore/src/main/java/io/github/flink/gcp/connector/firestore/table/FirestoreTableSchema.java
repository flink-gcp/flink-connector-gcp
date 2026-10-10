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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A {@code firestore} table's physical schema, checked against what a Firestore document can hold:
 * which column is the document id, and what Firestore value each column and nested field is.
 *
 * <p>A field path names a column, then a ROW field by {@code .name}; an ARRAY is transparent, so a
 * path through an ARRAY names its elements, and a MAP's values are {@code .value}. The two schema
 * markers name such paths: {@code geo-point-field-paths} for a ROW of the DOUBLE fields {@code
 * latitude} and {@code longitude} that is a geographical point, and {@code reference-field-paths}
 * for a STRING that is a reference to a document.
 */
@Internal
public final class FirestoreTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The two fields, in this order, of a ROW a geo-point marker may name. */
    static final List<String> GEO_POINT_FIELDS = List.of("latitude", "longitude");

    private final RowType rowType;
    private final int keyIndex;
    private final Set<String> geoPointPaths;
    private final Set<String> referencePaths;

    private FirestoreTableSchema(
            RowType rowType, int keyIndex, Set<String> geoPointPaths, Set<String> referencePaths) {
        this.rowType = rowType;
        this.keyIndex = keyIndex;
        this.geoPointPaths = geoPointPaths;
        this.referencePaths = referencePaths;
    }

    /**
     * Checks a table's physical row type, its primary key and its markers.
     *
     * @param rowType the physical row type
     * @param primaryKeyIndexes the declared primary key's column indexes, empty when none
     * @param geoPointPaths the {@code geo-point-field-paths} value
     * @param referencePaths the {@code reference-field-paths} value
     * @return the schema
     * @throws ValidationException if a column has no Firestore form, the key is not one STRING
     *     column, or a marker names a path that is not of its type
     */
    public static FirestoreTableSchema of(
            RowType rowType,
            int[] primaryKeyIndexes,
            List<String> geoPointPaths,
            List<String> referencePaths) {
        Set<String> geoPoints = new LinkedHashSet<>(geoPointPaths);
        Set<String> references = new LinkedHashSet<>(referencePaths);
        Set<String> both = new LinkedHashSet<>(geoPoints);
        both.retainAll(references);
        if (!both.isEmpty()) {
            throw new ValidationException(
                    FirestoreConnectorOptions.GEO_POINT_FIELD_PATHS.key()
                            + " and "
                            + FirestoreConnectorOptions.REFERENCE_FIELD_PATHS.key()
                            + " both name "
                            + both
                            + "; a field is one or the other.");
        }
        int keyIndex = keyIndex(rowType, primaryKeyIndexes);
        Set<String> consumed = new LinkedHashSet<>();
        Set<String> seen = new LinkedHashSet<>();
        Set<String> ambiguous = new LinkedHashSet<>();
        for (RowType.RowField field : rowType.getFields()) {
            if (keyIndex >= 0 && field.getName().equals(rowType.getFieldNames().get(keyIndex))) {
                if (geoPoints.contains(field.getName()) || references.contains(field.getName())) {
                    throw new ValidationException(
                            "The PRIMARY KEY column '"
                                    + field.getName()
                                    + "' is the document id, which is never stored as a field, so"
                                    + " no schema marker can name it.");
                }
                continue;
            }
            checkFieldName(field.getName(), field.getName());
            check(
                    field.getName(),
                    field.getType(),
                    geoPoints,
                    references,
                    consumed,
                    seen,
                    ambiguous);
        }
        ambiguous.retainAll(consumed);
        if (!ambiguous.isEmpty()) {
            throw new ValidationException(
                    "Schema markers name field paths that more than one field has: "
                            + ambiguous
                            + ". A path joins names with '.', so a column whose name contains a"
                            + " dot can share it with a ROW field; rename one of them.");
        }
        Set<String> unknown = new LinkedHashSet<>(geoPoints);
        unknown.addAll(references);
        unknown.removeAll(consumed);
        if (!unknown.isEmpty()) {
            throw new ValidationException(
                    "Schema markers name field paths the table does not declare: "
                            + unknown
                            + ". A path names a column, then a ROW field by '.name'; an ARRAY is"
                            + " passed through, and a MAP's values are '.value'.");
        }
        return new FirestoreTableSchema(rowType, keyIndex, geoPoints, references);
    }

    private static int keyIndex(RowType rowType, int[] primaryKeyIndexes) {
        if (primaryKeyIndexes.length == 0) {
            return -1;
        }
        if (primaryKeyIndexes.length > 1) {
            throw new ValidationException(
                    "A firestore table's PRIMARY KEY is the document id, one STRING column, but"
                            + " this one has "
                            + primaryKeyIndexes.length
                            + " columns.");
        }
        int index = primaryKeyIndexes[0];
        LogicalType type = rowType.getTypeAt(index);
        if (!type.isAnyOf(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR)) {
            throw new ValidationException(
                    "The PRIMARY KEY column '"
                            + rowType.getFieldNames().get(index)
                            + "' is the document id and must be STRING, but it is "
                            + type.asSummaryString()
                            + ".");
        }
        return index;
    }

    private static void check(
            String path,
            LogicalType type,
            Set<String> geoPoints,
            Set<String> references,
            Set<String> consumed,
            Set<String> seen,
            Set<String> ambiguous) {
        if (!type.is(LogicalTypeRoot.ARRAY) && !seen.add(path)) {
            ambiguous.add(path);
        }
        if (geoPoints.contains(path) && !type.is(LogicalTypeRoot.ARRAY)) {
            consumed.add(path);
            if (!isGeoPointRow(type)) {
                throw unsupported(
                        path,
                        type,
                        FirestoreConnectorOptions.GEO_POINT_FIELD_PATHS.key()
                                + " names it, so it must be a ROW of the DOUBLE fields latitude"
                                + " and longitude, in that order.");
            }
            return;
        }
        if (references.contains(path) && !type.is(LogicalTypeRoot.ARRAY)) {
            consumed.add(path);
            if (!type.isAnyOf(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR)) {
                throw unsupported(
                        path,
                        type,
                        FirestoreConnectorOptions.REFERENCE_FIELD_PATHS.key()
                                + " names it, so it must be STRING.");
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
                // An array directly inside an array is mapped too: Google's documentation refuses
                // one in a Standard-edition database, but the service stored one when measured
                // (FirestoreTableRealGcpITCase), and an Enterprise-edition database allows it.
                LogicalType element = ((ArrayType) type).getElementType();
                check(path, element, geoPoints, references, consumed, seen, ambiguous);
                return;
            case MAP:
                MapType map = (MapType) type;
                if (!map.getKeyType().isAnyOf(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR)) {
                    throw unsupported(
                            path,
                            type,
                            "A Firestore map's keys are field names, so they are STRING.");
                }
                check(
                        path + ".value",
                        map.getValueType(),
                        geoPoints,
                        references,
                        consumed,
                        seen,
                        ambiguous);
                return;
            case ROW:
                for (RowType.RowField field : ((RowType) type).getFields()) {
                    checkFieldName(path + "." + field.getName(), field.getName());
                    check(
                            path + "." + field.getName(),
                            field.getType(),
                            geoPoints,
                            references,
                            consumed,
                            seen,
                            ambiguous);
                }
                return;
            default:
                throw unsupported(
                        path,
                        type,
                        "A firestore table holds STRING, BIGINT, DOUBLE, BOOLEAN, BYTES,"
                                + " TIMESTAMP_LTZ, ARRAY, MAP with STRING keys and ROW; write an"
                                + " INT as a BIGINT and a FLOAT as a DOUBLE.");
        }
    }

    /**
     * Refuses a field name Firestore reserves, {@code __} at both ends: the service would refuse
     * every document the table writes, so the name is checked once here rather than failing each
     * record (ADR-0127). A MAP's keys are data and are left to the service.
     */
    private static void checkFieldName(String path, String name) {
        if (name.length() >= 4 && name.startsWith("__") && name.endsWith("__")) {
            throw new ValidationException(
                    "Field '"
                            + path
                            + "' has a name Firestore reserves: a field name that starts and ends"
                            + " with '__' is refused for every document. Rename the column or"
                            + " field.");
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

    private static ValidationException unsupported(String path, LogicalType type, String reason) {
        return new ValidationException(
                "Field '" + path + "' has the type " + type.asSummaryString() + ". " + reason);
    }

    /** Returns the physical row type. */
    public RowType getRowType() {
        return rowType;
    }

    /** Returns whether the table declares a PRIMARY KEY, the document id. */
    public boolean hasPrimaryKey() {
        return keyIndex >= 0;
    }

    /** Returns the index of the document-id column, or {@code -1} when there is none. */
    public int getKeyIndex() {
        return keyIndex;
    }

    /** Returns whether the field at {@code path} is a geographical point. */
    public boolean isGeoPoint(String path) {
        return geoPointPaths.contains(path);
    }

    /** Returns whether the field at {@code path} is a reference to a document. */
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
        FirestoreTableSchema that = (FirestoreTableSchema) o;
        return keyIndex == that.keyIndex
                && rowType.equals(that.rowType)
                && geoPointPaths.equals(that.geoPointPaths)
                && referencePaths.equals(that.referencePaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, keyIndex, geoPointPaths, referencePaths);
    }
}
