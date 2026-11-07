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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides how far a planner field path may be pushed into Storage Read as a nested path.
 *
 * <p>The projection, the converter and the filter translator share these rules, so a path the
 * session is asked for and a path a restriction names cannot disagree (ADR-0100).
 */
@Internal
final class NestedFieldPaths {

    private static final Set<String> RANGE_FIELDS = new HashSet<>(Arrays.asList("start", "end"));
    private static final Set<String> INTERVAL_FIELDS =
            new HashSet<>(Arrays.asList("months", "days", "microseconds"));

    private NestedFieldPaths() {}

    /**
     * Whether a nested path may descend into the given type.
     *
     * <p>Only a ROW that BigQuery stores as a STRUCT can be descended into. The planner does not
     * fetch the BigQuery schema, so a ROW declaring only fields of the source's {@code RANGE}
     * mapping, {@code start} and {@code end}, may be a BigQuery {@code RANGE}, read whole or in
     * part by name; Storage Read rejects a selected field or a restriction below a {@code RANGE},
     * so the path stops at such a ROW and reads it whole. A ROW declaring only fields of the
     * measured {@code INTERVAL} record stops for the same reason, which also leaves its rejection
     * to the converter.
     *
     * @param type the type of the field the path would descend from
     * @return whether a nested path may name a field of it
     */
    private static boolean isDescendable(LogicalType type) {
        if (!(type instanceof RowType)) {
            return false;
        }
        List<String> names = ((RowType) type).getFieldNames();
        return !RANGE_FIELDS.containsAll(names) && !INTERVAL_FIELDS.containsAll(names);
    }

    /**
     * Whether a ROW reachable through ROW fields alone holds a NOT NULL field below a nullable one.
     *
     * <p>Flink's nested projection types a projected field as it is declared, so such a field would
     * be produced NOT NULL although its ROW may be NULL (#2050).
     *
     * @param type the type to inspect
     * @return whether nested projection must not be advertised for it
     */
    static boolean hasNotNullFieldUnderNullableRow(LogicalType type) {
        return hasNotNullFieldUnderNullableRow(type, false);
    }

    private static boolean hasNotNullFieldUnderNullableRow(
            LogicalType type, boolean underNullable) {
        if (underNullable && !type.isNullable()) {
            return true;
        }
        if (!(type instanceof RowType)) {
            return false;
        }
        for (LogicalType field : type.getChildren()) {
            if (hasNotNullFieldUnderNullableRow(field, underNullable || type.isNullable())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the field names of the part of a planner path that Storage Read is asked for.
     *
     * <p>The path stops at the first field that cannot be descended into; the converter reads the
     * rest of the path from the value returned for that field.
     *
     * @param physicalRowType the physical row type
     * @param path the planner path, by field index
     * @return the names of the selected prefix, never empty
     */
    private static List<String> selectedPrefix(RowType physicalRowType, int[] path) {
        List<String> names = new ArrayList<>(path.length);
        LogicalType type = physicalRowType;
        for (int index : path) {
            if (!names.isEmpty() && !isDescendable(type)) {
                break;
            }
            RowType row = (RowType) type;
            names.add(row.getFieldNames().get(index));
            type = row.getTypeAt(index);
        }
        return names;
    }

    /**
     * Returns the Storage Read {@code selected_fields} for a projection.
     *
     * <p>Equal paths collapse, and a path below another selected path is dropped because Storage
     * Read returns the whole field for the shorter one. The order of the result does not matter to
     * Storage Read, which orders fields by the table schema; the converter addresses fields by
     * name.
     *
     * @param physicalRowType the physical row type
     * @param paths the planner paths, by field index
     * @return the dotted field names
     */
    static List<String> selectedFields(RowType physicalRowType, int[][] paths) {
        Set<String> dotted = new LinkedHashSet<>();
        for (int[] path : paths) {
            dotted.add(String.join(".", selectedPrefix(physicalRowType, path)));
        }
        List<String> selected = new ArrayList<>();
        for (String candidate : dotted) {
            if (!hasSelectedAncestor(candidate, dotted)) {
                selected.add(candidate);
            }
        }
        return selected;
    }

    private static boolean hasSelectedAncestor(String candidate, Set<String> selected) {
        for (int dot = candidate.lastIndexOf('.');
                dot > 0;
                dot = candidate.lastIndexOf('.', dot - 1)) {
            if (selected.contains(candidate.substring(0, dot))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves a nested field reference for a row restriction.
     *
     * <p>A path translates only through ROWs that can be descended into, and only where no NOT NULL
     * field follows a nullable one: Flink treats such a field as never NULL, while BigQuery yields
     * NULL for it under a NULL parent, so the two would disagree on {@code IS NULL}, {@code IS NOT
     * NULL} and every comparison's NULL outcome.
     *
     * @param physicalRowType the physical row type
     * @param fieldNames the referenced path, outermost first
     * @return the leaf type, or {@code null} when the path does not translate
     */
    @Nullable
    static LogicalType restrictableLeaf(RowType physicalRowType, String[] fieldNames) {
        LogicalType type = physicalRowType;
        boolean nullableAbove = false;
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0 && !isDescendable(type)) {
                return null;
            }
            RowType row = (RowType) type;
            int index = row.getFieldNames().indexOf(fieldNames[i]);
            if (index < 0) {
                return null;
            }
            if (i > 0) {
                nullableAbove |= type.isNullable();
            }
            type = row.getTypeAt(index);
            if (nullableAbove && !type.isNullable()) {
                return null;
            }
        }
        return type;
    }
}
