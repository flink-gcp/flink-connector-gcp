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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Computes the union of a destination table's live schema and the serializer's desired schema — the
 * schema the sink proposes when connector-driven schema updates are enabled.
 *
 * <p>The union is strictly widening and order-preserving, which keeps both the table's existing
 * data and the sink's already-serialized row bytes valid:
 *
 * <ul>
 *   <li>existing fields are never dropped, reordered or re-typed (a type difference rejects the
 *       union — BigQuery cannot change column types, and a lost mapping would corrupt rows)
 *   <li>desired-only fields are appended at the end (top level and inside {@code STRUCT}s), forced
 *       {@code NULLABLE} unless {@code REPEATED} (BigQuery cannot add {@code REQUIRED} columns);
 *       gated by {@link SchemaUpdateOptions#isAllowNewFields()}
 *   <li>{@code REQUIRED} existing fields are relaxed to {@code NULLABLE} when the desired schema
 *       does not declare the field {@code REQUIRED} (an unset mode counts as nullable) and {@link
 *       SchemaUpdateOptions#isAllowFieldRelaxation()} is set; modes are never tightened, and {@code
 *       REPEATED} can neither be added to nor removed from an existing field
 *   <li>field names match case-insensitively (BigQuery column names are case-insensitive); the
 *       existing spelling and all other existing attributes (description, max length, ...) are kept
 * </ul>
 *
 * <p>Because the union starts from a fresh read and only ever adds, concurrent unions by parallel
 * subtasks converge: re-running the union on the updated schema yields {@code changed == false}.
 *
 * <p>(The union rules are an independent reimplementation informed by the schema-evolution design
 * of the Aiven/kafka-connect-bigquery connector; see the module README.)
 */
@Internal
public final class SchemaUnifier {

    private SchemaUnifier() {}

    /**
     * A checked rejection of a schema union: the desired schema requires a change the rules (or the
     * configured options) do not permit. Retrying cannot help; the failure is terminal.
     */
    public static final class SchemaUnionException extends IOException {
        private static final long serialVersionUID = 1L;

        SchemaUnionException(String message) {
            super(message);
        }
    }

    /** The result of a union: the unified schema and whether it differs from the existing one. */
    public static final class UnionResult {
        private final TableSchema schema;
        private final boolean changed;

        private UnionResult(TableSchema schema, boolean changed) {
            this.schema = schema;
            this.changed = changed;
        }

        /** Returns the unified schema. */
        public TableSchema getSchema() {
            return schema;
        }

        /** Returns whether the unified schema differs from the existing one. */
        public boolean isChanged() {
            return changed;
        }
    }

    /**
     * Unions the existing (live table) schema with the desired (serializer) schema under the given
     * options.
     *
     * @param existing the live table schema
     * @param desired the serializer's schema
     * @param options the gating options
     * @return the union result
     * @throws SchemaUnionException if the desired schema requires an impermissible change
     */
    public static UnionResult union(
            TableSchema existing, TableSchema desired, SchemaUpdateOptions options)
            throws SchemaUnionException {
        Union union = unionFields(existing.getFieldsList(), desired.getFieldsList(), options, "");
        return new UnionResult(
                TableSchema.newBuilder().addAllFields(union.fields).build(), union.changed);
    }

    private static final class Union {
        private final List<TableFieldSchema> fields;
        private final boolean changed;

        Union(List<TableFieldSchema> fields, boolean changed) {
            this.fields = fields;
            this.changed = changed;
        }
    }

    private static Union unionFields(
            List<TableFieldSchema> existing,
            List<TableFieldSchema> desired,
            SchemaUpdateOptions options,
            String path)
            throws SchemaUnionException {
        Map<String, TableFieldSchema> desiredByName = new LinkedHashMap<>();
        for (TableFieldSchema field : desired) {
            desiredByName.put(field.getName().toLowerCase(Locale.ROOT), field);
        }
        List<TableFieldSchema> unified = new ArrayList<>(existing.size() + desired.size());
        boolean changed = false;
        for (TableFieldSchema existingField : existing) {
            TableFieldSchema desiredField =
                    desiredByName.remove(existingField.getName().toLowerCase(Locale.ROOT));
            if (desiredField == null) {
                // Never drop: fields absent from the desired schema are kept as they are.
                unified.add(existingField);
                continue;
            }
            TableFieldSchema unifiedField = unifyField(existingField, desiredField, options, path);
            changed |= !unifiedField.equals(existingField);
            unified.add(unifiedField);
        }
        for (TableFieldSchema newField : desiredByName.values()) {
            if (!options.isAllowNewFields()) {
                throw new SchemaUnionException(
                        "The serializer schema adds the field "
                                + path
                                + newField.getName()
                                + ", which the destination table does not have; adding fields is"
                                + " not enabled (SchemaUpdateOptions.allowNewFields)");
            }
            unified.add(forceAddable(newField));
            changed = true;
        }
        return new Union(unified, changed);
    }

    private static TableFieldSchema unifyField(
            TableFieldSchema existing,
            TableFieldSchema desired,
            SchemaUpdateOptions options,
            String path)
            throws SchemaUnionException {
        String fieldPath = path + existing.getName();
        if (existing.getType() != desired.getType()) {
            throw new SchemaUnionException(
                    "The serializer schema changes the type of the field "
                            + fieldPath
                            + " from "
                            + existing.getType()
                            + " to "
                            + desired.getType()
                            + "; BigQuery column types cannot be changed");
        }
        boolean existingRepeated = existing.getMode() == TableFieldSchema.Mode.REPEATED;
        boolean desiredRepeated = desired.getMode() == TableFieldSchema.Mode.REPEATED;
        if (existingRepeated != desiredRepeated) {
            throw new SchemaUnionException(
                    "The serializer schema changes the field "
                            + fieldPath
                            + (desiredRepeated ? " to" : " from")
                            + " REPEATED; the REPEATED mode of a BigQuery column cannot be"
                            + " changed");
        }
        TableFieldSchema.Builder unified = existing.toBuilder();
        if (existing.getMode() == TableFieldSchema.Mode.REQUIRED
                && desired.getMode() != TableFieldSchema.Mode.REQUIRED
                && options.isAllowFieldRelaxation()) {
            // Anything not explicitly declared REQUIRED counts as nullable (an unset mode
            // included), mirroring how the converters default modes; modes are never tightened.
            // Relaxation stays behind the allowFieldRelaxation opt-in because it is irreversible
            // on the BigQuery side.
            unified.setMode(TableFieldSchema.Mode.NULLABLE);
        }
        if (existing.getType() == TableFieldSchema.Type.STRUCT) {
            Union subUnion =
                    unionFields(
                            existing.getFieldsList(),
                            desired.getFieldsList(),
                            options,
                            fieldPath + ".");
            unified.clearFields().addAllFields(subUnion.fields);
        }
        return unified.build();
    }

    /**
     * Prepares a desired-only field for appending: {@code REQUIRED} becomes {@code NULLABLE}
     * (BigQuery cannot add {@code REQUIRED} columns), {@code REPEATED} is kept.
     */
    private static TableFieldSchema forceAddable(TableFieldSchema field) {
        TableFieldSchema.Builder addable = field.toBuilder();
        if (field.getMode() == TableFieldSchema.Mode.REQUIRED) {
            addable.setMode(TableFieldSchema.Mode.NULLABLE);
        }
        if (field.getType() == TableFieldSchema.Type.STRUCT) {
            List<TableFieldSchema> subFields = new ArrayList<>(field.getFieldsCount());
            for (TableFieldSchema subField : field.getFieldsList()) {
                subFields.add(forceAddable(subField));
            }
            addable.clearFields().addAllFields(subFields);
        }
        return addable.build();
    }
}
