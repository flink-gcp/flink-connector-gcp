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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;

/** Checks the operand types before a read-modify-write sink reaches a TaskManager. */
@Internal
public final class ReadModifyWriteSchemaChecks {
    private ReadModifyWriteSchemaChecks() {}

    /**
     * Validates cells of a read-modify-write schema; other write modes keep their own contract.
     *
     * @param schema the parsed physical schema
     * @param mode the selected operation
     */
    public static void validate(BigtableTableSchema schema, WriteMode mode) {
        if (mode != WriteMode.APPEND && mode != WriteMode.INCREMENT) {
            return;
        }
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            if (family.isMap()) {
                // A map's rules are per entry (ADR-0172), so its value type is the operand type.
                LogicalType type = family.getMapValueType();
                if (!accepts(mode, type)) {
                    throw new ValidationException(
                            "Bigtable 'sink.write-mode' = '"
                                    + mode
                                    + "' requires "
                                    + accepted(mode)
                                    + " values; MAP column family '"
                                    + family.getName()
                                    + "' has value type "
                                    + type.asSummaryString()
                                    + ".");
                }
                continue;
            }
            for (BigtableTableSchema.Qualifier qualifier : family.getQualifiers()) {
                if (!accepts(mode, qualifier.getType())) {
                    throw new ValidationException(
                            "Bigtable 'sink.write-mode' = '"
                                    + mode
                                    + "' requires "
                                    + accepted(mode)
                                    + " cells; column '"
                                    + family.getName()
                                    + "."
                                    + qualifier.getName()
                                    + "' has type "
                                    + qualifier.getType().asSummaryString()
                                    + ".");
                }
            }
        }
    }

    private static boolean accepts(WriteMode mode, LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        return mode == WriteMode.INCREMENT
                ? root == LogicalTypeRoot.BIGINT
                : root == LogicalTypeRoot.CHAR
                        || root == LogicalTypeRoot.VARCHAR
                        || root == LogicalTypeRoot.BINARY
                        || root == LogicalTypeRoot.VARBINARY;
    }

    private static String accepted(WriteMode mode) {
        return mode == WriteMode.INCREMENT ? "BIGINT" : "CHAR, VARCHAR, BINARY or VARBINARY";
    }
}
