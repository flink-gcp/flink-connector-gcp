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
            for (BigtableTableSchema.Qualifier qualifier : family.getQualifiers()) {
                LogicalTypeRoot type = qualifier.getType().getTypeRoot();
                boolean valid =
                        mode == WriteMode.INCREMENT
                                ? type == LogicalTypeRoot.BIGINT
                                : type == LogicalTypeRoot.CHAR
                                        || type == LogicalTypeRoot.VARCHAR
                                        || type == LogicalTypeRoot.BINARY
                                        || type == LogicalTypeRoot.VARBINARY;
                if (!valid) {
                    throw new ValidationException(
                            "Bigtable 'sink.write-mode' = '"
                                    + mode
                                    + "' requires "
                                    + (mode == WriteMode.INCREMENT
                                            ? "BIGINT"
                                            : "CHAR, VARCHAR, BINARY or VARBINARY")
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
}
