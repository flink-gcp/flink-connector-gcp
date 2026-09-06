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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Resolves the aggregate input contract before a sink reaches a task manager. */
@Internal
public final class AggregateOptionsMapper {
    private AggregateOptionsMapper() {}

    /** Returns validated family types, or an empty map for another write mode. */
    public static Map<String, ColumnFamilyType> map(
            ReadableConfig config, BigtableTableSchema schema) {
        String key = BigtableConnectorOptions.SINK_AGGREGATE_COLUMN_FAMILY_TYPES.key();
        Map<String, String> declared =
                config.getOptional(BigtableConnectorOptions.SINK_AGGREGATE_COLUMN_FAMILY_TYPES)
                        .orElse(null);
        if (config.get(BigtableConnectorOptions.SINK_WRITE_MODE) != WriteMode.AGGREGATE) {
            if (declared != null) {
                throw new ValidationException(
                        "Option '" + key + "' requires 'sink.write-mode' = 'aggregate'.");
            }
            return Collections.emptyMap();
        }
        if (declared == null) {
            throw new ValidationException(
                    "'sink.write-mode' = 'aggregate' requires '" + key + "'.");
        }
        Map<String, ColumnFamilyType> types = new LinkedHashMap<>();
        Set<LogicalTypeRoot> supported =
                Set.of(
                        LogicalTypeRoot.TINYINT,
                        LogicalTypeRoot.SMALLINT,
                        LogicalTypeRoot.INTEGER,
                        LogicalTypeRoot.BIGINT);
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            String value = declared.get(family.getName());
            ColumnFamilyType type = null;
            for (ColumnFamilyType candidate : ColumnFamilyType.values()) {
                if (candidate != ColumnFamilyType.RAW
                        && candidate.toString().equalsIgnoreCase(value)) {
                    type = candidate;
                }
            }
            if (type == null) {
                throw new ValidationException(
                        "Option '"
                                + key
                                + "' must declare column family '"
                                + family.getName()
                                + "' as int64-sum, int64-min, int64-max or int64-hll; found "
                                + value
                                + ".");
            }
            for (BigtableTableSchema.Qualifier qualifier : family.getQualifiers()) {
                if (!supported.contains(qualifier.getType().getTypeRoot())) {
                    throw new ValidationException(
                            "Aggregate column '"
                                    + family.getName()
                                    + "."
                                    + qualifier.getName()
                                    + "' requires TINYINT, SMALLINT, INT or BIGINT; found "
                                    + qualifier.getType()
                                    + ".");
                }
            }
            types.put(family.getName(), type);
        }
        for (String family : declared.keySet()) {
            if (!types.containsKey(family)) {
                throw new ValidationException(
                        "Option '" + key + "' names undeclared column family '" + family + "'.");
            }
        }
        return Collections.unmodifiableMap(types);
    }
}
