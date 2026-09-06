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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;

import java.io.Serializable;
import java.util.List;

/** One ordered conditional mutation, with its target fixed by the SQL settings. */
@Internal
final class SqlMutationTemplate implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;
    private final String operation;
    private String family;
    private ByteString qualifier;
    private SqlValueBinding value;
    private SqlValueBinding timestamp;
    private SqlValueBinding start;
    private SqlValueBinding end;

    private SqlMutationTemplate(String key, String operation) {
        this.key = key;
        this.operation = operation;
    }

    static SqlMutationTemplate parse(SqlSettingMap settings, List<DataType> arguments) {
        String operation = settings.required("operation");
        SqlMutationTemplate template =
                new SqlMutationTemplate(settings.key("operation"), operation);
        switch (operation) {
            case "delete-row":
                break;
            case "delete-family":
                template.family = settings.nonblank("family");
                break;
            case "delete-cells":
                template.family = settings.nonblank("family");
                template.qualifier = SqlValueBinding.qualifier(settings);
                template.start =
                        SqlValueBinding.time(settings, "start-timestamp", arguments, false);
                template.end = SqlValueBinding.time(settings, "end-timestamp", arguments, false);
                if (template.start != null) {
                    template.start.requireMinimum(0);
                }
                if (template.end != null) {
                    template.end.requireMinimum(1);
                }
                SqlValueBinding.checkRange(template.start, template.end);
                break;
            case "set-cell":
            case "add-to-cell":
            case "merge-to-cell":
                template.family = settings.nonblank("family");
                template.qualifier = SqlValueBinding.qualifier(settings);
                template.value = SqlValueBinding.value(settings, arguments);
                boolean aggregate = !operation.equals("set-cell");
                template.timestamp =
                        SqlValueBinding.time(settings, "timestamp", arguments, aggregate);
                if (template.timestamp != null) {
                    template.timestamp.requireMinimum(aggregate ? 0 : -1);
                }
                if (aggregate) {
                    template.value.requireAggregate();
                }
                break;
            default:
                throw settings.error("operation", "has an unsupported conditional mutation");
        }
        settings.finish();
        return template;
    }

    ConditionalMutation instantiate(RowData input) {
        switch (operation) {
            case "delete-row":
                return ConditionalMutation.deleteRow();
            case "delete-family":
                return ConditionalMutation.deleteFamily(family);
            case "delete-cells":
                Long from = start == null ? null : start.numberAtLeast(input, 0);
                Long to = end == null ? null : end.numberAtLeast(input, 1);
                if (from != null && to != null && to <= from) {
                    throw new ValidationException(
                            "Option '" + key + "' requires end timestamp greater than start.");
                }
                return ConditionalMutation.deleteCells(family, qualifier, from, to);
            case "set-cell":
                return ConditionalMutation.setCell(
                        family,
                        qualifier,
                        timestamp == null
                                ? System.currentTimeMillis() * 1000L
                                : timestamp.numberAtLeast(input, -1),
                        value.bytes(input));
            case "add-to-cell":
                return ConditionalMutation.addToCell(
                        family,
                        qualifier,
                        timestamp.numberAtLeast(input, 0),
                        value.aggregate(input));
            case "merge-to-cell":
                return ConditionalMutation.mergeToCell(
                        family,
                        qualifier,
                        timestamp.numberAtLeast(input, 0),
                        value.aggregate(input));
            default:
                throw new IllegalStateException("Unknown compiled mutation");
        }
    }
}
