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
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalFilter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Fixed filter structure with typed runtime value bindings. */
@Internal
final class SqlFilterTemplate implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;
    private final String type;
    private String family;
    private ByteString qualifier;
    private SqlValueBinding value;
    private SqlValueBinding start;
    private SqlValueBinding end;
    private int count;
    private List<SqlFilterTemplate> children = List.of();

    private SqlFilterTemplate(String key, String type) {
        this.key = key;
        this.type = type;
    }

    static SqlFilterTemplate parse(SqlSettingMap settings, List<DataType> arguments) {
        String type = settings.required("type");
        SqlFilterTemplate template = new SqlFilterTemplate(settings.key("type"), type);
        switch (type) {
            case "row-exists":
                break;
            case "cell-exists":
            case "latest-cell-value-equals":
                template.family = settings.nonblank("family");
                template.qualifier = SqlValueBinding.qualifier(settings);
                if (type.equals("latest-cell-value-equals")) {
                    template.value = SqlValueBinding.value(settings, arguments);
                }
                break;
            case "family-equals":
                template.family = settings.nonblank("family");
                break;
            case "qualifier-equals":
                template.qualifier = SqlValueBinding.qualifier(settings);
                break;
            case "value-equals":
                template.value = SqlValueBinding.value(settings, arguments);
                break;
            case "timestamp-range":
                template.start = SqlValueBinding.time(settings, "start-timestamp", arguments, true);
                template.end = SqlValueBinding.time(settings, "end-timestamp", arguments, true);
                template.start.requireMinimum(0);
                template.end.requireMinimum(1);
                SqlValueBinding.checkRange(template.start, template.end);
                break;
            case "cells-per-column":
            case "cells-per-row":
                long count =
                        SqlSettingMap.integer(settings.key("count"), settings.required("count"));
                if (count <= 0 || count > Integer.MAX_VALUE) {
                    throw settings.error("count", "must be a positive INT");
                }
                template.count = (int) count;
                break;
            case "chain":
            case "interleave":
                List<SqlFilterTemplate> children = new ArrayList<>();
                for (SqlSettingMap child : settings.numbered("children")) {
                    children.add(parse(child, arguments));
                }
                if (children.isEmpty()) {
                    throw settings.error("children", "requires at least one child");
                }
                template.children = List.copyOf(children);
                break;
            default:
                throw settings.error("type", "has an unsupported filter type");
        }
        settings.finish();
        return template;
    }

    ConditionalFilter instantiate(RowData input) {
        switch (type) {
            case "row-exists":
                return ConditionalFilter.rowExists();
            case "cell-exists":
                return ConditionalFilter.cellExists(family, qualifier);
            case "latest-cell-value-equals":
                return ConditionalFilter.latestCellValueEquals(
                        family, qualifier, value.bytes(input));
            case "family-equals":
                return ConditionalFilter.familyEquals(family);
            case "qualifier-equals":
                return ConditionalFilter.qualifierEquals(qualifier);
            case "value-equals":
                return ConditionalFilter.valueEquals(value.bytes(input));
            case "timestamp-range":
                long from = start.numberAtLeast(input, 0);
                long to = end.numberAtLeast(input, 1);
                if (to <= from) {
                    throw new ValidationException(
                            "Option '" + key + "' requires end timestamp greater than start.");
                }
                return ConditionalFilter.timestampRange(from, to);
            case "cells-per-column":
                return ConditionalFilter.cellsPerColumn(count);
            case "cells-per-row":
                return ConditionalFilter.cellsPerRow(count);
            case "chain":
            case "interleave":
                ConditionalFilter[] filters =
                        children.stream()
                                .map(child -> child.instantiate(input))
                                .toArray(ConditionalFilter[]::new);
                return type.equals("chain")
                        ? ConditionalFilter.chain(filters)
                        : ConditionalFilter.interleave(filters);
            default:
                throw new IllegalStateException("Unknown compiled filter");
        }
    }
}
