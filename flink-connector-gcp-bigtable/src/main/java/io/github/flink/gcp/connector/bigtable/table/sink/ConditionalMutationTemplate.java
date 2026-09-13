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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;

import javax.annotation.Nullable;

import java.io.Serializable;

/** One immutable ordered mutation with DDL-fixed operation and target. */
@Internal
final class ConditionalMutationTemplate implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;
    private final String operation;
    @Nullable private final String family;
    @Nullable private final ByteString qualifier;
    @Nullable private final ConditionalValueBinding value;
    @Nullable private final ConditionalValueBinding timestamp;
    @Nullable private final ConditionalValueBinding start;
    @Nullable private final ConditionalValueBinding end;

    private ConditionalMutationTemplate(
            String key,
            String operation,
            @Nullable String family,
            @Nullable ByteString qualifier,
            @Nullable ConditionalValueBinding value,
            @Nullable ConditionalValueBinding timestamp,
            @Nullable ConditionalValueBinding start,
            @Nullable ConditionalValueBinding end) {
        this.key = key;
        this.operation = operation;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
        this.start = start;
        this.end = end;
    }

    static ConditionalMutationTemplate compile(ConditionalSettings settings, RowType rowType) {
        String operation = settings.required("operation");
        String family = null;
        ByteString qualifier = null;
        ConditionalValueBinding value = null;
        ConditionalValueBinding timestamp = null;
        ConditionalValueBinding start = null;
        ConditionalValueBinding end = null;
        switch (operation) {
            case "delete-row":
                break;
            case "delete-family":
                family = settings.family();
                break;
            case "delete-cells":
                family = settings.family();
                qualifier = ConditionalValueBinding.qualifier(settings);
                start =
                        ConditionalValueBinding.time(
                                settings, "start-timestamp", rowType, false, 0);
                end = ConditionalValueBinding.time(settings, "end-timestamp", rowType, false, 1);
                ConditionalValueBinding.checkLiteralRange(start, end);
                break;
            case "set-cell":
            case "add-to-cell":
            case "merge-to-cell":
                family = settings.family();
                qualifier = ConditionalValueBinding.qualifier(settings);
                value = ConditionalValueBinding.value(settings, rowType);
                boolean aggregate = !operation.equals("set-cell");
                timestamp =
                        ConditionalValueBinding.time(
                                settings, "timestamp", rowType, aggregate, aggregate ? 0 : -1);
                if (aggregate) {
                    value.requireAggregate();
                }
                break;
            default:
                throw settings.error("operation", "has an unsupported conditional mutation");
        }
        settings.finish();
        return new ConditionalMutationTemplate(
                settings.key("operation"),
                operation,
                family,
                qualifier,
                value,
                timestamp,
                start,
                end);
    }

    ConditionalMutation instantiate(RowData input, RowDataSerializationSchema.CellClock clock) {
        switch (operation) {
            case "delete-row":
                return ConditionalMutation.deleteRow();
            case "delete-family":
                return ConditionalMutation.deleteFamily(family);
            case "delete-cells":
                Long from = start == null ? null : start.number(input, 0);
                Long to = end == null ? null : end.number(input, 1);
                if (from != null && to != null && from >= to) {
                    throw new ValidationException(
                            "Option '" + key + "' requires end timestamp greater than start.");
                }
                return ConditionalMutation.deleteCells(family, qualifier, from, to);
            case "set-cell":
                return ConditionalMutation.setCell(
                        family,
                        qualifier,
                        timestamp == null ? clock.micros() : timestamp.number(input, -1),
                        value.bytes(input));
            case "add-to-cell":
                return ConditionalMutation.addToCell(
                        family, qualifier, timestamp.number(input, 0), value.aggregate(input));
            case "merge-to-cell":
                return ConditionalMutation.mergeToCell(
                        family, qualifier, timestamp.number(input, 0), value.aggregate(input));
            default:
                throw new IllegalStateException(
                        "Unknown compiled conditional mutation: " + operation);
        }
    }
}
