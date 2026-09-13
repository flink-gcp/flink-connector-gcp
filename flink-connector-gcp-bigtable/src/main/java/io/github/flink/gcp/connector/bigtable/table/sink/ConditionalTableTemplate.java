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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalFilter;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A command DDL compiled once into typed bindings and immutable ordered branches. */
@Internal
final class ConditionalTableTemplate implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String PREFIX = "sink.conditional.";
    private final ConditionalValueBinding rowKey;
    private final String predicate;
    @Nullable private final String family;
    @Nullable private final ByteString qualifier;
    @Nullable private final ConditionalValueBinding expected;
    private final List<ConditionalMutationTemplate> thenMutations;
    private final List<ConditionalMutationTemplate> otherwiseMutations;

    private ConditionalTableTemplate(
            ConditionalValueBinding rowKey,
            String predicate,
            @Nullable String family,
            @Nullable ByteString qualifier,
            @Nullable ConditionalValueBinding expected,
            List<ConditionalMutationTemplate> thenMutations,
            List<ConditionalMutationTemplate> otherwiseMutations) {
        this.rowKey = rowKey;
        this.predicate = predicate;
        this.family = family;
        this.qualifier = qualifier;
        this.expected = expected;
        this.thenMutations = List.copyOf(thenMutations);
        this.otherwiseMutations = List.copyOf(otherwiseMutations);
    }

    static ConditionalTableTemplate compile(RowType rowType, Map<String, String> options) {
        Configuration config = Configuration.fromMap(options);
        String rowKeyKey = BigtableConnectorOptions.SINK_CONDITIONAL_ROW_KEY_COLUMN.key();
        String name = options.get(rowKeyKey);
        if (name == null) {
            throw new ValidationException("Option '" + rowKeyKey + "' is required.");
        }
        ConditionalValueBinding rowKey = ConditionalValueBinding.column(rowKeyKey, name, rowType);
        Map<String, String> attributes = new HashMap<>();
        options.forEach(
                (key, value) -> {
                    if (key.startsWith(PREFIX + "predicate.")) {
                        attributes.put(key.substring((PREFIX + "predicate.").length()), value);
                    }
                });
        ConditionalSettings settings = new ConditionalSettings(PREFIX + "predicate.", attributes);
        String predicate = options.get(BigtableConnectorOptions.SINK_CONDITIONAL_PREDICATE.key());
        if (predicate == null) {
            throw new ValidationException("Option 'sink.conditional.predicate' is required.");
        }
        String family = null;
        ByteString qualifier = null;
        ConditionalValueBinding expected = null;
        switch (predicate) {
            case "row-exists":
                break;
            case "cell-exists":
            case "latest-cell-value-equals":
                family = settings.family();
                qualifier = ConditionalValueBinding.qualifier(settings);
                if (predicate.equals("latest-cell-value-equals")) {
                    expected = ConditionalValueBinding.value(settings, rowType);
                }
                break;
            default:
                throw new ValidationException(
                        "Option 'sink.conditional.predicate' must be row-exists, cell-exists or latest-cell-value-equals.");
        }
        settings.finish();
        List<ConditionalMutationTemplate> thenMutations =
                branch(BigtableConnectorOptions.SINK_CONDITIONAL_THEN, rowType, options, config);
        List<ConditionalMutationTemplate> otherwiseMutations =
                branch(
                        BigtableConnectorOptions.SINK_CONDITIONAL_OTHERWISE,
                        rowType,
                        options,
                        config);
        if (thenMutations.isEmpty() && otherwiseMutations.isEmpty()) {
            throw new ValidationException(
                    "Options 'sink.conditional.then' and 'sink.conditional.otherwise' must contain at least one mutation.");
        }
        return new ConditionalTableTemplate(
                rowKey, predicate, family, qualifier, expected, thenMutations, otherwiseMutations);
    }

    private static List<ConditionalMutationTemplate> branch(
            ConfigOption<Map<String, String>> option,
            RowType rowType,
            Map<String, String> raw,
            Configuration config) {
        if (raw.containsKey(option.key())
                && raw.keySet().stream().anyMatch(key -> key.startsWith(option.key() + "."))) {
            throw new ValidationException(
                    "Option '"
                            + option.key()
                            + "' must use either the packed map syntax or prefixed map entries, not both.");
        }
        List<ConditionalMutationTemplate> result = new ArrayList<>();
        for (ConditionalSettings mutation :
                ConditionalSettings.branch(
                        option.key(), config.getOptional(option).orElse(Map.of()))) {
            result.add(ConditionalMutationTemplate.compile(mutation, rowType));
        }
        return result;
    }

    ConditionalRequest instantiate(RowData input, RowDataSerializationSchema.CellClock clock) {
        if (input.getRowKind() != RowKind.INSERT) {
            throw new ValidationException(
                    "Bigtable 'sink.write-mode' = 'conditional' requires INSERT-only input.");
        }
        ByteString key = rowKey.bytes(input);
        if (key.isEmpty()) {
            throw new ValidationException(
                    "Option 'sink.conditional.row-key-column' must not encode to an empty row key.");
        }
        ConditionalFilter filter;
        switch (predicate) {
            case "row-exists":
                filter = ConditionalFilter.rowExists();
                break;
            case "cell-exists":
                filter = ConditionalFilter.cellExists(family, qualifier);
                break;
            case "latest-cell-value-equals":
                filter =
                        ConditionalFilter.latestCellValueEquals(
                                family, qualifier, expected.bytes(input));
                break;
            default:
                throw new IllegalStateException(
                        "Unknown compiled conditional predicate: " + predicate);
        }
        return ConditionalRequest.of(
                key,
                filter,
                instantiate(thenMutations, input, clock),
                instantiate(otherwiseMutations, input, clock));
    }

    private static List<ConditionalMutation> instantiate(
            List<ConditionalMutationTemplate> templates,
            RowData input,
            RowDataSerializationSchema.CellClock clock) {
        List<ConditionalMutation> mutations = new ArrayList<>(templates.size());
        for (ConditionalMutationTemplate template : templates) {
            mutations.add(template.instantiate(input, clock));
        }
        return mutations;
    }
}
