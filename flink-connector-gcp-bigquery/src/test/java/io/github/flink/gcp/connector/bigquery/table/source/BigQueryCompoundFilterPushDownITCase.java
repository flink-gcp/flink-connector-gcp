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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Executes compound Boolean composition and residual evaluation locally, without GCP. */
class BigQueryCompoundFilterPushDownITCase {
    @Test
    void aBudgetedNecessaryConditionAndTheResidualPreserveRowsIncludingNulls() throws Exception {
        DataType type =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("tag", DataTypes.INT()));
        ResolvedExpression id = new FieldReferenceExpression("id", DataTypes.BIGINT(), 0, 0);
        ResolvedExpression name = new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);
        String literal = "a moderately long literal that cannot fit in the remaining budget";
        ResolvedExpression predicate =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(
                                BuiltInFunctionDefinitions.AND,
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        id,
                                        new ValueLiteralExpression(1L)),
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        name,
                                        new ValueLiteralExpression(literal)),
                                call(
                                        BuiltInFunctionDefinitions.NOT,
                                        call(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                id,
                                                new ValueLiteralExpression(0L)))),
                        call(BuiltInFunctionDefinitions.IS_NULL, id));
        String expected = "((((`id` = 1)) OR (`id` IS NULL)))";
        // Raw SQL is used only to reserve exactly the available generated-text budget here.
        String explicit =
                "x"
                        .repeat(
                                BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES
                                        - BigQueryFilterPushDown.combinedRowRestriction(
                                                        "", expected)
                                                .getBytes(StandardCharsets.UTF_8)
                                                .length);
        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(
                        (RowType) type.getLogicalType(),
                        Collections.singletonList(predicate),
                        explicit);
        assertThat(state.rowRestriction()).isEqualTo(expected);
        assertThat(state.result().getRemainingFilters()).containsExactly(predicate);

        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.getConfig().set("parallelism.default", "1");
        table.createTemporaryView(
                "input_rows",
                table.fromValues(
                        type,
                        Row.of(1L, literal, 10),
                        Row.of(1L, "short", 11),
                        Row.of(2L, literal, 12),
                        Row.of(null, null, 13),
                        Row.of(1L, null, 14),
                        Row.of(null, "short", 15)));
        String residual = "((id = 1 AND name = '" + literal + "' AND NOT(id = 0)) OR id IS NULL)";
        assertThat(tags(table, residual)).containsExactlyInAnyOrder(10, 13, 15);
        // Flink executes the emitted integer/null/Boolean SQL subset as a composition oracle;
        // this does not re-establish BigQuery's service-specific scalar comparison semantics.
        assertThat(tags(table, state.rowRestriction()))
                .containsExactlyInAnyOrder(10, 11, 13, 14, 15);
        assertThat(tags(table, "(" + state.rowRestriction() + ") AND " + residual))
                .containsExactlyInAnyOrder(10, 13, 15);
    }

    private static List<Integer> tags(TableEnvironment table, String predicate) throws Exception {
        List<Integer> tags = new ArrayList<>();
        try (CloseableIterator<Row> rows =
                table.executeSql("SELECT tag FROM input_rows WHERE " + predicate).collect()) {
            rows.forEachRemaining(row -> tags.add((Integer) row.getField(0)));
        }
        return tags;
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }
}
