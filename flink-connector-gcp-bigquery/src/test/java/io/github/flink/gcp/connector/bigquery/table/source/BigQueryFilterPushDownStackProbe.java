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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.RowType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Child-process entry point for {@link BigQueryFilterPushDownStackBoundaryTest}. */
final class BigQueryFilterPushDownStackProbe {
    private static final RowType ROW =
            (RowType) DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())).getLogicalType();
    private static final ResolvedExpression LEAF = equalTo("a");
    private static final String LEAF_SQL = "(`value` = 'a')";

    private BigQueryFilterPushDownStackProbe() {}

    public static void main(String[] args) {
        String scenario = args[0];
        switch (scenario) {
            case "fallback":
                fallback();
                break;
            case "budget":
                budget();
                break;
            case "and-left":
            case "and-right":
            case "or-left":
            case "or-right":
            case "mixed-left":
            case "mixed-right":
                for (int leaves : new int[] {2_048, 8_192, 32_768}) {
                    ResolvedExpression expression = nested(scenario, leaves);
                    assertRestriction(
                            Collections.singletonList(expression),
                            null,
                            expected(scenario, leaves),
                            expression);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
        System.out.println("PASS " + scenario);
    }

    private static void fallback() {
        ResolvedExpression deep = nested("mixed-left", 8_192);
        ResolvedExpression unsupported = call(BuiltInFunctionDefinitions.NOT, LEAF);
        ResolvedExpression later = equalTo("b");
        ResolvedExpression failed = call(BuiltInFunctionDefinitions.OR, deep, unsupported);
        assertRestriction(Arrays.asList(failed, later), null, "((`value` = 'b'))", later);
        ResolvedExpression and = call(BuiltInFunctionDefinitions.AND, failed, later);
        assertRestriction(Collections.singletonList(and), null, "(((`value` = 'b')))", and);

        ResolvedExpression partial = call(BuiltInFunctionDefinitions.AND, deep, unsupported);
        ResolvedExpression or = call(BuiltInFunctionDefinitions.OR, partial, later);
        String deepSql = expected("mixed-left", 8_192);
        assertRestriction(
                Collections.singletonList(or), null, "((" + deepSql + " OR (`value` = 'b')))", or);

        ResolvedExpression alternating = LEAF;
        for (int i = 0; i < 8_192; i++) {
            alternating =
                    call(
                            BuiltInFunctionDefinitions.AND,
                            call(BuiltInFunctionDefinitions.OR, alternating, unsupported),
                            later);
        }
        assertRestriction(
                Collections.singletonList(alternating), null, "(((`value` = 'b')))", alternating);
    }

    private static void budget() {
        ResolvedExpression deepOr = nested("or-left", 8_192);
        String expected = expected("or-left", 8_192);
        String prefix = "é漢😀";
        String explicit =
                prefix
                        + "x"
                                .repeat(
                                        BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES
                                                - utf8(
                                                        BigQueryFilterPushDown
                                                                .combinedRowRestriction(
                                                                        prefix, expected)));
        assertRestriction(Collections.singletonList(deepOr), explicit, expected, deepOr);
        assertRestriction(Collections.singletonList(deepOr), explicit + "x", null);

        ResolvedExpression oversizedOr = nested("or-left", 65_536);
        String laterValue =
                "b"
                        .repeat(
                                BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES
                                        - utf8("(" + LEAF_SQL + ")")
                                        + 1);
        ResolvedExpression later = equalTo(laterValue);
        String laterSql = "((`value` = '" + laterValue + "'))";
        assertThat(utf8(laterSql)).isEqualTo(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        assertRestriction(Arrays.asList(oversizedOr, later), null, laterSql, later);
        ResolvedExpression oversizedAnd = nested("and-left", 65_536);
        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(
                        ROW, Collections.singletonList(oversizedAnd), null);
        assertThat(state.rowRestriction()).isNotNull();
        assertThat(utf8(state.rowRestriction()))
                .isLessThanOrEqualTo(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        assertThat(state.rowRestriction()).contains(LEAF_SQL);
        assertIdentities(state.result().getRemainingFilters(), oversizedAnd);
        assertIdentities(state.result().getAcceptedFilters(), oversizedAnd);
    }

    private static ResolvedExpression nested(String scenario, int leaves) {
        ResolvedExpression expression = LEAF;
        for (int i = 1; i < leaves; i++) {
            BuiltInFunctionDefinition operator = operator(scenario, i);
            expression =
                    scenario.endsWith("left")
                            ? call(operator, expression, LEAF)
                            : call(operator, LEAF, expression);
        }
        return expression;
    }

    private static String expected(String scenario, int leaves) {
        StringBuilder output = new StringBuilder("(");
        if (scenario.endsWith("left")) {
            output.append("(".repeat(leaves - 1)).append(LEAF_SQL);
            for (int i = 1; i < leaves; i++) {
                output.append(separator(scenario, i)).append(LEAF_SQL).append(')');
            }
        } else {
            for (int i = leaves - 1; i > 0; i--) {
                output.append('(').append(LEAF_SQL).append(separator(scenario, i));
            }
            output.append(LEAF_SQL).append(")".repeat(leaves - 1));
        }
        return output.append(')').toString();
    }

    private static String separator(String scenario, int index) {
        return operator(scenario, index) == BuiltInFunctionDefinitions.AND ? " AND " : " OR ";
    }

    private static BuiltInFunctionDefinition operator(String scenario, int index) {
        return scenario.startsWith("and") || (scenario.startsWith("mixed") && index % 2 == 0)
                ? BuiltInFunctionDefinitions.AND
                : BuiltInFunctionDefinitions.OR;
    }

    private static ResolvedExpression equalTo(String value) {
        return call(
                BuiltInFunctionDefinitions.EQUALS,
                new FieldReferenceExpression("value", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression(value));
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }

    private static void assertRestriction(
            List<ResolvedExpression> filters,
            String explicit,
            String expected,
            ResolvedExpression... accepted) {
        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(ROW, filters, explicit);
        assertThat(state.rowRestriction()).isEqualTo(expected);
        assertIdentities(
                state.result().getRemainingFilters(), filters.toArray(new ResolvedExpression[0]));
        assertIdentities(state.result().getAcceptedFilters(), accepted);
        if (expected != null) {
            assertThat(utf8(BigQueryFilterPushDown.combinedRowRestriction(explicit, expected)))
                    .isLessThanOrEqualTo(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        }
    }

    private static void assertIdentities(
            List<ResolvedExpression> actual, ResolvedExpression... expected) {
        // Flink's recursive expression equality and diagnostics are outside this probe's boundary.
        assertThat(actual.size()).isEqualTo(expected.length);
        for (int i = 0; i < expected.length; i++) {
            assertThat(actual.get(i) == expected[i]).as("filter identity at index %s", i).isTrue();
        }
    }

    private static int utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
