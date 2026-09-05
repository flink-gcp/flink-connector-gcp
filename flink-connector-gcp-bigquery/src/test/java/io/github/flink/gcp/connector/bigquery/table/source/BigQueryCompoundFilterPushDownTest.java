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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Compound restrictions are selected against one budget before their text is rendered. */
class BigQueryCompoundFilterPushDownTest {
    private static final RowType ROW =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                                    DataTypes.FIELD("payload", DataTypes.BYTES()))
                            .getLogicalType();

    @ParameterizedTest
    @ValueSource(ints = {8, 32, 128})
    void wideAndKeepsFittingChildrenAndWideOrRequiresEveryChild(int width) {
        ResolvedExpression large = eq("name", DataTypes.STRING(), "a".repeat(200_000));
        ResolvedExpression[] children = new ResolvedExpression[width];
        Arrays.fill(children, large);
        String leaf = "(`name` = '" + "a".repeat(200_000) + "')";
        String expected = "((" + String.join(" AND ", Collections.nCopies(4, leaf)) + "))";
        assertRestriction(call(BuiltInFunctionDefinitions.AND, children), null, expected);
        assertRestriction(call(BuiltInFunctionDefinitions.OR, children), null, null);
    }

    @ParameterizedTest
    @ValueSource(ints = {8, 32, 128})
    void nestedAndKeepsItsGroupingAndNestedOrCannotLoseBranches(int depth) {
        ResolvedExpression leaf = eq("name", DataTypes.STRING(), "a".repeat(200_000));
        ResolvedExpression and = leaf;
        ResolvedExpression or = leaf;
        String expected = "(`name` = '" + "a".repeat(200_000) + "')";
        String leafSql = expected;
        for (int i = 1; i < depth; i++) {
            and = call(BuiltInFunctionDefinitions.AND, and, leaf);
            or = call(BuiltInFunctionDefinitions.OR, or, leaf);
            expected = "(" + expected + (i < 4 ? " AND " + leafSql : "") + ")";
        }
        assertRestriction(and, null, "(" + expected + ")");
        assertRestriction(or, null, null);
    }

    @Test
    void andSkipsAnOversizedMiddleChildAndStillUsesALaterSmallChild() {
        ResolvedExpression first = eq("id", DataTypes.BIGINT(), 1L);
        ResolvedExpression large = eq("name", DataTypes.STRING(), "x".repeat(100));
        ResolvedExpression last =
                call(BuiltInFunctionDefinitions.IS_NOT_NULL, field("payload", DataTypes.BYTES()));
        String expected = "(((`id` = 1) AND (`payload` IS NOT NULL)))";
        assertRestriction(
                call(BuiltInFunctionDefinitions.AND, first, large, last),
                explicitFor(expected),
                expected);
    }

    @Test
    void failedOrRestoresTheBudgetForTheNextTopLevelFilter() {
        ResolvedExpression large = eq("name", DataTypes.STRING(), "x".repeat(200_000));
        ResolvedExpression failed =
                call(
                        BuiltInFunctionDefinitions.OR,
                        large,
                        call(BuiltInFunctionDefinitions.NOT, large));
        ResolvedExpression successor = eq("name", DataTypes.STRING(), "y".repeat(200_000));
        String expected = "((`name` = '" + "y".repeat(200_000) + "'))";
        // The tentative large branch fits, but retaining its charge would exclude the successor.
        String explicit = explicitFor(expected).substring(16);
        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(ROW, Arrays.asList(failed, successor), explicit);
        assertThat(state.rowRestriction()).isEqualTo(expected);
        assertThat(state.result().getAcceptedFilters()).containsExactly(successor);
        assertThat(state.result().getRemainingFilters()).containsExactly(failed, successor);
    }

    @Test
    void failedOrRestoresTheBudgetInsideAnAnd() {
        ResolvedExpression small = eq("id", DataTypes.BIGINT(), 9L);
        ResolvedExpression failed =
                call(BuiltInFunctionDefinitions.OR, small, eq("name", DataTypes.STRING(), "long"));
        String expected = "(((`id` = 9)))";
        // The tentative OR needs its own two parentheses before its first child can fit.
        String explicit = explicitFor(expected).substring(2);
        assertRestriction(call(BuiltInFunctionDefinitions.AND, failed, small), explicit, expected);
    }

    @Test
    void orAcceptsPartialAndBranchesButDoesNotRebalanceEarlierBranches() {
        ResolvedExpression id = eq("id", DataTypes.BIGINT(), 1L);
        ResolvedExpression large = eq("name", DataTypes.STRING(), "x".repeat(100));
        ResolvedExpression unsupported = call(BuiltInFunctionDefinitions.NOT, id);
        ResolvedExpression nullId =
                call(BuiltInFunctionDefinitions.IS_NULL, field("id", DataTypes.BIGINT()));
        String expected = "((((`id` = 1)) OR (`id` IS NULL)))";
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(BuiltInFunctionDefinitions.AND, id, large, unsupported),
                        nullId),
                explicitFor(expected),
                expected);
        ResolvedExpression early = call(BuiltInFunctionDefinitions.AND, id, id);
        // Both early conjuncts fit before the final OR branch is visited; no search retries that
        // branch with fewer conjuncts after the last branch runs out of room.
        assertRestriction(
                call(BuiltInFunctionDefinitions.OR, early, nullId), explicitFor(expected), null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "é漢😀", "\u0001\n'\\", ""})
    void exactAndExceededCompoundBudgetsIncludeEscapedUtf8(String literal) {
        ResolvedExpression leaf = eq("name", DataTypes.STRING(), literal);
        ResolvedExpression expression = call(BuiltInFunctionDefinitions.OR, leaf, leaf);
        String expected =
                BigQueryFilterPushDown.translate(ROW, Collections.singletonList(expression), null)
                        .rowRestriction();
        String explicit = explicitFor(expected);
        assertRestriction(expression, explicit, expected);
        assertThat(utf8(BigQueryFilterPushDown.combinedRowRestriction(explicit, expected)))
                .isEqualTo(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        assertRestriction(expression, explicit + "x", null);
    }

    @Test
    void exactAndExceededByteBudgetsIncludeEveryHexEscape() {
        ResolvedExpression leaf = eq("payload", DataTypes.BYTES(), new byte[] {0, -1, 39, 92});
        ResolvedExpression expression = call(BuiltInFunctionDefinitions.OR, leaf, leaf);
        String leafSql = "(`payload` = b'\\x00\\xff\\x27\\x5c')";
        String expected = "((" + leafSql + " OR " + leafSql + "))";
        assertRestriction(expression, explicitFor(expected), expected);
        assertRestriction(expression, explicitFor(expected) + "x", null);
    }

    @Test
    void andAtTheBoundaryDropsOnlyTheLastChild() {
        ResolvedExpression first = eq("id", DataTypes.BIGINT(), 1L);
        ResolvedExpression second = eq("id", DataTypes.BIGINT(), 2L);
        String expected = "(((`id` = 1) AND (`id` = 2)))";
        ResolvedExpression expression = call(BuiltInFunctionDefinitions.AND, first, second);
        assertRestriction(expression, explicitFor(expected), expected);
        assertRestriction(expression, explicitFor(expected) + "x", "(((`id` = 1)))");
    }

    @Test
    void identifiersAndRawExplicitSqlUseTheirEscapedUtf8Sizes() {
        String name = "é漢😀`\\";
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD(name, DataTypes.BIGINT())).getLogicalType();
        ResolvedExpression leaf = eq(name, DataTypes.BIGINT(), 1L);
        ResolvedExpression expression = call(BuiltInFunctionDefinitions.OR, leaf, leaf);
        String part = "(`é漢😀\\`\\\\` = 1)";
        String expected = "((" + part + " OR " + part + "))";
        String prefix = "é漢😀\ud800";
        String explicit = prefix + explicitFor(expected).substring(utf8(prefix));
        assertThat(
                        BigQueryFilterPushDown.translate(
                                        row, Collections.singletonList(expression), explicit)
                                .rowRestriction())
                .isEqualTo(expected);
        assertThat(
                        BigQueryFilterPushDown.translate(
                                        row, Collections.singletonList(expression), explicit + "x")
                                .rowRestriction())
                .isNull();
    }

    @Test
    void enormousIdentifiersAndExhaustedExplicitBudgetsStayResidual() {
        String name = "`".repeat(600_000);
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD(name, DataTypes.BIGINT())).getLogicalType();
        ResolvedExpression leaf = eq(name, DataTypes.BIGINT(), 1L);
        assertThat(
                        BigQueryFilterPushDown.translate(row, Collections.singletonList(leaf), null)
                                .rowRestriction())
                .isNull();
        assertRestriction(
                eq("id", DataTypes.BIGINT(), 1L),
                "x".repeat(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES),
                null);
    }

    @Test
    void repeatedIdentifiersInScalarBoundsConsumeBudgetAtomically() {
        String name = "`".repeat(260_000);
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD(name, DataTypes.FLOAT())).getLogicalType();
        ResolvedExpression equals = eq(name, DataTypes.FLOAT(), 1.0f);
        // The quoted identifier fits once, but FLOAT equality needs both adjacent bounds.
        assertThat(
                        BigQueryFilterPushDown.translate(
                                        row, Collections.singletonList(equals), null)
                                .rowRestriction())
                .isNull();
    }

    private static void assertRestriction(
            ResolvedExpression expression, String explicit, String expected) {
        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(
                        ROW, Collections.singletonList(expression), explicit);
        assertThat(state.rowRestriction()).isEqualTo(expected);
        assertThat(state.result().getRemainingFilters()).containsExactly(expression);
        if (expected == null) {
            assertThat(state.result().getAcceptedFilters()).isEmpty();
        } else {
            assertThat(state.result().getAcceptedFilters()).containsExactly(expression);
            assertThat(utf8(BigQueryFilterPushDown.combinedRowRestriction(explicit, expected)))
                    .isLessThanOrEqualTo(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        }
    }

    private static String explicitFor(String expected) {
        return "x"
                .repeat(
                        BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES
                                - utf8(
                                        BigQueryFilterPushDown.combinedRowRestriction(
                                                "", expected)));
    }

    private static int utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static FieldReferenceExpression field(String name, DataType type) {
        return new FieldReferenceExpression(name, type, 0, 0);
    }

    private static ResolvedExpression eq(String name, DataType type, Object value) {
        return call(
                BuiltInFunctionDefinitions.EQUALS,
                field(name, type),
                new ValueLiteralExpression(value));
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }
}
