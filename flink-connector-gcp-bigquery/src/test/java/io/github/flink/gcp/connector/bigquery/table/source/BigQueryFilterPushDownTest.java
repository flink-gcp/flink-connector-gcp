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
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the conservative GoogleSQL row-restriction translation. */
class BigQueryFilterPushDownTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("count`\\value", DataTypes.BIGINT()),
                    DataTypes.FIELD("price", DataTypes.DECIMAL(38, 9)),
                    DataTypes.FIELD("day", DataTypes.DATE()),
                    DataTypes.FIELD("active", DataTypes.BOOLEAN()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("ratio", DataTypes.DOUBLE()),
                    DataTypes.FIELD("fixed", DataTypes.CHAR(4)),
                    DataTypes.FIELD(
                            "nested", DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING()))),
                    DataTypes.FIELD("single_ratio", DataTypes.FLOAT()),
                    DataTypes.FIELD("civil_time", DataTypes.TIMESTAMP(6)),
                    DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(6)),
                    DataTypes.FIELD("millis_time", DataTypes.TIMESTAMP(3)),
                    DataTypes.FIELD("clock_time", DataTypes.TIME(3)),
                    DataTypes.FIELD("payload", DataTypes.BYTES()));
    private static final RowType ROW_TYPE = (RowType) PHYSICAL.getLogicalType();

    @Test
    void translatesTheSupportedComparisonAndNullMatrix() {
        assertRestriction(equals(field(0), literal(7)), "((`count\\`\\\\value` = 7))");
        assertRestriction(
                call(BuiltInFunctionDefinitions.LESS_THAN, field(2), date("2026-08-29")),
                "((`day` < DATE '2026-08-29'))");
        assertRestriction(
                equals(field(3), new ValueLiteralExpression(true)), "((`active` = TRUE))");
        assertRestriction(call(BuiltInFunctionDefinitions.IS_NULL, field(4)), "((`name` IS NULL))");
        assertRestriction(
                call(BuiltInFunctionDefinitions.IS_NOT_NULL, field(5)), "((`ratio` IS NOT NULL))");
    }

    @Test
    void normalizesReversedOrderedComparisons() {
        assertRestriction(
                call(BuiltInFunctionDefinitions.LESS_THAN, literal(7), field(0)),
                "((`count\\`\\\\value` > 7))");
        assertRestriction(
                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, literal(7), field(0)),
                "((`count\\`\\\\value` <= 7))");
    }

    @Test
    void resolvesAProjectedFieldByNameRatherThanItsCurrentRowIndex() {
        FieldReferenceExpression reordered =
                new FieldReferenceExpression("active", PHYSICAL.getChildren().get(3), 0, 0);
        FieldReferenceExpression unknown =
                new FieldReferenceExpression("unknown", PHYSICAL.getChildren().get(0), 0, 0);
        ResolvedExpression unknownFilter =
                equals(unknown, new ValueLiteralExpression(BigDecimal.ONE));

        assertRestriction(
                equals(reordered, new ValueLiteralExpression(true)), "((`active` = TRUE))");
        BigQueryFilterPushDown.State unknownState = translate(unknownFilter);
        assertResult(
                unknownState.result(),
                Collections.emptyList(),
                Collections.singletonList(unknownFilter));
        assertThat(unknownState.rowRestriction()).isNull();
    }

    @Test
    void aConjunctionUsesEverySafeNecessaryChild() {
        ResolvedExpression supported = equals(field(0), literal(7));
        ResolvedExpression unsupported =
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        field(6),
                        new ValueLiteralExpression("a"));
        ResolvedExpression conjunction =
                call(BuiltInFunctionDefinitions.AND, supported, unsupported);

        BigQueryFilterPushDown.State state = translate(conjunction);

        assertResult(
                state.result(),
                Collections.singletonList(conjunction),
                Collections.singletonList(conjunction));
        assertThat(state.rowRestriction()).isEqualTo("(((`count\\`\\\\value` = 7)))");
    }

    @Test
    void aDisjunctionRequiresEveryBranch() {
        ResolvedExpression first = equals(field(0), literal(1));
        ResolvedExpression second = equals(field(0), literal(2));
        ResolvedExpression exact = call(BuiltInFunctionDefinitions.OR, first, second);
        ResolvedExpression partial =
                call(
                        BuiltInFunctionDefinitions.OR,
                        first,
                        call(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                field(6),
                                new ValueLiteralExpression("a")));

        assertRestriction(exact, "(((`count\\`\\\\value` = 1) OR (`count\\`\\\\value` = 2)))");
        BigQueryFilterPushDown.State unsupported = translate(partial);
        assertResult(
                unsupported.result(), Collections.emptyList(), Collections.singletonList(partial));
        assertThat(unsupported.rowRestriction()).isNull();
    }

    @Test
    void translatesStringEqualityAndEscapesGoogleSqlLiterals() {
        assertRestriction(
                equals(field(4), new ValueLiteralExpression("O'Reilly\\line\n\t\u0001😀")),
                "((`name` = 'O\\'Reilly\\\\line\\n\\t\\u0001😀'))");
    }

    @Test
    void translatesDecimalComparisonsAsRoundingSafeNecessaryConditions() {
        assertRestriction(
                equals(field(1), decimal("7.250000000")),
                "(((`price` >= BIGNUMERIC '7.249999999') AND (`price` <= BIGNUMERIC '7.250000001')))");
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                        field(1),
                        decimal("7.250000000")),
                "((`price` < BIGNUMERIC '7.250000001'))");
        assertRestriction(
                call(BuiltInFunctionDefinitions.GREATER_THAN, decimal("7.250000000"), field(1)),
                "((`price` < BIGNUMERIC '7.250000001'))");
        assertRestriction(
                call(BuiltInFunctionDefinitions.NOT_EQUALS, field(1), decimal("7.250000000")),
                "((`price` <> BIGNUMERIC '7.250000000'))");
        assertRestriction(
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(1), decimal("7.250000000")),
                "((`price` > BIGNUMERIC '7.249999999'))");
    }

    @Test
    void translatesDoubleDirectlyAndFloatWithRoundingSafeBounds() {
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        field(5),
                        new ValueLiteralExpression(1.25d)),
                "((`ratio` > 1.25))");
        assertRestriction(
                equals(field(8), new ValueLiteralExpression(1.0f)),
                "(((`single_ratio` > 0.9999999403953552) AND (`single_ratio` < 1.0000001192092896)))");
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                        field(8),
                        new ValueLiteralExpression(1.0f)),
                "((`single_ratio` < 1.0000001192092896))");
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.NOT_EQUALS,
                        field(8),
                        new ValueLiteralExpression(1.0f)),
                "((`single_ratio` <> 1.0))");
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        field(8),
                        new ValueLiteralExpression(1.0f)),
                "((`single_ratio` > 0.9999999403953552))");
    }

    @Test
    void translatesMicrosecondTimestampComparisons() {
        assertRestriction(
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        field(9),
                        new ValueLiteralExpression(
                                LocalDateTime.parse("2026-08-29T12:34:56.123456"))),
                "((`civil_time` >= DATETIME '2026-08-29 12:34:56.123456'))");
        assertRestriction(
                equals(
                        field(10),
                        new ValueLiteralExpression(
                                Instant.parse("2026-08-29T03:34:56.123456Z"),
                                DataTypes.TIMESTAMP_LTZ(6).notNull())),
                "((`event_time` = TIMESTAMP '2026-08-29 03:34:56.123456Z'))");
    }

    @Test
    void unsupportedTypesShapesAndNullComparisonsRemainOnlyWithFlink() {
        ResolvedExpression charComparison = equals(field(6), new ValueLiteralExpression("abcd"));
        ResolvedExpression nestedComparison =
                equals(
                        new NestedFieldReferenceExpression(
                                new String[] {"nested", "value"},
                                new int[] {7, 0},
                                DataTypes.STRING()),
                        new ValueLiteralExpression("value"));
        ResolvedExpression functionComparison =
                equals(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.UPPER,
                                Collections.singletonList(field(4)),
                                DataTypes.STRING()),
                        new ValueLiteralExpression("ALICE"));
        ResolvedExpression nullComparison =
                equals(field(0), new ValueLiteralExpression(null, DataTypes.BIGINT()));
        ResolvedExpression complexNullCheck = call(BuiltInFunctionDefinitions.IS_NULL, field(7));
        ResolvedExpression charNullCheck = call(BuiltInFunctionDefinitions.IS_NULL, field(6));
        ResolvedExpression millisecondTimestampNullCheck =
                call(BuiltInFunctionDefinitions.IS_NOT_NULL, field(11));
        ResolvedExpression timeNullCheck = call(BuiltInFunctionDefinitions.IS_NULL, field(12));
        ResolvedExpression bytesNullCheck = call(BuiltInFunctionDefinitions.IS_NOT_NULL, field(13));
        ResolvedExpression millisecondTimestamp =
                equals(
                        field(11),
                        new ValueLiteralExpression(
                                LocalDateTime.parse("2026-08-29T12:34:56.123"),
                                DataTypes.TIMESTAMP(3).notNull()));
        ResolvedExpression nonFiniteDouble =
                equals(field(5), new ValueLiteralExpression(Double.NaN));
        ResolvedExpression malformedUnicode =
                equals(field(4), new ValueLiteralExpression("\ud800"));
        ResolvedExpression stringInequality =
                call(
                        BuiltInFunctionDefinitions.NOT_EQUALS,
                        field(4),
                        new ValueLiteralExpression("alice"));
        ResolvedExpression timeComparison =
                equals(
                        field(12),
                        new ValueLiteralExpression(
                                LocalTime.parse("12:34:56.123"), DataTypes.TIME(3).notNull()));
        ResolvedExpression bytesComparison =
                equals(
                        field(13),
                        new ValueLiteralExpression(
                                new byte[] {1, 2, 3}, DataTypes.BYTES().notNull()));
        ResolvedExpression outsideDatetimeRange =
                equals(
                        field(9),
                        new ValueLiteralExpression(
                                LocalDateTime.of(10_000, 1, 1, 0, 0),
                                DataTypes.TIMESTAMP(6).notNull()));
        ResolvedExpression outsideDateRange =
                equals(
                        field(2),
                        new ValueLiteralExpression(
                                LocalDate.of(0, 1, 1), DataTypes.DATE().notNull()));
        ResolvedExpression outsideTimestampRange =
                equals(
                        field(10),
                        new ValueLiteralExpression(
                                Instant.MAX, DataTypes.TIMESTAMP_LTZ(6).notNull()));

        List<ResolvedExpression> filters =
                Arrays.asList(
                        charComparison,
                        nestedComparison,
                        functionComparison,
                        nullComparison,
                        complexNullCheck,
                        charNullCheck,
                        millisecondTimestampNullCheck,
                        timeNullCheck,
                        bytesNullCheck,
                        millisecondTimestamp,
                        nonFiniteDouble,
                        malformedUnicode,
                        stringInequality,
                        timeComparison,
                        bytesComparison,
                        outsideDateRange,
                        outsideDatetimeRange,
                        outsideTimestampRange);
        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(ROW_TYPE, filters, null);

        assertResult(state.result(), Collections.emptyList(), filters);
        assertThat(state.rowRestriction()).isNull();
    }

    @Test
    void allAcceptedFiltersAlsoRemainAsFlinkResiduals() {
        ResolvedExpression active = equals(field(3), new ValueLiteralExpression(true));
        ResolvedExpression count =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(0), literal(3));

        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(ROW_TYPE, Arrays.asList(active, count), null);

        assertResult(state.result(), Arrays.asList(active, count), Arrays.asList(active, count));
        assertThat(state.rowRestriction())
                .isEqualTo("((`active` = TRUE) AND (`count\\`\\\\value` > 3))");
    }

    @Test
    void anOversizedGeneratedRestrictionRemainsOnlyWithFlink() {
        String fieldName = "x".repeat(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        DataType physical = DataTypes.ROW(DataTypes.FIELD(fieldName, DataTypes.BIGINT()));
        RowType rowType = (RowType) physical.getLogicalType();
        ResolvedExpression filter =
                equals(
                        new FieldReferenceExpression(
                                fieldName, physical.getChildren().get(0), 0, 0),
                        literal(7));

        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(rowType, Collections.singletonList(filter), null);

        assertResult(state.result(), Collections.emptyList(), Collections.singletonList(filter));
        assertThat(state.rowRestriction()).isNull();
    }

    @Test
    void aFittingRestrictionIsRetainedAfterAnOversizedRestriction() {
        String fieldName = "x".repeat(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES);
        DataType physical =
                DataTypes.ROW(
                        DataTypes.FIELD(fieldName, DataTypes.BIGINT()),
                        DataTypes.FIELD("id", DataTypes.BIGINT()));
        RowType rowType = (RowType) physical.getLogicalType();
        ResolvedExpression oversized =
                equals(
                        new FieldReferenceExpression(
                                fieldName, physical.getChildren().get(0), 0, 0),
                        literal(7));
        ResolvedExpression fitting =
                equals(
                        new FieldReferenceExpression("id", physical.getChildren().get(1), 0, 1),
                        literal(7));

        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(rowType, Arrays.asList(oversized, fitting), null);

        assertResult(
                state.result(),
                Collections.singletonList(fitting),
                Arrays.asList(oversized, fitting));
        assertThat(state.rowRestriction()).isEqualTo("((`id` = 7))");
    }

    @Test
    void aMultibyteExplicitRestrictionConsumesTheUtf8SizeBudget() {
        ResolvedExpression filter = equals(field(0), literal(7));
        String explicit = "é".repeat(BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES / 2);

        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(
                        ROW_TYPE, Collections.singletonList(filter), explicit);

        assertResult(state.result(), Collections.emptyList(), Collections.singletonList(filter));
        assertThat(state.rowRestriction()).isNull();
    }

    @Test
    void generatedRestrictionsShareOneCumulativeSizeBudget() {
        ResolvedExpression first = equals(field(3), new ValueLiteralExpression(false));
        ResolvedExpression second = equals(field(3), new ValueLiteralExpression(true));
        String expectedFirst = "((`active` = FALSE))";
        int wrapperBytes =
                BigQueryFilterPushDown.combinedRowRestriction("", "")
                        .getBytes(StandardCharsets.UTF_8)
                        .length;
        String explicit =
                "x"
                        .repeat(
                                BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES
                                        - wrapperBytes
                                        - expectedFirst.getBytes(StandardCharsets.UTF_8).length);

        BigQueryFilterPushDown.State state =
                BigQueryFilterPushDown.translate(ROW_TYPE, Arrays.asList(first, second), explicit);

        assertResult(
                state.result(), Collections.singletonList(first), Arrays.asList(first, second));
        assertThat(state.rowRestriction()).isEqualTo(expectedFirst);
    }

    private static BigQueryFilterPushDown.State translate(ResolvedExpression filter) {
        return BigQueryFilterPushDown.translate(ROW_TYPE, Collections.singletonList(filter), null);
    }

    private static void assertRestriction(ResolvedExpression filter, String expected) {
        BigQueryFilterPushDown.State state = translate(filter);
        assertResult(
                state.result(),
                Collections.singletonList(filter),
                Collections.singletonList(filter));
        assertThat(state.rowRestriction()).isEqualTo(expected);
    }

    private static void assertResult(
            SupportsFilterPushDown.Result result,
            List<ResolvedExpression> accepted,
            List<ResolvedExpression> remaining) {
        assertThat(result.getAcceptedFilters()).containsExactlyElementsOf(accepted);
        assertThat(result.getRemainingFilters()).containsExactlyElementsOf(remaining);
    }

    private static FieldReferenceExpression field(int index) {
        return new FieldReferenceExpression(
                ROW_TYPE.getFieldNames().get(index),
                PHYSICAL.getChildren().get(index),
                index,
                index);
    }

    private static ValueLiteralExpression literal(long value) {
        return new ValueLiteralExpression(BigDecimal.valueOf(value));
    }

    private static ValueLiteralExpression decimal(String value) {
        return new ValueLiteralExpression(new BigDecimal(value));
    }

    private static ValueLiteralExpression date(String value) {
        return new ValueLiteralExpression(LocalDate.parse(value));
    }

    private static CallExpression equals(ResolvedExpression left, ResolvedExpression right) {
        return call(BuiltInFunctionDefinitions.EQUALS, left, right);
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }
}
