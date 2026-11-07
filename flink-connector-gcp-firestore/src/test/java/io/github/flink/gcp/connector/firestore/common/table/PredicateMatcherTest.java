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

package io.github.flink.gcp.connector.firestore.common.table;

import org.apache.flink.table.api.DataTypes;
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests which predicates yield comparisons, and what each comparison carries. */
class PredicateMatcherTest {

    private static final RowType ROW =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("s", DataTypes.STRING()),
                                    DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                                    DataTypes.FIELD(
                                            "e",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("x", DataTypes.BIGINT()),
                                                    DataTypes.FIELD(
                                                            "f",
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "g",
                                                                            DataTypes
                                                                                    .BOOLEAN()))))),
                                    DataTypes.FIELD(
                                            "loose",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(
                                                            "a", DataTypes.BIGINT().notNull()))),
                                    DataTypes.FIELD(
                                            "held",
                                            DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "a",
                                                                    DataTypes.BIGINT().notNull()))
                                                    .notNull()))
                            .getLogicalType();

    /**
     * Describes a comparison as its joined path, whether it is column 0 itself, its operator and
     * its value, and a disjunction as its branches joined by {@code |}.
     */
    private static final PredicateMatcher.Translator<String> DESCRIBE =
            translator(comparison -> List.of(describe(comparison)));

    private static String describe(PredicateMatcher.Comparison comparison) {
        String operator;
        switch (comparison.operator()) {
            case EQUAL:
                operator = "=";
                break;
            case NOT_EQUAL:
                operator = "<>";
                break;
            case LESS_THAN:
                operator = "<";
                break;
            case LESS_THAN_OR_EQUAL:
                operator = "<=";
                break;
            case GREATER_THAN:
                operator = ">";
                break;
            case GREATER_THAN_OR_EQUAL:
                operator = ">=";
                break;
            default:
                operator = " IS NOT NULL";
        }
        return comparison.joinedPath(comparison.path().size())
                + (comparison.isColumn(0) ? "@0" : "")
                + operator
                + comparison
                        .valueAs(Object.class)
                        .or(() -> comparison.valueAs(Boolean.class))
                        .map(String::valueOf)
                        .orElse("");
    }

    private static PredicateMatcher.Translator<String> translator(
            Function<PredicateMatcher.Comparison, List<String>> translate) {
        return new PredicateMatcher.Translator<String>() {
            @Override
            public List<String> translate(PredicateMatcher.Comparison comparison) {
                return translate.apply(comparison);
            }

            @Override
            public String anyOf(List<String> branches) {
                return String.join("|", branches);
            }

            @Override
            public int width(String filter) {
                // A filter described with a leading "2x" stands for an IN of two values.
                return filter.startsWith("2x") ? 2 : filter.split("\\|").length;
            }
        };
    }

    private static List<String> translate(ResolvedExpression... filters) {
        return PredicateMatcher.translate(ROW, List.of(filters), DESCRIBE).filters();
    }

    private static FieldReferenceExpression column(String name, DataType type, int index) {
        return new FieldReferenceExpression(name, type, 0, index);
    }

    private static NestedFieldReferenceExpression nested(DataType type, String... names) {
        int[] indices = new int[names.length];
        RowType row = ROW;
        for (int i = 0; i < names.length; i++) {
            indices[i] = row.getFieldNames().indexOf(names[i]);
            if (i + 1 < names.length) {
                row = (RowType) row.getTypeAt(indices[i]);
            }
        }
        return new NestedFieldReferenceExpression(names, indices, type);
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, List.of(children), DataTypes.BOOLEAN());
    }

    @Test
    void anEqualityMatchesEitherWayRound() {
        ValueLiteralExpression x = new ValueLiteralExpression("x");
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);

        assertThat(translate(call(BuiltInFunctionDefinitions.EQUALS, s, x)))
                .containsExactly("s@0=x");
        assertThat(translate(call(BuiltInFunctionDefinitions.EQUALS, x, s)))
                .containsExactly("s@0=x");
    }

    @Test
    void aBareBooleanAndItsNegationMatchTrueAndFalseAtAnyDepth() {
        FieldReferenceExpression b = column("b", DataTypes.BOOLEAN(), 1);
        NestedFieldReferenceExpression g = nested(DataTypes.BOOLEAN(), "e", "f", "g");

        assertThat(translate(b, call(BuiltInFunctionDefinitions.NOT, g)))
                .containsExactly("b=true", "e.f.g=false");
    }

    @Test
    void aConjunctionYieldsEveryConjunctAndADisjunctionOfTwoReferencesNone() {
        ResolvedExpression equality =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        nested(DataTypes.BIGINT(), "e", "x"),
                        new ValueLiteralExpression(5L));
        FieldReferenceExpression b = column("b", DataTypes.BOOLEAN(), 1);

        PushedFilters<String> and =
                PredicateMatcher.translate(
                        ROW, List.of(call(BuiltInFunctionDefinitions.AND, equality, b)), DESCRIBE);
        PushedFilters<String> or =
                PredicateMatcher.translate(
                        ROW, List.of(call(BuiltInFunctionDefinitions.OR, equality, b)), DESCRIBE);

        assertThat(and.filters()).containsExactly("e.x=5", "b=true");
        assertThat(and.result().getAcceptedFilters()).hasSize(1);
        assertThat(or.filters()).isEmpty();
        assertThat(or.result().getAcceptedFilters()).isEmpty();
        assertThat(or.result().getRemainingFilters()).hasSize(1);
    }

    @Test
    void aNullLiteralAndAComparisonOfTwoReferencesYieldNothing() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);

        assertThat(
                        translate(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        s,
                                        new ValueLiteralExpression(null, DataTypes.STRING())),
                                call(BuiltInFunctionDefinitions.EQUALS, s, s)))
                .isEmpty();
    }

    @Test
    void aNotNullFieldIsResolvedOnlyWhereNoNullableRowLiesAboveIt() {
        // Flink takes loose.a to be never NULL, although a NULL loose makes it so.
        assertThat(
                        translate(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        nested(DataTypes.BIGINT().notNull(), "loose", "a"),
                                        new ValueLiteralExpression(1L)),
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        nested(DataTypes.BIGINT().notNull(), "held", "a"),
                                        new ValueLiteralExpression(1L))))
                .containsExactly("held.a=1");
    }

    @Test
    void theTranslatorDecidesAndEveryPredicateStaysResidual() {
        ResolvedExpression equality =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        column("s", DataTypes.STRING(), 0),
                        new ValueLiteralExpression("x"));

        PushedFilters<String> declined =
                PredicateMatcher.translate(
                        ROW, List.of(equality), translator(anything -> List.of()));
        PushedFilters<String> accepted =
                PredicateMatcher.translate(ROW, List.of(equality), DESCRIBE);

        assertThat(declined).isEqualTo(PushedFilters.declined(List.of(equality)));
        assertThat(accepted.result().getAcceptedFilters()).containsExactly(equality);
        assertThat(accepted.result().getRemainingFilters()).containsExactly(equality);
    }

    @Test
    void negationAndABareReferenceYieldAnEqualityOnlyOverABooleanReference() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);
        ResolvedExpression equality =
                call(BuiltInFunctionDefinitions.EQUALS, s, new ValueLiteralExpression("x"));

        assertThat(
                        translate(
                                s,
                                call(BuiltInFunctionDefinitions.NOT, s),
                                call(BuiltInFunctionDefinitions.NOT, equality),
                                nested(DataTypes.BIGINT(), "e", "x")))
                .isEmpty();
    }

    @Test
    void aReferenceTheRowTypeDoesNotDeclareYieldsNothing() {
        assertThat(
                        translate(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        column("absent", DataTypes.STRING(), 9),
                                        new ValueLiteralExpression("x"))))
                .isEmpty();
    }

    @Test
    void aNestedFieldIsNotItsColumnAndTheLiteralsNumericFormsAreExact() {
        List<PredicateMatcher.Comparison> seen = new ArrayList<>();
        PredicateMatcher.translate(
                ROW,
                List.of(
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                nested(DataTypes.BIGINT(), "e", "x"),
                                new ValueLiteralExpression(new BigDecimal("5.0"))),
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                column("s", DataTypes.STRING(), 0),
                                new ValueLiteralExpression(new BigDecimal("5.5")))),
                translator(
                        comparison -> {
                            seen.add(comparison);
                            return List.of();
                        }));

        assertThat(seen).hasSize(2);
        assertThat(seen.get(0).isColumn(2)).isFalse();
        assertThat(seen.get(0).longValue()).contains(5L);
        assertThat(seen.get(1).isColumn(0)).isTrue();
        assertThat(seen.get(1).longValue()).isEmpty();
        assertThat(seen.get(1).doubleValue()).contains(5.5d);
    }

    @Test
    void everyComparisonOperatorMatchesWithTheReferenceOnTheLeft() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);
        ValueLiteralExpression x = new ValueLiteralExpression("x");

        assertThat(
                        translate(
                                call(BuiltInFunctionDefinitions.NOT_EQUALS, s, x),
                                call(BuiltInFunctionDefinitions.LESS_THAN, s, x),
                                call(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, x, s),
                                call(BuiltInFunctionDefinitions.GREATER_THAN, x, s),
                                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, s, x)))
                .containsExactly(
                        "s@0<>x",
                        "s@0<x",
                        // x <= s is s >= x, and x > s is s < x.
                        "s@0>=x",
                        "s@0<x",
                        "s@0>=x");
        // IS NOT NULL takes the not-equal filter <> would, so each is searched alone.
        assertThat(translate(call(BuiltInFunctionDefinitions.IS_NOT_NULL, s)))
                .containsExactly("s@0 IS NOT NULL");
        assertThat(
                        translate(
                                call(
                                        BuiltInFunctionDefinitions.IS_NOT_NULL,
                                        nested(DataTypes.BIGINT(), "e", "x"))))
                .containsExactly("e.x IS NOT NULL");
    }

    @Test
    void aDisjunctionOfEqualitiesOnOneReferenceYieldsOneFilter() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);
        ResolvedExpression in =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(
                                BuiltInFunctionDefinitions.OR,
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        s,
                                        new ValueLiteralExpression("a")),
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        new ValueLiteralExpression("b"),
                                        s)),
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                s,
                                new ValueLiteralExpression("c")));
        ResolvedExpression range =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(BuiltInFunctionDefinitions.EQUALS, s, new ValueLiteralExpression("a")),
                        call(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                s,
                                new ValueLiteralExpression("b")));

        PushedFilters<String> pushed =
                PredicateMatcher.translate(ROW, List.of(in, range), DESCRIBE);

        assertThat(pushed.filters()).containsExactly("s@0=a|s@0=b|s@0=c");
        assertThat(pushed.result().getAcceptedFilters()).containsExactly(in);
        assertThat(pushed.result().getRemainingFilters()).containsExactly(in, range);
    }

    @Test
    void aDisjunctionWithABranchTheTranslatorDeclinesYieldsNothing() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);
        ResolvedExpression in =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(BuiltInFunctionDefinitions.EQUALS, s, new ValueLiteralExpression("a")),
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                s,
                                new ValueLiteralExpression("b")));

        assertThat(
                        PredicateMatcher.translate(
                                        ROW,
                                        List.of(in),
                                        translator(
                                                comparison ->
                                                        comparison
                                                                        .valueAs(String.class)
                                                                        .orElse("")
                                                                        .equals("b")
                                                                ? List.of()
                                                                : List.of(describe(comparison))))
                                .filters())
                .isEmpty();
    }

    @Test
    void onlyTheFirstNotEqualTranslates() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);

        assertThat(
                        translate(
                                call(
                                        BuiltInFunctionDefinitions.NOT_EQUALS,
                                        s,
                                        new ValueLiteralExpression("a")),
                                call(
                                        BuiltInFunctionDefinitions.NOT_EQUALS,
                                        s,
                                        new ValueLiteralExpression("b"))))
                .containsExactly("s@0<>a");
    }

    @Test
    void disjunctionsTranslateWhileTheirProductStaysWithinTheLimit() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);
        NestedFieldReferenceExpression x = nested(DataTypes.BIGINT(), "e", "x");
        ResolvedExpression five = anyOf(s, 5);
        ResolvedExpression six = anyOf(x, 6);
        ResolvedExpression thirty = anyOf(s, PredicateMatcher.MAX_DISJUNCTS);
        ResolvedExpression thirtyOne = anyOf(s, PredicateMatcher.MAX_DISJUNCTS + 1);

        // 5 x 6 = 30 fits, and a further disjunction of 2 would make 60.
        assertThat(
                        PredicateMatcher.translate(ROW, List.of(five, six, anyOf(s, 2)), DESCRIBE)
                                .filters())
                .hasSize(2);
        assertThat(PredicateMatcher.translate(ROW, List.of(thirty), DESCRIBE).filters()).hasSize(1);
        assertThat(PredicateMatcher.translate(ROW, List.of(thirtyOne), DESCRIBE).filters())
                .isEmpty();
    }

    /** Returns {@code reference = 0 OR ... OR reference = count - 1}, as a left-deep tree. */
    private static ResolvedExpression anyOf(ResolvedExpression reference, int count) {
        ResolvedExpression tree = null;
        for (int i = 0; i < count; i++) {
            ResolvedExpression equality =
                    call(
                            BuiltInFunctionDefinitions.EQUALS,
                            reference,
                            new ValueLiteralExpression(
                                    reference instanceof FieldReferenceExpression
                                            ? (Object) String.valueOf(i)
                                            : (Object) (long) i));
            tree = tree == null ? equality : call(BuiltInFunctionDefinitions.OR, tree, equality);
        }
        return tree;
    }

    @Test
    void anIntegerLiteralHasADoubleForm() {
        List<PredicateMatcher.Comparison> seen = new ArrayList<>();
        PredicateMatcher.translate(
                ROW,
                List.of(
                        call(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                column("s", DataTypes.STRING(), 0),
                                new ValueLiteralExpression(5))),
                translator(
                        comparison -> {
                            seen.add(comparison);
                            return List.of();
                        }));

        assertThat(seen.get(0).doubleValue()).contains(5.0d);
        assertThat(seen.get(0).longValue()).contains(5L);
    }

    @Test
    void anIsNotNullTakesTheQuerysOneNotEqualFilter() {
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);

        assertThat(
                        translate(
                                call(BuiltInFunctionDefinitions.IS_NOT_NULL, s),
                                call(
                                        BuiltInFunctionDefinitions.NOT_EQUALS,
                                        s,
                                        new ValueLiteralExpression("a")),
                                call(
                                        BuiltInFunctionDefinitions.IS_NOT_NULL,
                                        nested(DataTypes.BIGINT(), "e", "x"))))
                .containsExactly("s@0 IS NOT NULL");
    }

    @Test
    void aFilterWiderThanOneDisjunctCountsItsWidthTowardsTheLimit() {
        // Each comparison translates to a filter of width two, as an IN of two values would.
        PredicateMatcher.Translator<String> wide =
                translator(comparison -> List.of("2x" + describe(comparison)));
        List<ResolvedExpression> five = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            five.add(
                    call(
                            BuiltInFunctionDefinitions.EQUALS,
                            column("s", DataTypes.STRING(), 0),
                            new ValueLiteralExpression("v" + i)));
        }
        FieldReferenceExpression s = column("s", DataTypes.STRING(), 0);

        // 2^4 = 16 fits; a fifth would make 32.
        assertThat(PredicateMatcher.translate(ROW, five, wide).filters()).hasSize(4);
        // A disjunction of 15 such equalities is 30 disjuncts, one of 16 is 32.
        assertThat(PredicateMatcher.translate(ROW, List.of(anyOf(s, 15)), wide).filters())
                .hasSize(1);
        assertThat(PredicateMatcher.translate(ROW, List.of(anyOf(s, 16)), wide).filters())
                .isEmpty();
    }
}
