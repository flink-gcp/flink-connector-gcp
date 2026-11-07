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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests which predicates yield equalities, and what each equality carries. */
class EqualityMatcherTest {

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

    /** Describes an equality as its joined path, whether it is column 0 itself, and its value. */
    private static final EqualityMatcher.Translator<String> DESCRIBE =
            equality ->
                    Optional.of(
                            equality.joinedPath(equality.path().size())
                                    + (equality.isColumn(0) ? "@0" : "")
                                    + "="
                                    + equality.valueAs(Object.class)
                                            .or(() -> equality.valueAs(Boolean.class))
                                            .orElse(null));

    private static List<String> translate(ResolvedExpression... filters) {
        return EqualityMatcher.translate(ROW, List.of(filters), DESCRIBE).filters();
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
    void aConjunctionYieldsEveryConjunctAndADisjunctionNone() {
        ResolvedExpression equality =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        nested(DataTypes.BIGINT(), "e", "x"),
                        new ValueLiteralExpression(5L));
        FieldReferenceExpression b = column("b", DataTypes.BOOLEAN(), 1);

        PushedFilters<String> and =
                EqualityMatcher.translate(
                        ROW, List.of(call(BuiltInFunctionDefinitions.AND, equality, b)), DESCRIBE);
        PushedFilters<String> or =
                EqualityMatcher.translate(
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
                EqualityMatcher.translate(ROW, List.of(equality), anything -> Optional.empty());
        PushedFilters<String> accepted =
                EqualityMatcher.translate(ROW, List.of(equality), DESCRIBE);

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
        List<EqualityMatcher.Equality> seen = new ArrayList<>();
        EqualityMatcher.translate(
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
                equality -> {
                    seen.add(equality);
                    return Optional.empty();
                });

        assertThat(seen).hasSize(2);
        assertThat(seen.get(0).isColumn(2)).isFalse();
        assertThat(seen.get(0).longValue()).contains(5L);
        assertThat(seen.get(1).isColumn(0)).isTrue();
        assertThat(seen.get(1).longValue()).isEmpty();
        assertThat(seen.get(1).doubleValue()).contains(5.5d);
    }
}
