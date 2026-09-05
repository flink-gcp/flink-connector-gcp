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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerFloatFilterPushDownTest {

    private static final FieldReferenceExpression FIELD =
            new FieldReferenceExpression("ratio", DataTypes.DOUBLE().notNull(), 0, 0);
    private static final List<KeyColumn> ASC = List.of(new KeyColumn("ratio", 0, false, false));
    private static final List<KeyColumn> DESC = List.of(new KeyColumn("ratio", 0, true, false));
    private static final List<BuiltInFunctionDefinition> OPERATORS =
            List.of(
                    BuiltInFunctionDefinitions.LESS_THAN,
                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                    BuiltInFunctionDefinitions.GREATER_THAN,
                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                    BuiltInFunctionDefinitions.EQUALS);
    private static final double[] VALUES = {
        Double.NEGATIVE_INFINITY,
        -Double.MAX_VALUE,
        -1.0d,
        -Double.MIN_NORMAL,
        -Double.MIN_VALUE,
        -0.0d,
        0.0d,
        Double.MIN_VALUE,
        Double.MIN_NORMAL,
        1.0d,
        Math.nextUp(1.0d),
        Double.MAX_VALUE,
        Double.POSITIVE_INFINITY
    };

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void acceptedComparisonsAndTheirReversalsMatchPrimitiveDoubleSemantics(Dialect dialect) {
        for (BuiltInFunctionDefinition operator : OPERATORS) {
            for (double bound : VALUES) {
                for (boolean reversed : List.of(false, true)) {
                    ResolvedExpression predicate = predicate(operator, bound, reversed);
                    SpannerFilterPushDown.State state = translate(dialect, false, predicate);
                    assertThat(state.result().getAcceptedFilters()).containsExactly(predicate);
                    assertThat(state.result().getRemainingFilters()).isEmpty();
                    for (double candidate : VALUES) {
                        boolean expected =
                                reversed
                                        ? evaluate(operator, bound, candidate)
                                        : evaluate(operator, candidate, bound);
                        assertThat(state.runtime().matchesPrimaryKey(Key.of(candidate)))
                                .as(
                                        "%s: %s, reversed=%s, candidate=%s",
                                        operator, bound, reversed, candidate)
                                .isEqualTo(expected);
                    }
                    assertThat(state.runtime().matchesPrimaryKey(Key.of(Double.NaN))).isFalse();
                }
            }
        }
    }

    @Test
    void anUnconvertedDecimalLiteralRemainsResidual() {
        ResolvedExpression predicate =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.LESS_THAN,
                        List.of(FIELD, new ValueLiteralExpression(new BigDecimal("0.0"))),
                        DataTypes.BOOLEAN());
        SpannerFilterPushDown.State state =
                translate(Dialect.GOOGLE_STANDARD_SQL, false, predicate);
        assertThat(state.result().getAcceptedFilters()).isEmpty();
        assertThat(state.result().getRemainingFilters()).containsExactly(predicate);
        assertThat(state.keySet(ASC)).isNull();
    }

    @Test
    void oneSidedRangesExcludeNanAndNullAndRespectPhysicalDirection() {
        SpannerFilterPushDown.State below =
                translate(
                        Dialect.GOOGLE_STANDARD_SQL,
                        false,
                        predicate(BuiltInFunctionDefinitions.LESS_THAN, 1.0d, false));
        SpannerFilterPushDown.State above =
                translate(
                        Dialect.GOOGLE_STANDARD_SQL,
                        false,
                        predicate(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, -1.0d, false));

        assertThat(below.keySet(ASC).getRanges())
                .containsExactly(
                        KeyRange.closedOpen(Key.of(Double.NEGATIVE_INFINITY), Key.of(1.0d)));
        assertThat(below.keySet(DESC).getRanges())
                .containsExactly(
                        KeyRange.openClosed(Key.of(1.0d), Key.of(Double.NEGATIVE_INFINITY)));
        assertThat(above.keySet(ASC).getRanges())
                .containsExactly(
                        KeyRange.closedClosed(Key.of(-1.0d), Key.of(Double.POSITIVE_INFINITY)));
        assertThat(above.keySet(DESC).getRanges())
                .containsExactly(
                        KeyRange.closedClosed(Key.of(Double.POSITIVE_INFINITY), Key.of(-1.0d)));
        assertThat(
                        below.runtime()
                                .matchesPrimaryKey(Key.newBuilder().append((Double) null).build()))
                .isFalse();
        assertThat(above.directionIndependentPrimaryKeySet(ASC)).isNull();
    }

    @Test
    void contradictionsIncludeSignedZeroAndInfiniteDomainEndpoints() {
        for (List<ResolvedExpression> predicates :
                List.of(
                        List.of(
                                predicate(
                                        BuiltInFunctionDefinitions.GREATER_THAN,
                                        Double.POSITIVE_INFINITY,
                                        false)),
                        List.of(
                                predicate(
                                        BuiltInFunctionDefinitions.LESS_THAN,
                                        Double.NEGATIVE_INFINITY,
                                        false)),
                        List.of(
                                predicate(BuiltInFunctionDefinitions.GREATER_THAN, -0.0d, false),
                                predicate(
                                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                        0.0d,
                                        false)),
                        List.of(
                                predicate(
                                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                        0.0d,
                                        false),
                                predicate(BuiltInFunctionDefinitions.LESS_THAN, -0.0d, false)),
                        List.of(
                                predicate(
                                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                        1.0d,
                                        false),
                                predicate(
                                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                        -1.0d,
                                        false)),
                        List.of(
                                predicate(BuiltInFunctionDefinitions.EQUALS, -0.0d, false),
                                predicate(BuiltInFunctionDefinitions.GREATER_THAN, 0.0d, false)))) {
            SpannerFilterPushDown.State state =
                    translate(
                            Dialect.GOOGLE_STANDARD_SQL,
                            false,
                            predicates.toArray(new ResolvedExpression[0]));
            assertThat(state.keySet(ASC)).isEqualTo(KeySet.newBuilder().build());
            assertThat(state.keySet(DESC)).isEqualTo(KeySet.newBuilder().build());
            assertThat(state.result().getRemainingFilters()).isEmpty();
            for (double candidate : VALUES) {
                assertThat(state.runtime().matchesPrimaryKey(Key.of(candidate))).isFalse();
            }
        }
    }

    @Test
    void competingBoundsAndEqualitiesTreatBothZerosAsTheSameValue() {
        SpannerFilterPushDown.State zeros =
                translate(
                        Dialect.GOOGLE_STANDARD_SQL,
                        false,
                        predicate(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, -1.0d, false),
                        predicate(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, -0.0d, false),
                        predicate(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, 1.0d, false),
                        predicate(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, 0.0d, false));
        assertThat(zeros.keySet(ASC).getRanges())
                .containsExactly(KeyRange.closedClosed(Key.of(-0.0d), Key.of(0.0d)));
        assertThat(zeros.runtime().matchesPrimaryKey(Key.of(-0.0d))).isTrue();
        assertThat(zeros.runtime().matchesPrimaryKey(Key.of(0.0d))).isTrue();
        assertThat(zeros.runtime().matchesPrimaryKey(Key.of(-Double.MIN_VALUE))).isFalse();
        assertThat(zeros.runtime().matchesPrimaryKey(Key.of(Double.MIN_VALUE))).isFalse();

        SpannerFilterPushDown.State equality =
                translate(
                        Dialect.GOOGLE_STANDARD_SQL,
                        false,
                        predicate(BuiltInFunctionDefinitions.EQUALS, -0.0d, false),
                        predicate(BuiltInFunctionDefinitions.EQUALS, 0.0d, false),
                        predicate(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, 0.0d, false));
        assertThat(equality.keySet(ASC)).isEqualTo(KeySet.singleKey(Key.of(-0.0d)));
        assertThat(equality.runtime().matchesPrimaryKey(Key.of(0.0d))).isTrue();
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void nanLiteralsRemainResidualForEveryComparisonAndAccessPath(Dialect dialect) {
        for (BuiltInFunctionDefinition operator : OPERATORS) {
            for (boolean secondary : List.of(false, true)) {
                for (boolean reversed : List.of(false, true)) {
                    ResolvedExpression predicate = predicate(operator, Double.NaN, reversed);
                    SpannerFilterPushDown.State state = translate(dialect, secondary, predicate);
                    assertThat(state.result().getAcceptedFilters()).isEmpty();
                    assertThat(state.result().getRemainingFilters()).containsExactly(predicate);
                    assertThat(state.keySet(ASC)).isNull();
                }
            }
        }
    }

    @Test
    void secondaryIndexRangesRemainResidualAndSurviveSerialization() throws Exception {
        ResolvedExpression predicate = predicate(BuiltInFunctionDefinitions.LESS_THAN, 1.0d, false);
        SpannerFilterPushDown.State state = translate(Dialect.POSTGRESQL, true, predicate);
        assertThat(state.result().getAcceptedFilters()).containsExactly(predicate);
        assertThat(state.result().getRemainingFilters()).containsExactly(predicate);
        assertThat(state.runtime().provesNonNull(0)).isTrue();
        SpannerFilterPushDown.RuntimeState restored = InstantiationUtil.clone(state.runtime());
        assertThat(restored).isEqualTo(state.runtime());
        assertThat(restored.keySet(DESC)).isEqualTo(state.keySet(DESC));
        assertThat(restored.matchesPrimaryKey(Key.of(-0.0d))).isTrue();
        assertThat(restored.matchesPrimaryKey(Key.of(Double.NaN))).isFalse();
    }

    @Test
    void equalityPrefixesAndTrailingKeysPreserveTheFloatRange() {
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("tenant", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("ratio", DataTypes.DOUBLE().notNull()),
                                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()))
                                .getLogicalType();
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        row,
                        new int[] {0, 1, 2},
                        Dialect.GOOGLE_STANDARD_SQL,
                        List.of(),
                        List.of(),
                        Map.of(),
                        Map.of());
        ResolvedExpression tenant =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        List.of(
                                new FieldReferenceExpression("tenant", DataTypes.STRING(), 0, 0),
                                new ValueLiteralExpression("eu")),
                        DataTypes.BOOLEAN());
        ResolvedExpression range =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.LESS_THAN,
                        List.of(
                                new FieldReferenceExpression("ratio", DataTypes.DOUBLE(), 1, 1),
                                new ValueLiteralExpression(0.0d)),
                        DataTypes.BOOLEAN());
        List<KeyColumn> keys =
                List.of(
                        new KeyColumn("tenant", 0, false, false),
                        new KeyColumn("ratio", 1, true, false),
                        new KeyColumn("id", 2, false, false));
        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(schema, List.of(tenant, range), false);
        assertThat(state.keySet(keys).getRanges())
                .containsExactly(
                        KeyRange.openClosed(
                                Key.of("eu", 0.0d), Key.of("eu", Double.NEGATIVE_INFINITY)));
        assertThat(state.runtime().matchesPrimaryKey(Key.of("eu", -Double.MIN_VALUE, 1L))).isTrue();
        assertThat(state.runtime().matchesPrimaryKey(Key.of("us", -Double.MIN_VALUE, 1L)))
                .isFalse();
        assertThat(state.runtime().matchesPrimaryKey(Key.of("eu", -0.0d, 1L))).isFalse();
        assertThat(
                        SpannerFilterPushDown.translate(schema, List.of(range), false)
                                .result()
                                .getRemainingFilters())
                .containsExactly(range);
    }

    private static SpannerFilterPushDown.State translate(
            Dialect dialect, boolean secondary, ResolvedExpression... predicates) {
        SpannerTableSchemaConverter schema =
                SpannerTableSchemaConverter.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "ratio", DataTypes.DOUBLE().notNull()))
                                        .getLogicalType(),
                        new int[] {0},
                        dialect,
                        List.of(),
                        List.of(),
                        Map.of(),
                        Map.of());
        return SpannerFilterPushDown.translate(schema, List.of(predicates), secondary);
    }

    private static ResolvedExpression predicate(
            BuiltInFunctionDefinition operator, double bound, boolean reversed) {
        ValueLiteralExpression literal = new ValueLiteralExpression(bound);
        return CallExpression.permanent(
                operator,
                reversed ? List.of(literal, FIELD) : List.of(FIELD, literal),
                DataTypes.BOOLEAN());
    }

    private static boolean evaluate(BuiltInFunctionDefinition operator, double left, double right) {
        if (operator == BuiltInFunctionDefinitions.LESS_THAN) {
            return left < right;
        }
        if (operator == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return left <= right;
        }
        if (operator == BuiltInFunctionDefinitions.GREATER_THAN) {
            return left > right;
        }
        if (operator == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return left >= right;
        }
        return left == right;
    }
}
