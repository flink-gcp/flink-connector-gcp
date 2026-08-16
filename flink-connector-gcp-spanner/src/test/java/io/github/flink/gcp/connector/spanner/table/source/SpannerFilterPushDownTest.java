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
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerFilterPushDownTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("tenant", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                    DataTypes.FIELD("score", DataTypes.BIGINT()),
                    DataTypes.FIELD("ratio", DataTypes.DOUBLE()));
    private static final SpannerTableSchemaConverter SCHEMA = schema(PHYSICAL, 0, 1);

    @Test
    void exactCompositePrimaryKeyPredicatesBecomeAPointRead() {
        ResolvedExpression tenant = equals(field(0), literal("eu"));
        ResolvedExpression id = equals(field(1), literal(7));

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(tenant, id), false);

        assertResult(state.result(), Arrays.asList(tenant, id), Collections.emptyList());
        assertThat(state.keySet(primaryKey())).isEqualTo(KeySet.singleKey(Key.of("eu", 7L)));
        assertThat(state.runtime().matchesPrimaryKey(Key.of("eu", 7L))).isTrue();
        assertThat(state.runtime().matchesPrimaryKey(Key.of("eu", 8L))).isFalse();
        assertThat(state.directionIndependentPrimaryKeySet(primaryKey()))
                .isEqualTo(KeySet.singleKey(Key.of("eu", 7L)));
    }

    @Test
    void leadingEqualitiesAndTheNextRangeBecomeALexicographicRange() {
        ResolvedExpression tenant = equals(field(0), literal("eu"));
        ResolvedExpression lower =
                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, field(1), literal(7));
        ResolvedExpression upper = call(BuiltInFunctionDefinitions.LESS_THAN, field(1), literal(9));
        ResolvedExpression nonKey = equals(field(2), literal(1));

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(
                        SCHEMA, Arrays.asList(tenant, lower, upper, nonKey), false);

        assertResult(
                state.result(),
                Arrays.asList(tenant, lower, upper),
                Collections.singletonList(nonKey));
        assertThat(state.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.closedOpen(Key.of("eu", 7L), Key.of("eu", 9L)));
        assertThat(state.directionIndependentPrimaryKeySet(primaryKey())).isNull();
    }

    @Test
    void competingBoundsChooseTheStrongestEndpoints() {
        ResolvedExpression tenant = equals(field(0), literal("eu"));
        ResolvedExpression lowerSeven =
                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, field(1), literal(7));
        ResolvedExpression lowerEight =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(1), literal(8));
        ResolvedExpression upperTen =
                call(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, field(1), literal(10));
        ResolvedExpression upperNine =
                call(BuiltInFunctionDefinitions.LESS_THAN, field(1), literal(9));

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(
                        SCHEMA,
                        Arrays.asList(tenant, lowerSeven, lowerEight, upperTen, upperNine),
                        false);

        assertThat(state.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.openOpen(Key.of("eu", 8L), Key.of("eu", 9L)));
    }

    @Test
    void descendingKeyPartsReverseThePhysicalRangeEndpoints() {
        ResolvedExpression lower =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(2), literal(10));
        ResolvedExpression upper =
                call(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, field(2), literal(20));
        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(lower, upper), true);
        List<SpannerFilterPushDown.KeyPart> descending =
                Collections.singletonList(
                        new SpannerFilterPushDown.KeyPart("score", 2, true, true));

        assertThat(state.keySet(descending).getRanges())
                .containsExactly(KeyRange.closedOpen(Key.of(20L), Key.of(10L)));
    }

    @Test
    void oneSidedRangesUsePrefixEndpointsWithoutWidening() {
        ResolvedExpression tenant = equals(field(0), literal("eu"));
        ResolvedExpression lower =
                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, field(1), literal(7));
        ResolvedExpression strictLower =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(1), literal(7));
        ResolvedExpression upper = call(BuiltInFunctionDefinitions.LESS_THAN, field(1), literal(9));

        SpannerFilterPushDown.State fromSeven =
                SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(tenant, lower), false);
        SpannerFilterPushDown.State afterSeven =
                SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(tenant, strictLower), false);
        SpannerFilterPushDown.State beforeNine =
                SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(tenant, upper), false);

        assertThat(fromSeven.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.closedClosed(Key.of("eu", 7L), Key.of("eu")));
        assertThat(afterSeven.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.openClosed(Key.of("eu", 7L), Key.of("eu")));
        assertThat(beforeNine.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.closedOpen(Key.of("eu"), Key.of("eu", 9L)));
    }

    @Test
    void stringBoundsFollowSpannerUnicodeCodePointOrder() {
        ResolvedExpression privateUseLower =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(0), literal("\uE000"));
        ResolvedExpression supplementaryLower =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(0), literal("\uD83D\uDE00"));

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(
                        SCHEMA, Arrays.asList(privateUseLower, supplementaryLower), false);

        assertThat(state.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.openClosed(Key.of("\uD83D\uDE00"), Key.of()));
    }

    @Test
    void aPartiallyPushableAndRemainsAsOneFlinkResidual() {
        ResolvedExpression tenant = equals(field(0), literal("eu"));
        ResolvedExpression unsupported =
                call(
                        BuiltInFunctionDefinitions.OR,
                        equals(field(2), literal(1)),
                        equals(field(2), literal(2)));
        ResolvedExpression conjunction = call(BuiltInFunctionDefinitions.AND, tenant, unsupported);

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(
                        SCHEMA, Collections.singletonList(conjunction), false);

        assertResult(
                state.result(),
                Collections.singletonList(conjunction),
                Collections.singletonList(conjunction));
        assertThat(state.keySet(primaryKey()).getRanges())
                .containsExactly(KeyRange.prefix(Key.of("eu")));
    }

    @Test
    void secondaryIndexCandidatesRemainForFlinkWithoutAPlanningCostClaim() {
        ResolvedExpression score = equals(field(2), literal(5));

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(SCHEMA, Collections.singletonList(score), true);

        assertResult(state.result(), Collections.emptyList(), Collections.singletonList(score));
        assertThat(state.keySet(secondaryKey()).getRanges())
                .containsExactly(KeyRange.prefix(Key.of(5L)));
    }

    @Test
    void orderedFloatAndUnsupportedOperatorsRemainForFlink() {
        SpannerTableSchemaConverter floatKey = schema(PHYSICAL, 3);
        ResolvedExpression ordered =
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        field(3),
                        new ValueLiteralExpression(1.0d));
        ResolvedExpression notEquals =
                call(
                        BuiltInFunctionDefinitions.NOT_EQUALS,
                        field(3),
                        new ValueLiteralExpression(2.0d));

        for (ResolvedExpression filter : Arrays.asList(ordered, notEquals)) {
            SpannerFilterPushDown.State state =
                    SpannerFilterPushDown.translate(
                            floatKey, Collections.singletonList(filter), false);

            assertResult(
                    state.result(), Collections.emptyList(), Collections.singletonList(filter));
            assertThat(
                            state.keySet(
                                    Collections.singletonList(
                                            new SpannerFilterPushDown.KeyPart(
                                                    "ratio", 3, false, true))))
                    .isNull();
        }
    }

    @Test
    void uuidEqualityPushesButUuidOrderingRemainsForFlink() {
        SpannerTableSchemaConverter uuidKey =
                SpannerTableSchemaConverter.of(
                        (RowType) PHYSICAL.getLogicalType(),
                        new int[] {0},
                        Dialect.GOOGLE_STANDARD_SQL,
                        Collections.emptyList(),
                        Collections.singletonList("tenant"),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        String value = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6";
        ResolvedExpression equality = equals(field(0), literal(value));
        ResolvedExpression ordered =
                call(BuiltInFunctionDefinitions.GREATER_THAN, field(0), literal(value));

        SpannerFilterPushDown.State exact =
                SpannerFilterPushDown.translate(
                        uuidKey, Collections.singletonList(equality), false);
        SpannerFilterPushDown.State residual =
                SpannerFilterPushDown.translate(uuidKey, Collections.singletonList(ordered), false);

        assertResult(exact.result(), Collections.singletonList(equality), Collections.emptyList());
        assertThat(
                        exact.keySet(
                                Collections.singletonList(
                                        new SpannerFilterPushDown.KeyPart(
                                                "tenant", 0, false, false))))
                .isEqualTo(KeySet.singleKey(Key.of(UUID.fromString(value))));
        assertResult(
                residual.result(), Collections.emptyList(), Collections.singletonList(ordered));
    }

    @Test
    void aNullComparisonLiteralRemainsForFlink() {
        ResolvedExpression filter =
                equals(field(1), new ValueLiteralExpression(null, DataTypes.BIGINT()));

        SpannerFilterPushDown.State state =
                SpannerFilterPushDown.translate(SCHEMA, Collections.singletonList(filter), false);

        assertResult(state.result(), Collections.emptyList(), Collections.singletonList(filter));
        assertThat(state.keySet(primaryKey())).isNull();
    }

    @Test
    void comparisonsAndIsNotNullProveNullFilteredIndexSafety() {
        ResolvedExpression comparison = equals(field(2), literal(5));
        ResolvedExpression nonNull = call(BuiltInFunctionDefinitions.IS_NOT_NULL, field(3));
        SpannerFilterPushDown.RuntimeState runtime =
                SpannerFilterPushDown.translate(SCHEMA, Arrays.asList(comparison, nonNull), true)
                        .runtime();

        assertThat(runtime.provesNonNull(2)).isTrue();
        assertThat(runtime.provesNonNull(3)).isTrue();
        assertThat(runtime.provesNonNull(1)).isFalse();
    }

    @Test
    void runtimeStateIsSerializableWithoutPlannerExpressions() throws Exception {
        SpannerFilterPushDown.RuntimeState runtime =
                SpannerFilterPushDown.translate(
                                SCHEMA,
                                Collections.singletonList(equals(field(0), literal("eu"))),
                                false)
                        .runtime();

        SpannerFilterPushDown.RuntimeState copy = InstantiationUtil.clone(runtime);

        assertThat(copy).isEqualTo(runtime);
        assertThat(copy.matchesPrimaryKey(Key.of("eu", 7L))).isTrue();
        assertThat(copy.matchesPrimaryKey(Key.of("us", 7L))).isFalse();
    }

    private static List<SpannerFilterPushDown.KeyPart> primaryKey() {
        return Arrays.asList(
                new SpannerFilterPushDown.KeyPart("tenant", 0, false, false),
                new SpannerFilterPushDown.KeyPart("id", 1, false, false));
    }

    private static List<SpannerFilterPushDown.KeyPart> secondaryKey() {
        return Arrays.asList(
                new SpannerFilterPushDown.KeyPart("score", 2, false, true),
                new SpannerFilterPushDown.KeyPart("tenant", 0, false, false),
                new SpannerFilterPushDown.KeyPart("id", 1, false, false));
    }

    private static void assertResult(
            SupportsFilterPushDown.Result result,
            List<ResolvedExpression> accepted,
            List<ResolvedExpression> remaining) {
        assertThat(result.getAcceptedFilters()).containsExactlyElementsOf(accepted);
        assertThat(result.getRemainingFilters()).containsExactlyElementsOf(remaining);
    }

    private static SpannerTableSchemaConverter schema(DataType type, int... primaryKey) {
        return SpannerTableSchemaConverter.of(
                (RowType) type.getLogicalType(),
                primaryKey,
                Dialect.GOOGLE_STANDARD_SQL,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private static FieldReferenceExpression field(int index) {
        List<DataType> types = PHYSICAL.getChildren();
        return new FieldReferenceExpression(
                ((RowType) PHYSICAL.getLogicalType()).getFieldNames().get(index),
                types.get(index),
                index,
                index);
    }

    private static ValueLiteralExpression literal(long value) {
        return new ValueLiteralExpression(BigDecimal.valueOf(value));
    }

    private static ValueLiteralExpression literal(String value) {
        return new ValueLiteralExpression(value);
    }

    private static CallExpression equals(ResolvedExpression left, ResolvedExpression right) {
        return call(BuiltInFunctionDefinitions.EQUALS, left, right);
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }
}
