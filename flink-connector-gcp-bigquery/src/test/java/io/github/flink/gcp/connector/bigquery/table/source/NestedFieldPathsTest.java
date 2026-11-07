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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests which planner paths become Storage Read paths. */
class NestedFieldPathsTest {

    private static final RowType TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                                    DataTypes.FIELD(
                                            "r",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("a", DataTypes.BIGINT()),
                                                    DataTypes.FIELD(
                                                            "c",
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "d",
                                                                            DataTypes.BIGINT()))))),
                                    DataTypes.FIELD(
                                            "span",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("start", DataTypes.DATE()),
                                                    DataTypes.FIELD("end", DataTypes.DATE()))),
                                    DataTypes.FIELD(
                                            "gap",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("months", DataTypes.INT()),
                                                    DataTypes.FIELD("days", DataTypes.INT()),
                                                    DataTypes.FIELD(
                                                            "microseconds", DataTypes.BIGINT()))),
                                    DataTypes.FIELD(
                                            "required",
                                            DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "a",
                                                                    DataTypes.BIGINT().notNull()))
                                                    .notNull()),
                                    DataTypes.FIELD(
                                            "loose",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(
                                                            "a", DataTypes.BIGINT().notNull()))))
                            .getLogicalType();

    @Test
    void selectsDottedPathsAndKeepsTopLevelColumnsAsNames() {
        assertThat(NestedFieldPaths.selectedFields(TYPE, new int[][] {{1, 1, 0}, {0}, {1, 0}}))
                .containsExactly("r.c.d", "id", "r.a");
    }

    @Test
    void collapsesEqualPathsAndDropsAPathBelowAnotherSelectedOne() {
        // Storage Read returns the whole STRUCT for "r", so "r.a" beside it adds nothing.
        assertThat(NestedFieldPaths.selectedFields(TYPE, new int[][] {{1, 0}, {1, 0}}))
                .containsExactly("r.a");
        assertThat(NestedFieldPaths.selectedFields(TYPE, new int[][] {{1, 1, 0}, {1}, {1, 1}}))
                .containsExactly("r");
    }

    @Test
    void stopsAtARangeOrIntervalShapedRow() {
        // Storage Read rejected "rng.start" on a RANGE column on 2026-11-07 ("do not exist").
        assertThat(NestedFieldPaths.selectedFields(TYPE, new int[][] {{2, 0}, {2, 1}}))
                .containsExactly("span");
        assertThat(NestedFieldPaths.selectedFields(TYPE, new int[][] {{3, 2}}))
                .containsExactly("gap");
    }

    @Test
    void stopsAtARowDeclaringOnlyPartOfARangeOrInterval() {
        // A RANGE may be declared with one endpoint, which the converter reads by name; selecting
        // "since.start" would fail the session as "span.start" does.
        RowType partial =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "since",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "start", DataTypes.DATE()))),
                                        DataTypes.FIELD(
                                                "gap",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("days", DataTypes.INT()))))
                                .getLogicalType();
        assertThat(NestedFieldPaths.selectedFields(partial, new int[][] {{0, 0}, {1, 0}}))
                .containsExactly("since", "gap");
        assertThat(NestedFieldPaths.restrictableLeaf(partial, new String[] {"since", "start"}))
                .isNull();
    }

    @Test
    void restrictsOnlyPathsWhoseNullabilityFlinkAndBigQueryAgreeOn() {
        assertThat(NestedFieldPaths.restrictableLeaf(TYPE, new String[] {"r", "c", "d"}))
                .satisfies(
                        type -> assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.BIGINT));
        // A NOT NULL ROW whose field is NOT NULL is never NULL to Flink, as a NOT NULL column is.
        assertThat(NestedFieldPaths.restrictableLeaf(TYPE, new String[] {"required", "a"}))
                .isNotNull();
        // Flink treats loose.a as never NULL; BigQuery yields NULL for it under a NULL loose.
        assertThat(NestedFieldPaths.restrictableLeaf(TYPE, new String[] {"loose", "a"})).isNull();
        assertThat(NestedFieldPaths.restrictableLeaf(TYPE, new String[] {"span", "start"}))
                .isNull();
        assertThat(NestedFieldPaths.restrictableLeaf(TYPE, new String[] {"gap", "days"})).isNull();
        assertThat(NestedFieldPaths.restrictableLeaf(TYPE, new String[] {"r", "zz"})).isNull();
    }

    @Test
    void findsANotNullFieldOnlyBelowANullableRow() {
        assertThat(NestedFieldPaths.hasNotNullFieldUnderNullableRow(TYPE.getTypeAt(4))).isFalse();
        assertThat(NestedFieldPaths.hasNotNullFieldUnderNullableRow(TYPE.getTypeAt(5))).isTrue();
        assertThat(NestedFieldPaths.hasNotNullFieldUnderNullableRow(TYPE.getTypeAt(1))).isFalse();
        // Two levels down: a NOT NULL row holding a nullable row holding a NOT NULL leaf.
        assertThat(
                        NestedFieldPaths.hasNotNullFieldUnderNullableRow(
                                DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "c",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "d",
                                                                        DataTypes.BIGINT()
                                                                                .notNull()))))
                                        .notNull()
                                        .getLogicalType()))
                .isTrue();
        assertThat(
                        NestedFieldPaths.hasNotNullFieldUnderNullableRow(
                                DataTypes.BIGINT().notNull().getLogicalType()))
                .isFalse();
    }
}
