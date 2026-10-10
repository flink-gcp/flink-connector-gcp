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

package io.github.flink.gcp.connector.datastore.source.batch;

import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.FindNearest;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Which queries are cut into key ranges, and why the rest are not. */
class SplittableQueriesTest {

    private static final Set<PropertyFilter.Operator> SPLITTABLE_OPERATORS =
            EnumSet.of(PropertyFilter.Operator.EQUAL, PropertyFilter.Operator.HAS_ANCESTOR);

    private static Query.Builder task() {
        return SplittableQueries.ofKind("Task").toBuilder();
    }

    private static Filter filter(PropertyFilter.Operator op) {
        return DatastoreHelper.makeFilter("done", op, DatastoreHelper.makeValue(false)).build();
    }

    @Test
    void aKindScanCanBeSplit() {
        assertThat(SplittableQueries.whyNotSplittable(task().build())).isNull();
        assertThat(SplittableQueries.kindOf(task().build())).isEqualTo("Task");
    }

    @Test
    void equalityAndAncestorFiltersUnderAndCanBeSplit() {
        Query query =
                task().setFilter(
                                DatastoreHelper.makeAndFilter(
                                        filter(PropertyFilter.Operator.EQUAL),
                                        DatastoreHelper.makeAndFilter(
                                                        filter(PropertyFilter.Operator.EQUAL),
                                                        filter(
                                                                PropertyFilter.Operator
                                                                        .HAS_ANCESTOR))
                                                .build()))
                        .addProjection(
                                com.google.datastore.v1.Projection.newBuilder()
                                        .setProperty(PropertyReference.newBuilder().setName("a")))
                        .build();

        assertThat(SplittableQueries.whyNotSplittable(query)).isNull();
    }

    @ParameterizedTest
    @EnumSource(
            value = PropertyFilter.Operator.class,
            names = {"UNRECOGNIZED"},
            mode = EnumSource.Mode.EXCLUDE)
    void everyOtherOperatorIsReadAsOneSplit(PropertyFilter.Operator op) {
        // The library's splitter refuses only <, <=, > and >=; != and NOT_IN are inequalities too,
        // and IN is a disjunction, each meeting the key range ANDed on.
        String reason = SplittableQueries.whyNotSplittable(task().setFilter(filter(op)).build());

        if (SPLITTABLE_OPERATORS.contains(op)) {
            assertThat(reason).isNull();
        } else {
            assertThat(reason).isEqualTo("it filters with the " + op + " operator");
        }
    }

    @Test
    void anOrIsReadAsOneSplit() {
        Query query =
                task().setFilter(
                                Filter.newBuilder()
                                        .setCompositeFilter(
                                                CompositeFilter.newBuilder()
                                                        .setOp(CompositeFilter.Operator.OR)
                                                        .addFilters(
                                                                filter(
                                                                        PropertyFilter.Operator
                                                                                .EQUAL))))
                        .build();

        assertThat(SplittableQueries.whyNotSplittable(query))
                .isEqualTo("it combines filters with OR");
    }

    @Test
    void anInequalityNestedUnderAndIsFound() {
        Query query =
                task().setFilter(
                                DatastoreHelper.makeAndFilter(
                                        filter(PropertyFilter.Operator.EQUAL),
                                        filter(PropertyFilter.Operator.GREATER_THAN)))
                        .build();

        assertThat(SplittableQueries.whyNotSplittable(query))
                .isEqualTo("it filters with the GREATER_THAN operator");
    }

    @Test
    void positionsInTheWholeResultAreReadAsOneSplit() {
        assertThat(SplittableQueries.whyNotSplittable(task().setLimit(Int32Value.of(5)).build()))
                .isEqualTo("it has a limit");
        assertThat(SplittableQueries.whyNotSplittable(task().setLimit(Int32Value.of(0)).build()))
                .isEqualTo("it has a limit");
        assertThat(SplittableQueries.whyNotSplittable(task().setOffset(1).build()))
                .isEqualTo("it has an offset");
        assertThat(
                        SplittableQueries.whyNotSplittable(
                                task().setStartCursor(ByteString.copyFromUtf8("c")).build()))
                .isEqualTo("it starts or ends at a cursor");
        assertThat(
                        SplittableQueries.whyNotSplittable(
                                task().setEndCursor(ByteString.copyFromUtf8("c")).build()))
                .isEqualTo("it starts or ends at a cursor");
    }

    @Test
    void anOrderingAndADistinctAreReadAsOneSplit() {
        assertThat(
                        SplittableQueries.whyNotSplittable(
                                task().addOrder(
                                                DatastoreHelper.makeOrder(
                                                        "a", PropertyOrder.Direction.ASCENDING))
                                        .build()))
                .isEqualTo("it orders its results");
        assertThat(
                        SplittableQueries.whyNotSplittable(
                                task().addDistinctOn(PropertyReference.newBuilder().setName("a"))
                                        .build()))
                .isEqualTo("it is a distinct query");
    }

    @Test
    void aFilterOfNoTypeIsReadAsOneSplit() {
        // The library's splitter throws on it, alone or under AND.
        Filter empty = Filter.getDefaultInstance();

        assertThat(SplittableQueries.whyNotSplittable(task().setFilter(empty).build()))
                .isEqualTo("it has a filter of no type");
        assertThat(
                        SplittableQueries.whyNotSplittable(
                                task().setFilter(
                                                DatastoreHelper.makeAndFilter(
                                                        filter(PropertyFilter.Operator.EQUAL),
                                                        empty))
                                        .build()))
                .isEqualTo("it has a filter of no type");
    }

    @Test
    void aNearestNeighbourSearchCannotBeReadAtAll() {
        assertThat(
                        SplittableQueries.whyNotReadable(
                                task().setFindNearest(FindNearest.newBuilder()).build()))
                .startsWith("it is a nearest-neighbour search");
        assertThat(SplittableQueries.whyNotReadable(task().build())).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"__namespace__", "__kind__", "__property__"})
    void aMetadataKindCannotBeReadAtAll(String kind) {
        String reason = SplittableQueries.whyKindNotReadable(kind);

        assertThat(reason)
                .startsWith(kind + " is a metadata kind")
                .contains("which the source can neither page through nor resume after a failure")
                .endsWith("read metadata with the client library instead");
        assertThat(SplittableQueries.whyNotReadable(SplittableQueries.ofKind(kind)))
                .isEqualTo(reason);
        // The service takes at most one kind, but the rule does not depend on its position.
        assertThat(
                        SplittableQueries.whyNotReadable(
                                task().addKind(KindExpression.newBuilder().setName(kind)).build()))
                .isEqualTo(reason);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "__Stat_Kind__",
                "__Stat_Total__",
                "__Stat_Ns_Kind__",
                "__KIND__",
                "__kind",
                "kind",
                "__kind___",
                "Task"
            })
    void anyOtherKindCanBeRead(String kind) {
        assertThat(SplittableQueries.whyKindNotReadable(kind)).isNull();
        assertThat(SplittableQueries.whyNotReadable(SplittableQueries.ofKind(kind))).isNull();
    }

    @Test
    void aQueryOfNoKindCanBeRead() {
        assertThat(SplittableQueries.whyNotReadable(Query.getDefaultInstance())).isNull();
    }

    @Test
    void aQueryOfNoKindOrOfSeveralIsReadAsOneSplit() {
        Query kindless = Query.newBuilder().build();
        Query twoKinds = task().addKind(KindExpression.newBuilder().setName("Other")).build();

        assertThat(SplittableQueries.whyNotSplittable(kindless))
                .isEqualTo("it does not name exactly one kind");
        assertThat(SplittableQueries.whyNotSplittable(twoKinds))
                .isEqualTo("it does not name exactly one kind");
        assertThat(SplittableQueries.kindOf(kindless)).isNull();
        assertThat(SplittableQueries.kindOf(twoKinds)).isNull();
    }
}
