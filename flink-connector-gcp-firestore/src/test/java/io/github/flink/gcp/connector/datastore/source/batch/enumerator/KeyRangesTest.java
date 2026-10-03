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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.DatastoreHelper;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The splitter's ranges laid out again in the service's key order. */
class KeyRangesTest {

    /** Ordered after an emoji by Java, before it by the service. */
    private static final String FULLWIDTH = "a！";

    private static final String EMOJI = "a😀";

    private static Key key(String name) {
        return DatastoreHelper.makeKey("K", name).build();
    }

    private static Key key(long id) {
        return DatastoreHelper.makeKey("K", id).build();
    }

    private static Query ones() {
        return SplittableQueries.ofKind("K").toBuilder()
                .setFilter(
                        DatastoreHelper.makeFilter(
                                        "k",
                                        PropertyFilter.Operator.EQUAL,
                                        DatastoreHelper.makeValue(1))
                                .build())
                .build();
    }

    /** Builds ranges in the given boundary order, in the splitter's shape. */
    private static List<Query> splitterShaped(Query query, String... boundaries) {
        List<Query> ranges = new ArrayList<>();
        Key start = null;
        for (String boundary : boundaries) {
            ranges.add(range(query, start, key(boundary)));
            start = key(boundary);
        }
        ranges.add(range(query, start, null));
        return ranges;
    }

    private static Query range(Query query, Key start, Key end) {
        List<Filter> filters = new ArrayList<>(List.of(query.getFilter()));
        if (start != null) {
            filters.add(bound(PropertyFilter.Operator.GREATER_THAN_OR_EQUAL, start));
        }
        if (end != null) {
            filters.add(bound(PropertyFilter.Operator.LESS_THAN, end));
        }
        return query.toBuilder().setFilter(DatastoreHelper.makeAndFilter(filters)).build();
    }

    private static Filter bound(PropertyFilter.Operator op, Key key) {
        return DatastoreHelper.makeFilter("__key__", op, DatastoreHelper.makeValue(key)).build();
    }

    @Test
    void ordersNamesByTheirUtf8BytesNotByJavaStringOrder() {
        assertThat(FULLWIDTH.compareTo(EMOJI))
                .as("Java, and the library's comparator, put the fullwidth name last")
                .isPositive();
        assertThat(KeyRanges.SERVICE_ORDER.compare(key(FULLWIDTH), key(EMOJI))).isNegative();
    }

    @Test
    void ordersIdsBeforeNamesAndShorterPathsFirst() {
        assertThat(KeyRanges.SERVICE_ORDER.compare(key(5L), key("a"))).isNegative();
        assertThat(KeyRanges.SERVICE_ORDER.compare(key(5L), key(40L))).isNegative();
        Key parent = key("p");
        Key child = DatastoreHelper.makeKey("K", "p", "C", "c").build();
        assertThat(KeyRanges.SERVICE_ORDER.compare(parent, child)).isNegative();
        assertThat(
                        KeyRanges.SERVICE_ORDER.compare(
                                DatastoreHelper.makeKey("A", "z").build(),
                                DatastoreHelper.makeKey("B", "a").build()))
                .isNegative();
    }

    @Test
    void rebuildsRangesWhoseBoundariesArrivedInJavaOrder() {
        // The splitter sorts its sample as Java strings, so it hands these over in this order.
        List<Query> fromSplitter = splitterShaped(ones(), "a", EMOJI, FULLWIDTH, "b");

        assertThat(KeyRanges.retile(ones(), fromSplitter))
                .containsExactlyElementsOf(splitterShaped(ones(), "a", FULLWIDTH, EMOJI, "b"));
    }

    @Test
    void dropsARepeatedBoundary() {
        List<Query> fromSplitter = splitterShaped(ones(), "a", "a", "b");

        assertThat(KeyRanges.retile(ones(), fromSplitter))
                .containsExactlyElementsOf(splitterShaped(ones(), "a", "b"));
    }

    @Test
    void leavesAnUnsplitAnswerAlone() {
        List<Query> whole = List.of(ones());

        assertThat(KeyRanges.retile(ones(), whole)).isSameAs(whole);
    }

    @Test
    void ignoresAKeyEqualityTheQueryCarriesItself() {
        Query byKey =
                SplittableQueries.ofKind("K").toBuilder()
                        .setFilter(
                                DatastoreHelper.makeFilter(
                                                "__key__",
                                                PropertyFilter.Operator.EQUAL,
                                                DatastoreHelper.makeValue(key("x")))
                                        .build())
                        .build();
        List<Query> fromSplitter = splitterShaped(byKey, "m");

        assertThat(KeyRanges.retile(byKey, fromSplitter))
                .containsExactlyElementsOf(splitterShaped(byKey, "m"));
    }
}
