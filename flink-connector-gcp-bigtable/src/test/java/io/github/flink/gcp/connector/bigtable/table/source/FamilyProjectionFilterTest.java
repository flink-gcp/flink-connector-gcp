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

package io.github.flink.gcp.connector.bigtable.table.source;

import com.google.bigtable.v2.RowFilter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Pins the wire shape of the projection filter, by proto equality. */
class FamilyProjectionFilterTest {

    @Test
    void noRetainedFamilyIsTheKeysOnlyChain() {
        // Not an empty interleave: Bigtable has no row without a cell, so a filter matching no
        // cell would drop every row rather than strip each to its key.
        RowFilter filter = FamilyProjectionFilter.of(Collections.emptyList()).toProto();

        assertThat(filter)
                .isEqualTo(
                        FILTERS.chain()
                                .filter(FILTERS.limit().cellsPerRow(1))
                                .filter(FILTERS.value().strip())
                                .toProto());
        // The shape itself, so a refactor of the expectation cannot drift with the code.
        assertThat(filter.getChain().getFiltersList()).hasSize(2);
        assertThat(filter.getChain().getFilters(0).getCellsPerRowLimitFilter()).isEqualTo(1);
        assertThat(filter.getChain().getFilters(1).getStripValueTransformer()).isTrue();
    }

    @Test
    void oneFamilyCollapsesToItsExactMatch() {
        // The client's InterleaveFilter drops the interleave for a single branch; pinned here so
        // an SDK that stopped collapsing would be noticed rather than silently change the wire.
        RowFilter filter = FamilyProjectionFilter.of(Collections.singletonList("cf1")).toProto();

        assertThat(filter).isEqualTo(FILTERS.family().exactMatch("cf1").toProto());
    }

    @Test
    void twoFamiliesInterleaveTheirExactMatches() {
        RowFilter filter = FamilyProjectionFilter.of(Arrays.asList("cf1", "cf2")).toProto();

        assertThat(filter)
                .isEqualTo(
                        FILTERS.interleave()
                                .filter(FILTERS.family().exactMatch("cf1"))
                                .filter(FILTERS.family().exactMatch("cf2"))
                                .toProto());
        assertThat(filter.getInterleave().getFiltersList()).hasSize(2);
    }

    @Test
    void aRegexMetacharacterInAFamilyNameIsEscaped() {
        // '.' is legal in a Bigtable family name and meaningful in RE2; exactMatch must quote it,
        // or a family named "a.b" would also read "axb".
        RowFilter filter = FamilyProjectionFilter.of(Collections.singletonList("a.b")).toProto();

        assertThat(filter.getFamilyNameRegexFilter()).isEqualTo("a\\.b");
    }
}
