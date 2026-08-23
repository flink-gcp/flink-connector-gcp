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

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.Filters;

import java.util.List;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

/**
 * Builds the read filter that serves a projection server-side: only the retained column families
 * leave the server.
 *
 * <p>Retaining no family at all — a row-key-only or empty projection — must not become an empty
 * interleave: Bigtable has no row without a cell, so a filter matching no cell would drop every row
 * rather than strip every row to its key. The keys-only chain below is the service's idiom for that
 * read: one cell per row, its value stripped, so each row arrives as its key and nothing else.
 */
@Internal
final class FamilyProjectionFilter {

    private FamilyProjectionFilter() {}

    /**
     * Builds the filter for the retained families.
     *
     * @param retainedFamilies the family names the query reads, in DDL order; empty for a
     *     row-key-only or empty projection
     * @return the filter to hand to the scan
     */
    static Filters.Filter of(List<String> retainedFamilies) {
        if (retainedFamilies.isEmpty()) {
            return FILTERS.chain()
                    .filter(FILTERS.limit().cellsPerRow(1))
                    .filter(FILTERS.value().strip());
        }
        Filters.InterleaveFilter interleave = FILTERS.interleave();
        for (String family : retainedFamilies) {
            // exactMatch, not regex: a family name is a literal here, and the client escapes it.
            // The one character escaping cannot save, ':', is rejected at DDL validation.
            interleave.filter(FILTERS.family().exactMatch(family));
        }
        return interleave;
    }
}
