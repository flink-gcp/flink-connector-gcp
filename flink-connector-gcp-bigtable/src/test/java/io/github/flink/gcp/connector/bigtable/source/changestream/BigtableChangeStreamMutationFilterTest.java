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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamMutationFilterTest {

    @Test
    void noFiltersSelectsTheConversionFastPath() {
        BigtableChangeStreamMutationFilter filter = BigtableChangeStreamMutationFilter.none();

        assertThat(filter.hasEntryFilters()).isFalse();
        assertThat(filter.hasQualifierFilters()).isFalse();
        assertThat(filter.includesFamily("any-family")).isTrue();
        assertThat(filter.skipsMessagesWithoutChange()).isFalse();
    }

    @Test
    void familyFiltersUseFullMatchSemantics() {
        BigtableChangeStreamMutationFilter include =
                filter(patterns("selected"), none(), none(), none(), false);
        BigtableChangeStreamMutationFilter exclude =
                filter(none(), patterns("selected"), none(), none(), false);

        assertThat(include.includesFamily("selected")).isTrue();
        assertThat(include.includesFamily("selected-backup")).isFalse();
        assertThat(exclude.includesFamily("selected")).isFalse();
        assertThat(exclude.includesFamily("selected-backup")).isTrue();
    }

    @Test
    void qualifierFiltersMatchFamilyAndCanonicalPaddedBase64Together() {
        BigtableChangeStreamMutationFilter filter =
                filter(none(), none(), patterns("selected:YQ=="), none(), false);

        assertThat(filter.hasEntryFilters()).isTrue();
        assertThat(filter.hasQualifierFilters()).isTrue();
        assertThat(filter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("a")))
                .isTrue();
        assertThat(filter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("ab")))
                .isFalse();
        assertThat(filter.includesQualifiedColumn("other", ByteString.copyFromUtf8("a"))).isFalse();
    }

    @Test
    void qualifierFiltersHandleEmptyAndNonUtf8Bytes() {
        BigtableChangeStreamMutationFilter filter =
                filter(
                        none(),
                        none(),
                        Arrays.asList(
                                Pattern.compile("selected:"), Pattern.compile("selected:/wA=")),
                        none(),
                        false);

        assertThat(filter.includesQualifiedColumn("selected", ByteString.EMPTY)).isTrue();
        assertThat(
                        filter.includesQualifiedColumn(
                                "selected", ByteString.copyFrom(new byte[] {(byte) 0xff, 0x00})))
                .isTrue();
        assertThat(filter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("other")))
                .isFalse();
    }

    @Test
    void qualifierExcludesLeaveOtherQualifiedColumnsIncluded() {
        BigtableChangeStreamMutationFilter filter =
                filter(none(), none(), none(), patterns("selected:YQ=="), true);

        assertThat(filter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("a")))
                .isFalse();
        assertThat(filter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("b")))
                .isTrue();
        assertThat(filter.skipsMessagesWithoutChange()).isTrue();
    }

    private static BigtableChangeStreamMutationFilter filter(
            List<Pattern> familyIncludes,
            List<Pattern> familyExcludes,
            List<Pattern> qualifierIncludes,
            List<Pattern> qualifierExcludes,
            boolean skip) {
        return new BigtableChangeStreamMutationFilter(
                familyIncludes, familyExcludes, qualifierIncludes, qualifierExcludes, skip);
    }

    private static List<Pattern> patterns(String pattern) {
        return Collections.singletonList(Pattern.compile(pattern));
    }

    private static List<Pattern> none() {
        return Collections.emptyList();
    }
}
