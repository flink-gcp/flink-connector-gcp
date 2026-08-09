/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableCreateOptions}. */
class TableCreateOptionsTest {

    @Test
    void carriesTheFamiliesInDeclarationOrder() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .columnFamily("first")
                        .columnFamily("second", GcRule.maxVersions(1))
                        .columnFamily("third")
                        .build();

        assertThat(options.getColumnFamilies().keySet())
                .containsExactly("first", "second", "third");
        assertThat(options.getColumnFamilies().get("first")).isNull();
        assertThat(options.getColumnFamilies().get("second")).isEqualTo(GcRule.maxVersions(1));
    }

    @Test
    void aRepeatedFamilyNameIsLastWriterWins() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .columnFamily("cf", GcRule.maxVersions(1))
                        .columnFamily("cf", GcRule.maxVersions(9))
                        .build();

        assertThat(options.getColumnFamilies().get("cf")).isEqualTo(GcRule.maxVersions(9));
        assertThat(options.getColumnFamilies()).hasSize(1);
    }

    @Test
    void requiresAtLeastOneFamily() {
        assertThatThrownBy(() -> TableCreateOptions.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("columnFamily");
    }

    @Test
    void rejectsBlankAndNullSettings() {
        TableCreateOptions.Builder builder = TableCreateOptions.builder();

        assertThatThrownBy(() -> builder.columnFamily(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.columnFamily("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.columnFamily("cf", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void theFamilyViewIsUnmodifiable() {
        TableCreateOptions options = TableCreateOptions.builder().columnFamily("cf").build();

        assertThatThrownBy(() -> options.getColumnFamilies().put("other", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void isValueBased() {
        TableCreateOptions options =
                TableCreateOptions.builder().columnFamily("cf", GcRule.maxVersions(1)).build();

        assertThat(options)
                .isEqualTo(
                        TableCreateOptions.builder()
                                .columnFamily("cf", GcRule.maxVersions(1))
                                .build())
                .hasSameHashCodeAs(
                        TableCreateOptions.builder()
                                .columnFamily("cf", GcRule.maxVersions(1))
                                .build())
                .isNotEqualTo(TableCreateOptions.builder().columnFamily("cf").build());
        assertThat(options.toString()).contains("cf", "maxVersions(1)");
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        // It ships in the job graph inside the sink configuration, families, rules, nulls and
        // order included.
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .columnFamily("plain")
                        .columnFamily(
                                "kept",
                                GcRule.union(
                                        GcRule.maxVersions(1), GcRule.maxAge(Duration.ofDays(7))))
                        .build();

        TableCreateOptions copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(options), getClass().getClassLoader());

        assertThat(copy).isEqualTo(options);
        assertThat(copy.getColumnFamilies().keySet()).containsExactly("plain", "kept");
    }
}
