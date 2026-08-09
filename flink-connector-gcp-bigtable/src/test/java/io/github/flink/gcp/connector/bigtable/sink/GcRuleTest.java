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

/** Tests for {@link GcRule}. */
class GcRuleTest {

    @Test
    void carriesEachLeafShape() {
        GcRule versions = GcRule.maxVersions(3);
        GcRule age = GcRule.maxAge(Duration.ofHours(24));

        assertThat(versions.getKind()).isEqualTo(GcRule.Kind.MAX_VERSIONS);
        assertThat(versions.getMaxVersions()).isEqualTo(3);
        assertThat(age.getKind()).isEqualTo(GcRule.Kind.MAX_AGE);
        assertThat(age.getMaxAge()).isEqualTo(Duration.ofHours(24));
    }

    @Test
    void composesNestedRules() {
        GcRule rule =
                GcRule.union(
                        GcRule.maxVersions(1),
                        GcRule.intersection(
                                GcRule.maxAge(Duration.ofDays(7)), GcRule.maxVersions(10)));

        assertThat(rule.getKind()).isEqualTo(GcRule.Kind.UNION);
        assertThat(rule.getRules()).hasSize(2);
        assertThat(rule.getRules().get(1).getKind()).isEqualTo(GcRule.Kind.INTERSECTION);
        assertThat(rule.toString())
                .isEqualTo("union(maxVersions(1), intersection(maxAge(PT168H), maxVersions(10)))");
    }

    @Test
    void rejectsShapesTheServiceCouldOnlyRefuseObscurely() {
        assertThatThrownBy(() -> GcRule.maxVersions(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxVersions");
        assertThatThrownBy(() -> GcRule.maxAge(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAge");
        assertThatThrownBy(() -> GcRule.maxAge(null)).isInstanceOf(NullPointerException.class);
        // A composite of one is that rule and of zero is meaningless, so both are rejected rather
        // than silently normalized.
        assertThatThrownBy(() -> GcRule.union(GcRule.maxVersions(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least two");
        assertThatThrownBy(GcRule::intersection).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcRule.union(GcRule.maxVersions(1), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void isValueBased() {
        GcRule rule = GcRule.union(GcRule.maxVersions(1), GcRule.maxAge(Duration.ofDays(7)));

        assertThat(rule)
                .isEqualTo(GcRule.union(GcRule.maxVersions(1), GcRule.maxAge(Duration.ofDays(7))))
                .hasSameHashCodeAs(
                        GcRule.union(GcRule.maxVersions(1), GcRule.maxAge(Duration.ofDays(7))))
                .isNotEqualTo(
                        GcRule.intersection(
                                GcRule.maxVersions(1), GcRule.maxAge(Duration.ofDays(7))))
                .isNotEqualTo(GcRule.maxVersions(1));
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        // It ships in the job graph inside TableCreateOptions, so the round trip is the contract.
        GcRule rule =
                GcRule.union(
                        GcRule.maxVersions(2),
                        GcRule.intersection(
                                GcRule.maxAge(Duration.ofDays(30)), GcRule.maxVersions(5)));

        GcRule copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(rule), getClass().getClassLoader());

        assertThat(copy).isEqualTo(rule);
    }
}
