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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The SQL {@code TIME} precision boundary, across the whole supported range in one build.
 *
 * <p>{@link BigQueryDynamicTableFactoryTest#flinkSqlTimePrecisionMatchesTheSupportedVersion()} runs
 * the boundary against the planner, but only on the one version the build put on the classpath.
 * These cases are the control it cannot be: the version below the boundary, the version at it, and
 * the versions after it, all evaluated wherever this suite runs.
 */
class FlinkVersionsTest {

    @ParameterizedTest
    @ValueSource(strings = {"1.20.4", "1.20", "1.20.4-SNAPSHOT", "2.0.0", "2.2.1", "2.2-SNAPSHOT"})
    void versionsBelowTheBoundaryResolveTimeToWholeSeconds(String version) {
        assertThat(FlinkVersions.retainsSqlTimePrecision(version)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"2.3", "2.3.0", "2.3.1", "2.3-SNAPSHOT", "2.4-SNAPSHOT", "2.10.0", "3.0.0"})
    void versionsFromTheBoundaryOnwardsKeepTheDeclaredPrecision(String version) {
        assertThat(FlinkVersions.retainsSqlTimePrecision(version)).isTrue();
    }

    @Test
    void theLtsStaysBelowTheBoundaryDespiteItsLargerMinor() {
        // What a minor-only comparison gets wrong: 1.20's minor is 20, which is above 3,
        // so dropping the major reads the LTS as keeping the precision it does not keep.
        // Named on its own so the intent survives a rewrite of the comparison.
        assertThat(FlinkVersions.retainsSqlTimePrecision("1.20.4")).isFalse();
        assertThat(FlinkVersions.retainsSqlTimePrecision("2.3.0")).isTrue();
    }

    @Test
    void aDoubleDigitMinorStaysAboveTheBoundary() {
        // What a lexicographic comparison gets wrong, which is a different pair: "2.10.0"
        // sorts *before* "2.3.0" because '1' < '3', so comparing the strings reads 2.10 as
        // below the boundary. The LTS case above does not catch this one -- "1.20.4" sorts
        // before "2.3.0" too, and there that is the right answer.
        assertThat(FlinkVersions.retainsSqlTimePrecision("2.10.0")).isTrue();
        assertThat("2.10.0".compareTo("2.3.0")).isNegative();
    }

    @Test
    void a24SnapshotIsNotTreatedAsTheFloor() {
        // The regression: `startsWith("2.3.")` was false for the version after the ceiling, so
        // the weekly `next` row expected TIME(0) from a Flink that had returned TIME(3)
        // (issue #933). Anything that keys on one exact minor again fails here.
        assertThat(FlinkVersions.retainsSqlTimePrecision("2.4-SNAPSHOT")).isTrue();
    }

    @Test
    void anUnreadableVersionFailsRatherThanAssumingTheFloor() {
        // Defaulting to false would assert the floor's behaviour against a Flink whose
        // behaviour is unknown, and pass on the floor for the wrong reason.
        assertThatThrownBy(() -> FlinkVersions.retainsSqlTimePrecision(null))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(DataTypes.class.getName());
        assertThatThrownBy(() -> FlinkVersions.retainsSqlTimePrecision("unknown"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("'unknown'");
        assertThatThrownBy(() -> FlinkVersions.retainsSqlTimePrecision("2"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void theFlinkOnThisClasspathAnswersTheSameQuestion() {
        // The no-argument overload is what the table tests call, so the delegation is worth
        // pinning: it must agree with the policy applied to the manifest it reads. Asserted
        // as an equality rather than a fixed expectation, because which answer is right here
        // depends on the version the build selected.
        String version = DataTypes.class.getPackage().getImplementationVersion();
        assertThat(version).as("Flink's Implementation-Version manifest entry").isNotNull();
        assertThat(FlinkVersions.retainsSqlTimePrecision())
                .isEqualTo(FlinkVersions.retainsSqlTimePrecision(version));
    }
}
