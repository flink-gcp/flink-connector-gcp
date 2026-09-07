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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CloudTasksReplaySnapshotTest {
    @Test
    void tracksIndependentMinimaWithoutAssumingOriginOrderOrEqualWindows() {
        var empty = CloudTasksReplaySnapshot.empty();
        var observed = empty.include(1000, 5000).include(900, 6000).include(1200, 4000);
        assertThat(empty.ageMillis(2000)).isEqualTo(-1);
        assertThat(empty.remainingMillis(2000)).isEqualTo(-1);
        assertThat(observed.ageMillis(2000)).isEqualTo(1100);
        assertThat(observed.remainingMillis(2000)).isEqualTo(2000);
        assertThat(observed.remainingMillis(4000)).isZero();
        assertThat(observed.remainingMillis(4001)).isZero();
        assertThat(observed.ageMillis(800)).isZero();
    }

    @Test
    void saturatesObservableDifferencesWithoutChangingAuthorizationArithmetic() {
        var observed = CloudTasksReplaySnapshot.empty().include(Long.MIN_VALUE, Long.MAX_VALUE);
        assertThat(observed.ageMillis(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
        assertThat(observed.remainingMillis(Long.MIN_VALUE)).isEqualTo(Long.MAX_VALUE);
    }
}
