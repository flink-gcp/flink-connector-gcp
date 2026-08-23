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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeStreamPartitionSplitStateTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void advancesProgressMonotonicallyWithoutChangingThePartitionDefinition() {
        ChangeStreamPartitionSplit original =
                new ChangeStreamPartitionSplit(
                        "token",
                        Collections.singletonList(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                        START,
                        START.plusSeconds(30),
                        2_000,
                        START,
                        PartitionLifecycleState.RUNNING,
                        START);
        ChangeStreamPartitionSplitState state = new ChangeStreamPartitionSplitState(original);

        state.advance(START.plusSeconds(10), START.plusSeconds(8));
        state.advance(START.plusSeconds(9), START.plusSeconds(7));

        ChangeStreamPartitionSplit checkpoint = state.toSplit();
        assertThat(state.splitId()).isEqualTo(original.splitId());
        assertThat(state.isInitialPartition()).isFalse();
        assertThat(checkpoint.samePartitionDefinition(original)).isTrue();
        assertThat(checkpoint.getLifecycleState()).isEqualTo(PartitionLifecycleState.RUNNING);
        assertThat(checkpoint.getCurrentPosition()).isEqualTo(START.plusSeconds(10));
        assertThat(checkpoint.getWatermark()).isEqualTo(START.plusSeconds(8));
    }
}
