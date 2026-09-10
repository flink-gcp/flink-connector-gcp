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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskCreationLedgerTest {
    @Test
    void twoSuccessfulResponsesAreNotEvidenceOfTwoCreations() {
        var ledger = new TaskCreationLedger();
        Task original = task(100);
        ledger.created(original);
        ledger.created(original);
        assertThat(ledger.generation(original.getName())).isEqualTo(1);
        assertThatThrownBy(() -> ledger.created(task(101)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Unexpected second creation");
    }

    @Test
    void negativeControlRequiresBothOriginalAndNewLiveObservations() {
        var ledger = new TaskCreationLedger();
        Task original = task(100);
        ledger.created(original);
        ledger.observed(original, false);
        assertThatThrownBy(() -> ledger.removed(original.getName()))
                .hasMessageContaining("not observed live");
        ledger.observed(original, true);
        ledger.removed(original.getName());
        ledger.expectRecreation(original.getName());
        assertThatThrownBy(() -> ledger.assertRecreatedAndObserved(original.getName()))
                .hasMessageContaining("did not detect a second creation");
        Task recreated = task(3_701);
        ledger.created(recreated);
        assertThatThrownBy(() -> ledger.assertRecreatedAndObserved(original.getName()))
                .hasMessageContaining("not observed live");
        ledger.observed(recreated, false);
        ledger.observed(recreated, true);
        ledger.assertRecreatedAndObserved(original.getName());
    }

    @Test
    void currentClientCannotSilentlyAcceptAnUnrecordedOldIncarnationCreation() {
        var ledger = new TaskCreationLedger();
        assertThatThrownBy(() -> ledger.observed(task(100), false))
                .hasMessageContaining("No captured CreateTask response");
        ledger.created(task(100));
        assertThatThrownBy(() -> ledger.observed(task(101), true))
                .hasMessageContaining("differs from the captured creation generation");
    }

    private static Task task(long created) {
        return Task.newBuilder()
                .setName("projects/p/locations/l/queues/q/tasks/name")
                .setCreateTime(Timestamp.newBuilder().setSeconds(created))
                .build();
    }
}
