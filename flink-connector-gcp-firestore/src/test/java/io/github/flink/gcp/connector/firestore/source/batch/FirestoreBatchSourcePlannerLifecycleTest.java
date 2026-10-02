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

package io.github.flink.gcp.connector.firestore.source.batch;

import org.apache.flink.api.connector.source.SplitEnumerator;

import io.github.flink.gcp.connector.firestore.source.TestSources;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.ScriptedQueryPlanner;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that one planner belongs to one enumerator ({@code docs/adr/0128}): the JobManager keeps one
 * source object for a job's whole life, and a coordinator reset builds the next enumerator from it,
 * so a planner carried on the configuration would already be closed. Two enumerators over one
 * source object, with a teardown between them.
 */
class FirestoreBatchSourcePlannerLifecycleTest {

    private static FirestoreBatchSource<String> source(ScriptedQueryPlanner.Factory planners) {
        return TestSources.source(
                builder ->
                        TestSources.withPlannerFactory(
                                builder.collectionGroup("orders"), planners));
    }

    @Test
    void eachEnumeratorPlansThroughItsOwnPlanner() throws Exception {
        ScriptedQueryPlanner.Factory planners = ScriptedQueryPlanner.Factory.answering("a");
        FirestoreBatchSource<String> source = source(planners);

        FakeSplitEnumeratorContext<QuerySplit> firstContext = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<QuerySplit, FirestoreBatchEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<QuerySplit, FirestoreBatchEnumeratorState> second =
                source.createEnumerator(context)) {
            second.start();
            context.runAsyncCalls();

            assertThat(second.snapshotState(1L).isPlanned())
                    .as("the second enumerator plans instead of meeting a closed planner")
                    .isTrue();
        }

        assertThat(planners.minted()).hasSize(2);
        assertThat(planners.minted().get(0).closeCalls()).isOne();
        assertThat(planners.minted().get(1).defaultPartitionCounts()).hasSize(1);
    }

    @Test
    void aRestoreFromAPlannedCheckpointMintsItsOwnPlannerAndPlansNothing() throws Exception {
        ScriptedQueryPlanner.Factory planners = ScriptedQueryPlanner.Factory.answering("a");
        FirestoreBatchSource<String> source = source(planners);

        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<QuerySplit, FirestoreBatchEnumeratorState> restored =
                source.restoreEnumerator(
                        context,
                        new FirestoreBatchEnumeratorState(true, Collections.emptyList()))) {
            restored.start();
            context.runAsyncCalls();
        }

        assertThat(planners.minted()).hasSize(1);
        assertThat(planners.minted().get(0).defaultPartitionCounts()).isEmpty();
        assertThat(planners.minted().get(0).closeCalls()).isOne();
    }

    @Test
    void theSourceClosesAPlannerItCouldNotHandOver() {
        ScriptedQueryPlanner.Factory planners = ScriptedQueryPlanner.Factory.answering("a");
        FirestoreBatchSource<String> source = source(planners);

        assertThatThrownBy(() -> source.createEnumerator(null))
                .isInstanceOf(NullPointerException.class);

        assertThat(planners.minted()).hasSize(1);
        assertThat(planners.minted().get(0).closeCalls()).isOne();
    }
}
