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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.cloud.spanner.TestPartitions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchEnumeratorState;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SpannerPartitionSplitEnumerator}.
 *
 * <p>The assignment protocol itself is {@code PullAssignmentSplitEnumeratorTest}'s in the base
 * module. What is here is what would pass there and still be wrong in this connector: a second
 * plan, a seam left open, a failure message that does not name the read, and a metric under the
 * wrong name.
 */
class SpannerPartitionSplitEnumeratorTest {

    @AfterEach
    void forgetRecordings() {
        ScriptedPartitionPlanner.reset();
    }

    @Test
    void aFreshEnumeratorPlansOnceAndNumbersTheSplitsInOrder() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a", "b", "c");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(2);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(planner.plans()).isEqualTo(1);
            SpannerBatchEnumeratorState state = enumerator.snapshotState(1L);
            assertThat(state.isPlanned()).isTrue();
            assertThat(state.getPendingSplits())
                    .extracting(PartitionSplit::splitId)
                    .containsExactly("0", "1", "2");
            assertThat(
                            state.getPendingSplits().stream()
                                    .map(
                                            split ->
                                                    split.getPartition()
                                                            .getPartitionToken()
                                                            .toStringUtf8())
                                    .toArray())
                    .containsExactly("a", "b", "c");
        }
    }

    @Test
    void everySplitOfOnePlanCarriesTheSameSnapshot() throws Exception {
        // The property the whole design rests on: one timestamp across every subtask. A plan that
        // handed each split its own transaction id would still pass every assignment test.
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a", "b");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(2);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(enumerator.snapshotState(1L).getPendingSplits())
                    .extracting(PartitionSplit::getBatchTransactionId)
                    .containsOnly(TestPartitions.batchTransactionId());
        }
    }

    @Test
    void aRestoredEnumeratorNeverPlansAgain() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);
        SpannerBatchEnumeratorState restored =
                new SpannerBatchEnumeratorState(
                        true,
                        Collections.singletonList(
                                new PartitionSplit(
                                        "0",
                                        TestPartitions.batchTransactionId(),
                                        TestPartitions.queryPartition("a", "SELECT 1"))));

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, restored)) {
            enumerator.start();
            context.runAsyncCalls();

            // A second plan would open a second batch transaction, at a second timestamp, and hand
            // its partitions out under the ids the readers already hold.
            assertThat(planner.plans()).isZero();
            assertThat(enumerator.snapshotState(1L).getPendingSplits()).hasSize(1);
        }
    }

    @Test
    void aRestoredButUnplannedStateStillPlans() throws Exception {
        // The other arm: a checkpoint taken before the plan landed has nothing to restore, and a
        // restore that treated it as planned would leave the source reading nothing.
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(
                        context,
                        planner,
                        new SpannerBatchEnumeratorState(false, Collections.emptyList()))) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(planner.plans()).isEqualTo(1);
        }
    }

    @Test
    void theReadParametersReachThePlanner() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(
                        context,
                        builder ->
                                TestSources.withPlanner(builder, planner)
                                        .timestampBound(
                                                TimestampBound.ofExactStaleness(
                                                        9, TimeUnit.SECONDS))
                                        .maxPartitions(4)
                                        .dataBoostEnabled(true)
                                        .rpcPriority(SpannerRpcPriority.LOW),
                        null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(planner.bounds())
                    .singleElement()
                    .extracting(TimestampBound::getMode)
                    .isEqualTo(TimestampBound.Mode.EXACT_STALENESS);
            assertThat(planner.partitionOptions())
                    .singleElement()
                    .extracting(options -> options.getMaxPartitions())
                    .isEqualTo(4L);
            assertThat(planner.dataBoostFlags()).containsExactly(true);
            assertThat(planner.priorities()).containsExactly("LOW");
        }
    }

    @Test
    void anUnsetPriorityReachesThePlannerAsNothingRatherThanAsADefault() throws Exception {
        // The other arm of the test above: a builder that quietly substituted HIGH would pass it
        // and would then send a priority on every request of every job that never asked for one.
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(planner.priorities()).containsExactly("null");
        }
    }

    @Test
    void aPlanningFailureNamesTheReadAndTheDatabase() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        planner.failNextPlan(new IllegalStateException("INVALID_ARGUMENT: not partitionable"));
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null)) {
            enumerator.start();

            assertThatThrownBy(context::runAsyncCalls)
                    .isInstanceOf(FlinkRuntimeException.class)
                    // Which read, and of what — the two things a user cannot work out from
                    // "INVALID_ARGUMENT" on its own.
                    .hasMessageContaining("query [SELECT id FROM singers]")
                    .hasMessageContaining("databases/db")
                    .hasRootCauseMessage("INVALID_ARGUMENT: not partitionable");
        }
    }

    @Test
    void closingTheEnumeratorClosesThePlannerOnce() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.close();

        // The planner holds the batch transaction's session and the client behind it; the
        // enumerator is the only owner of both.
        assertThat(planner.closes()).isEqualTo(1);
    }

    @Test
    void aRefusingPlannerFailsTheCloseByName() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        planner.failClose(new IllegalStateException("no"));
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null);
        enumerator.start();

        assertThatThrownBy(enumerator::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to close the Spanner partition planner.");
    }

    @Test
    void theCountersAreRegisteredUnderTheConnectorsOwnNames() throws Exception {
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a", "b");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);
        context.registerReader(0);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(context, planner, null)) {
            enumerator.start();
            context.runAsyncCalls();
            enumerator.handleSplitRequest(0, null);
            enumerator.addSplitsBack(
                    Collections.singletonList(context.assignedSplits(0).get(0)), 0);

            assertThat(context.counter(SpannerMetricNames.READS_PLANNED)).isEqualTo(1);
            assertThat(context.counter(SpannerMetricNames.SPLITS_ASSIGNED)).isEqualTo(1);
            assertThat(context.counter(SpannerMetricNames.SPLITS_RETURNED)).isEqualTo(1);
            assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        }
    }

    @Test
    void aRestoredEnumeratorReportsNoPlanningCall() throws Exception {
        // readsPlanned is 1 on a fresh run and 0 on a restored one, which is how an operator tells
        // the two apart at runtime rather than from the log.
        ScriptedPartitionPlanner planner = ScriptedPartitionPlanner.planning("plan", "a");
        FakeSplitEnumeratorContext<PartitionSplit> context = new FakeSplitEnumeratorContext<>(1);

        try (SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator =
                enumerator(
                        context,
                        planner,
                        new SpannerBatchEnumeratorState(true, Collections.emptyList()))) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(context.counter(SpannerMetricNames.READS_PLANNED)).isZero();
        }
    }

    private static SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator(
            SplitEnumeratorContext<PartitionSplit> context,
            ScriptedPartitionPlanner planner,
            @Nullable SpannerBatchEnumeratorState restored)
            throws Exception {
        return enumerator(context, builder -> TestSources.withPlanner(builder, planner), restored);
    }

    private static SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> enumerator(
            SplitEnumeratorContext<PartitionSplit> context,
            UnaryOperator<SpannerSourceBuilder<Long>> configure,
            @Nullable SpannerBatchEnumeratorState restored)
            throws Exception {
        Source<Long, PartitionSplit, SpannerBatchEnumeratorState> source =
                TestSources.source(configure);
        return restored == null
                ? source.createEnumerator(context)
                : source.restoreEnumerator(context, restored);
    }
}
