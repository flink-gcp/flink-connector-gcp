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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.api.connector.source.SplitEnumerator;

import com.google.cloud.spanner.TestPartitions;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.ScriptedPartitionPlanner;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that one planner belongs to one enumerator.
 *
 * <p>The JobManager keeps one source object for a job's whole life, and a coordinator reset builds
 * the next enumerator from it — so a planner carried on the source configuration would already have
 * been closed by the enumerator before it. Issue #990 was that, and it was invisible because every
 * other test builds a fresh source per enumerator and because the scripted planner used to keep
 * answering after {@code close()}.
 *
 * <p>These tests therefore do the one thing nothing else does: two enumerators over <em>one</em>
 * source object, with a teardown between them.
 */
class SpannerBatchReadSourcePlannerLifecycleTest {

    @AfterEach
    void forgetRecordings() {
        ScriptedPartitionPlanner.reset();
    }

    @Test
    void aSecondEnumeratorPlansThroughItsOwnPlanner() throws Exception {
        ScriptedPartitionPlanner.Factory planner =
                ScriptedPartitionPlanner.planning("lifecycle", "a", "b");
        SpannerBatchReadSource<Long> source =
                TestSources.source(builder -> TestSources.withPlannerFactory(builder, planner));

        FakeSplitEnumeratorContext<BatchReadSplit> firstContext =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BatchReadSplit, SpannerBatchReadEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }

        FakeSplitEnumeratorContext<BatchReadSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BatchReadSplit, SpannerBatchReadEnumeratorState> second =
                source.createEnumerator(context)) {
            second.start();
            context.runAsyncCalls();

            assertThat(second.snapshotState(1L).isPlanned())
                    .as("the second enumerator plans instead of meeting a closed planner")
                    .isTrue();
        }
        assertThat(planner.plans())
                .as("each enumerator planned once, through a planner of its own")
                .isEqualTo(2);
    }

    @Test
    void eachEnumeratorGetsItsOwnPlanner() throws Exception {
        ScriptedPartitionPlanner.Factory planner =
                ScriptedPartitionPlanner.planning("lifecycle", "a");
        SpannerBatchReadSource<Long> source =
                TestSources.source(builder -> TestSources.withPlannerFactory(builder, planner));

        source.createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();
        SplitEnumerator<BatchReadSplit, SpannerBatchReadEnumeratorState> second =
                source.createEnumerator(new FakeSplitEnumeratorContext<>(1));

        assertThat(planner.minted()).hasSize(2);
        assertThat(planner.minted().get(0)).isNotSameAs(planner.minted().get(1));
        assertThat(planner.minted().get(0).isClosed())
                .as("the first enumerator's planner is the one its teardown ended")
                .isTrue();
        assertThat(planner.minted().get(1).isClosed())
                .as("the second enumerator's planner is untouched by that teardown")
                .isFalse();
        assertThat(planner.closes())
                .as("one teardown closed one planner, rather than one planner twice")
                .isOne();
        assertThat(planner.minted().get(0).closeCalls()).isOne();

        second.close();
    }

    @Test
    void aRestoreFromAPlannedCheckpointMintsItsOwnPlannerAndOpensNothing() throws Exception {
        // The call the real restore makes, with the state it really carries. Passing a null
        // checkpoint would make restoreEnumerator behave exactly like createEnumerator, so it
        // would assert nothing the test above does not.
        ScriptedPartitionPlanner.Factory planner =
                ScriptedPartitionPlanner.planning("lifecycle", "a");
        SpannerBatchReadSource<Long> source =
                TestSources.source(builder -> TestSources.withPlannerFactory(builder, planner));

        FakeSplitEnumeratorContext<BatchReadSplit> firstContext =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BatchReadSplit, SpannerBatchReadEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }

        SpannerBatchReadEnumeratorState planned =
                new SpannerBatchReadEnumeratorState(
                        true,
                        Collections.singletonList(
                                new BatchReadSplit(
                                        "0",
                                        TestPartitions.batchTransactionId(),
                                        TestPartitions.queryPartition("a", "SELECT 1"))));
        FakeSplitEnumeratorContext<BatchReadSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BatchReadSplit, SpannerBatchReadEnumeratorState> restored =
                source.restoreEnumerator(context, planned)) {
            restored.start();
            context.runAsyncCalls();

            assertThat(restored.snapshotState(1L).getPendingSplits())
                    .as("the restore adopts the checkpointed plan")
                    .hasSize(1);
        }

        assertThat(planner.minted())
                .as("the restore mints a planner of its own rather than reusing a closed one")
                .hasSize(2);
        assertThat(planner.plans())
                .as("and plans through neither: only the first enumerator planned")
                .isOne();
        assertThat(planner.minted().get(1).closeCalls())
                .as("the planner it minted and never used is still closed exactly once")
                .isOne();
    }

    /**
     * Pins the window minting opens: between {@code create()} and the enumerator taking ownership,
     * the source is the only thing that can release the planner.
     */
    @Test
    void theSourceClosesAPlannerItCouldNotHandOver() {
        ScriptedPartitionPlanner.Factory planner =
                ScriptedPartitionPlanner.planning("lifecycle", "a");
        SpannerBatchReadSource<Long> source =
                TestSources.source(builder -> TestSources.withPlannerFactory(builder, planner));

        assertThatThrownBy(() -> source.createEnumerator(null))
                .isInstanceOf(NullPointerException.class);

        assertThat(planner.minted()).hasSize(1);
        assertThat(planner.closes())
                .as("a planner the enumerator never took is released by the source")
                .isOne();
    }

    /**
     * A release that itself fails must not replace the failure that caused it.
     *
     * <p>{@code Closers.closeAllSuppressing} attaches the close failure to the original; throwing
     * it instead would report "the planner would not close" where the real cause was the argument
     * that stopped the enumerator being built, and nothing else asserts that difference.
     */
    @Test
    void aReleaseFailureIsAttachedToTheFailureThatCausedIt() {
        ScriptedPartitionPlanner.Factory planner =
                ScriptedPartitionPlanner.planning("lifecycle", "a");
        planner.failClose(new IllegalStateException("the planner would not close"));
        SpannerBatchReadSource<Long> source =
                TestSources.source(builder -> TestSources.withPlannerFactory(builder, planner));

        assertThatThrownBy(() -> source.createEnumerator(null))
                .isInstanceOf(NullPointerException.class)
                .satisfies(
                        thrown ->
                                assertThat(thrown.getSuppressed())
                                        .as("the release failure rides along rather than replacing")
                                        .anyMatch(
                                                suppressed ->
                                                        suppressed
                                                                .getMessage()
                                                                .contains(
                                                                        "the planner would not"
                                                                                + " close")));
    }
}
