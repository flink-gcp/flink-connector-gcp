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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The assignment protocol every pull-assigned source shares.
 *
 * <p>Each case is an invariant a ledger-keeping enumerator breaks quietly — a split handed out
 * twice, a split handed to a subtask that went away, a subtask told there is nothing left and then
 * never served again — rather than the happy path with extras. What a connector adds on top (its
 * planning call, its state, its metric names) is tested in the connector.
 */
class PullAssignmentSplitEnumeratorTest {

    private static final String SPLITS_ASSIGNED = "splitsAssigned";
    private static final String SPLITS_RETURNED = "splitsReturned";
    private static final String PLANS_COMPLETED = "plansCompleted";

    private static final String PLAN_FAILURE_MESSAGE = "Failed to plan the test source.";
    private static final String CLOSE_FAILURE_MESSAGE = "Failed to close the test planner.";

    @Test
    void parksRequestsThatArriveBeforeThePlanAndServesThemInArrivalOrder() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(2);
        try (TestEnumerator enumerator = enumerator(context, ScriptedPlanner.planning(2))) {
            context.registerReader(0);
            context.registerReader(1);
            enumerator.start();

            enumerator.handleSplitRequest(1, "localhost");
            enumerator.handleSplitRequest(0, "localhost");
            assertThat(context.events()).isEmpty();

            context.runAsyncCalls();

            assertThat(context.events()).containsExactly("assign:1", "assign:0");
        }
    }

    @Test
    void skipsARequestFromASubtaskThatWentAwayWhileItWasParked() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(2);
        try (TestEnumerator enumerator = enumerator(context, ScriptedPlanner.planning(1))) {
            context.registerReader(0);
            enumerator.start();
            enumerator.handleSplitRequest(0, "localhost");
            context.unregisterReader(0);

            context.runAsyncCalls();

            // The fake refuses an assignment to an unregistered subtask; the point is that the
            // enumerator checks first, and that the split stays available for whoever comes back.
            assertThat(context.events()).isEmpty();
            assertThat(enumerator.snapshotState(1L).splits).hasSize(1);
        }
    }

    @Test
    void tellsASubtaskThereAreNoMoreSplitsOnceTheQueueIsEmpty() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(2);
        try (TestEnumerator enumerator = started(context, 1)) {
            context.registerReader(0);
            context.registerReader(1);

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");

            assertThat(context.readersToldNoMoreSplits()).containsExactly(1);
            assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:1");
        }
    }

    @Test
    void servesAReaderThatWasAlreadyToldThereAreNoMoreSplits() throws Exception {
        // Nothing here records that a reader was told, which is the point: a returned split must be
        // assignable to whoever asks next. (Flink's coordinator does keep such a flag and clears it
        // when the subtask is reset — the reset that also returns the splits — so this enumerator's
        // job is only to not add a second, staler copy of the same fact.)
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(2);
        try (TestEnumerator enumerator = started(context, 1)) {
            context.registerReader(0);
            context.registerReader(1);
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            assertThat(context.readersToldNoMoreSplits()).containsExactly(1);

            enumerator.addSplitsBack(context.assignedSplits(0), 0);
            enumerator.handleSplitRequest(1, "localhost");

            assertThat(context.assignedSplits(1)).hasSize(1);
            assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:1", "assign:1");
        }
    }

    @Test
    void returnsSplitsToTheQueueAndAssignsNothingItself() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(2);
        try (TestEnumerator enumerator = started(context, 2)) {
            context.registerReader(0);
            enumerator.handleSplitRequest(0, "localhost");

            enumerator.addSplitsBack(context.assignedSplits(0), 0);

            assertThat(enumerator.snapshotState(1L).splits).hasSize(2);
            assertThat(context.events()).containsExactly("assign:0");
            assertThat(context.counter(SPLITS_RETURNED)).isEqualTo(1);
        }
    }

    @Test
    void keepsEverySplitInExactlyOnePlaceAcrossFailoverAndReassignment() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(2);
        try (TestEnumerator enumerator = started(context, 4)) {
            context.registerReader(0);
            context.registerReader(1);
            Set<String> handedOut = new HashSet<>();

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            assertThat(enumerator.snapshotState(1L).splits).hasSize(2);

            enumerator.addSplitsBack(context.assignedSplits(0), 0);
            assertThat(enumerator.snapshotState(2L).splits).hasSize(3);

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(1, "localhost");
            enumerator.handleSplitRequest(1, "localhost");

            context.assignedSplits(0).forEach(split -> handedOut.add(split.splitId()));
            context.assignedSplits(1).forEach(split -> handedOut.add(split.splitId()));
            assertThat(handedOut).hasSize(4);
            assertThat(enumerator.snapshotState(3L).splits).isEmpty();
            // Five assignments for four splits — the returned one and no other was handed out
            // twice. The distinct set alone would hide a split assigned to two readers at once.
            assertThat(context.events())
                    .filteredOn(event -> event.startsWith("assign:"))
                    .hasSize(5);
            assertThat(context.counter(SPLITS_ASSIGNED)).isEqualTo(5);
        }
    }

    @Test
    void assignsNothingWhenAReaderMerelyRegisters() throws Exception {
        // Assignment is pull-based, so registering is not a request: a reader that gets a split
        // here would get a second one when it then asks for one.
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (TestEnumerator enumerator = started(context, 1)) {
            context.registerReader(0);

            enumerator.addReader(0);

            assertThat(context.events()).isEmpty();
            assertThat(enumerator.snapshotState(1L).splits).hasSize(1);
        }
    }

    @Test
    void plansOnceOnAFreshStart() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedPlanner planner = ScriptedPlanner.planning(2);
        try (TestEnumerator enumerator = enumerator(context, planner)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(planner.planCalls).isEqualTo(1);
            assertThat(enumerator.planningStartedCalls).isEqualTo(1);
            assertThat(enumerator.snapshotState(1L).planned).isTrue();
            assertThat(context.counter(PLANS_COMPLETED)).isEqualTo(1);
        }
    }

    @Test
    void doesNotPlanWhenAPlanWasRestored() throws Exception {
        // The guard every one-shot plan needs: planning again would describe the source at a
        // second instant, under the split ids the readers are already holding.
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedPlanner planner = ScriptedPlanner.planning(2);
        TestState restored = new TestState(true, Collections.singletonList(new TestSplit("7")));
        try (TestEnumerator enumerator = new TestEnumerator(context, planner, restored)) {
            context.registerReader(0);
            enumerator.start();
            context.runAsyncCalls();
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(planner.planCalls).isZero();
            assertThat(enumerator.planningStartedCalls).isZero();
            assertThat(context.counter(PLANS_COMPLETED)).isZero();
            assertThat(context.assignedSplits(0))
                    .extracting(TestSplit::splitId)
                    .containsExactly("7");
        }
    }

    @Test
    void doesNotPlanWhenTheRestoredPlanIsEmpty() throws Exception {
        // "Planned" is not the same statement as "there is something pending": a plan fully handed
        // out must not be recomputed either.
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedPlanner planner = ScriptedPlanner.planning(2);
        TestState restored = new TestState(true, Collections.emptyList());
        try (TestEnumerator enumerator = new TestEnumerator(context, planner, restored)) {
            context.registerReader(0);
            enumerator.start();
            context.runAsyncCalls();
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(planner.planCalls).isZero();
            assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        }
    }

    @Test
    void failsTheJobWhenPlanningFails() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedPlanner planner =
                ScriptedPlanner.failingWith(new IllegalStateException("permission denied"));
        try (TestEnumerator enumerator = enumerator(context, planner)) {
            enumerator.start();

            assertThatThrownBy(context::runAsyncCalls)
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessage(PLAN_FAILURE_MESSAGE)
                    .hasRootCauseMessage("permission denied");
        }
    }

    @Test
    void staysQuietWhenPlanningFailsAfterItWasClosed() throws Exception {
        // A teardown must not turn a cancellation into a job failure.
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        TestEnumerator enumerator =
                enumerator(context, ScriptedPlanner.failingWith(new IllegalStateException("late")));

        enumerator.start();
        enumerator.close();
        context.runAsyncCalls();

        assertThat(context.events()).isEmpty();
    }

    @Test
    void plansNothingWhenPlanningSucceedsAfterItWasClosed() throws Exception {
        // The other half of the same guard: a plan landing after teardown must not be handed to a
        // context that is being torn down.
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        TestEnumerator enumerator = enumerator(context, ScriptedPlanner.planning(2));
        context.registerReader(0);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.close();
        context.runAsyncCalls();

        assertThat(context.events()).isEmpty();
        assertThat(context.counter(PLANS_COMPLETED)).isZero();
        assertThat(enumerator.snapshotState(1L).planned).isFalse();
        assertThat(enumerator.snapshotState(1L).splits).isEmpty();
    }

    @Test
    void closesThePlannerItOwns() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedPlanner planner = ScriptedPlanner.planning(1);
        TestEnumerator enumerator = enumerator(context, planner);

        enumerator.start();
        enumerator.close();

        assertThat(planner.closeCalls).isEqualTo(1);
    }

    @Test
    void reportsAPlannerThatRefusesToCloseAsAnIoException() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedPlanner planner = ScriptedPlanner.planning(1);
        planner.failOnClose(new IllegalStateException("still in use"));
        TestEnumerator enumerator = enumerator(context, planner);

        enumerator.start();

        assertThatThrownBy(enumerator::close)
                .isInstanceOf(IOException.class)
                .hasMessage(CLOSE_FAILURE_MESSAGE)
                .hasRootCauseMessage("still in use");
    }

    @Test
    void reportsWhatItAssignedReturnedAndPlanned() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (TestEnumerator enumerator = started(context, 2)) {
            context.registerReader(0);
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(context.counter(SPLITS_ASSIGNED)).isEqualTo(1);
            assertThat(context.counter(SPLITS_RETURNED)).isZero();
            assertThat(context.counter(PLANS_COMPLETED)).isEqualTo(1);
            assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(1L);

            enumerator.addSplitsBack(context.assignedSplits(0), 0);

            assertThat(context.counter(SPLITS_RETURNED)).isEqualTo(1);
            assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        }
    }

    @Test
    void runsWithoutAMetricGroup() throws Exception {
        // Defensive, and cheap to hold: SplitEnumeratorContext#metricGroup() carries no nullability
        // annotation, and a context answering with nothing must not fail the job at startup.
        FakeSplitEnumeratorContext<TestSplit> delegate = new FakeSplitEnumeratorContext<>(1);
        WithoutMetrics context = new WithoutMetrics(delegate);
        try (TestEnumerator enumerator = enumerator(context, ScriptedPlanner.planning(1))) {
            delegate.registerReader(0);
            enumerator.start();
            context.runAsyncCalls();

            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(delegate.events()).containsExactly("assign:0", "noMoreSplits:0");
        }
    }

    @Test
    void checkpointsACopyOfTheQueue() throws Exception {
        FakeSplitEnumeratorContext<TestSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (TestEnumerator enumerator = started(context, 2)) {
            context.registerReader(0);
            TestState snapshot = enumerator.snapshotState(1L);

            enumerator.handleSplitRequest(0, "localhost");

            // A snapshot the enumerator kept handing out of would describe the queue as it is at
            // recovery time rather than as it was at the checkpoint.
            assertThat(snapshot.splits).hasSize(2);
        }
    }

    private static TestEnumerator enumerator(
            SplitEnumeratorContext<TestSplit> context, ScriptedPlanner planner) {
        return new TestEnumerator(context, planner, null);
    }

    /** An enumerator whose plan has completed, with the given number of splits. */
    private static TestEnumerator started(
            FakeSplitEnumeratorContext<TestSplit> context, int splitCount) {
        TestEnumerator enumerator = enumerator(context, ScriptedPlanner.planning(splitCount));
        enumerator.start();
        context.runAsyncCalls();
        return enumerator;
    }

    /** A split that is nothing but its id. */
    private static final class TestSplit implements SourceSplit {

        private final String id;

        private TestSplit(String id) {
            this.id = id;
        }

        @Override
        public String splitId() {
            return id;
        }

        @Override
        public String toString() {
            return "TestSplit{" + id + "}";
        }
    }

    /** What a checkpoint of the test enumerator holds. */
    private static final class TestState {

        private final boolean planned;
        private final List<TestSplit> splits;

        private TestState(boolean planned, List<TestSplit> splits) {
            this.planned = planned;
            this.splits = splits;
        }
    }

    /** The one asynchronous step, scripted: it answers with splits, or it fails. */
    private static final class ScriptedPlanner implements AutoCloseable {

        private final int splitCount;
        @Nullable private final Exception failure;
        @Nullable private RuntimeException closeFailure;

        private int planCalls;
        private int closeCalls;

        private ScriptedPlanner(int splitCount, @Nullable Exception failure) {
            this.splitCount = splitCount;
            this.failure = failure;
        }

        static ScriptedPlanner planning(int splitCount) {
            return new ScriptedPlanner(splitCount, null);
        }

        static ScriptedPlanner failingWith(Exception failure) {
            return new ScriptedPlanner(0, failure);
        }

        void failOnClose(RuntimeException failure) {
            this.closeFailure = failure;
        }

        List<TestSplit> plan() throws Exception {
            planCalls++;
            if (failure != null) {
                throw failure;
            }
            List<TestSplit> splits = new ArrayList<>();
            for (int i = 0; i < splitCount; i++) {
                splits.add(new TestSplit(String.valueOf(i)));
            }
            return splits;
        }

        @Override
        public void close() {
            closeCalls++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    /** The least a connector has to supply to get the protocol. */
    private static final class TestEnumerator
            extends PullAssignmentSplitEnumerator<TestSplit, TestState, List<TestSplit>> {

        private final ScriptedPlanner planner;
        @Nullable private final TestState restoredState;

        private int planningStartedCalls;

        private TestEnumerator(
                SplitEnumeratorContext<TestSplit> context,
                ScriptedPlanner planner,
                @Nullable TestState restoredState) {
            super(context, planner, "test split", PLAN_FAILURE_MESSAGE, CLOSE_FAILURE_MESSAGE);
            this.planner = planner;
            this.restoredState = restoredState;
        }

        @Override
        protected boolean restore() {
            if (restoredState == null || !restoredState.planned) {
                return false;
            }
            addPlannedSplits(restoredState.splits);
            return true;
        }

        @Override
        protected void onPlanningStarted() {
            planningStartedCalls++;
        }

        @Override
        protected List<TestSplit> plan() throws Exception {
            return planner.plan();
        }

        @Override
        protected void onPlanned(List<TestSplit> plan) {
            addPlannedSplits(plan);
        }

        @Override
        protected EnumeratorCounters registerCounters(SplitEnumeratorMetricGroup metricGroup) {
            return new EnumeratorCounters(
                    metricGroup.counter(SPLITS_ASSIGNED, new ThreadSafeSimpleCounter()),
                    metricGroup.counter(SPLITS_RETURNED, new ThreadSafeSimpleCounter()),
                    metricGroup.counter(PLANS_COMPLETED, new ThreadSafeSimpleCounter()));
        }

        @Override
        public TestState snapshotState(long checkpointId) {
            return new TestState(isPlanned(), pendingSplits());
        }
    }

    /**
     * A context that offers no metric group, which the shared fake cannot express and Flink's own
     * contexts never do.
     */
    private static final class WithoutMetrics implements SplitEnumeratorContext<TestSplit> {

        private final FakeSplitEnumeratorContext<TestSplit> delegate;

        private WithoutMetrics(FakeSplitEnumeratorContext<TestSplit> delegate) {
            this.delegate = delegate;
        }

        void runAsyncCalls() {
            delegate.runAsyncCalls();
        }

        @Override
        @Nullable
        public SplitEnumeratorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            delegate.sendEventToSourceReader(subtaskId, event);
        }

        @Override
        public int currentParallelism() {
            return delegate.currentParallelism();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return delegate.registeredReaders();
        }

        @Override
        public void assignSplits(SplitsAssignment<TestSplit> newSplitAssignments) {
            delegate.assignSplits(newSplitAssignments);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            delegate.signalNoMoreSplits(subtask);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            delegate.callAsync(callable, handler);
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelayMillis,
                long periodMillis) {
            delegate.callAsync(callable, handler, initialDelayMillis, periodMillis);
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            delegate.runInCoordinatorThread(runnable);
        }
    }
}
