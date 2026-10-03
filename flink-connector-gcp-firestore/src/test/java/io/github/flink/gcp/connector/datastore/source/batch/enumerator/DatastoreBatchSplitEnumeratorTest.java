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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchEnumeratorState;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What this enumerator adds to the shared protocol: one plan, at one read time, never made twice.
 *
 * <p>The assignment protocol itself — parking, serving, no-more-splits, a returned split, a plan
 * completing after close — is {@code PullAssignmentSplitEnumeratorTest}'s, in {@code
 * flink-connector-gcp-base}.
 */
@Timeout(30)
class DatastoreBatchSplitEnumeratorTest {

    private static DatastoreSourceConfig<String> config() {
        return TestSources.kindConfig(UnaryOperator.identity());
    }

    @Test
    void plansOnceAtTheParallelismAndCarriesTheReadTimeIntoEverySplit() throws Exception {
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(3);
        ScriptedQueryPlanner planner = ScriptedQueryPlanner.answering("a", "b");
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(context, config(), planner, null);
        context.registerReader(0);

        enumerator.start();
        context.runAsyncCalls();
        for (int i = 0; i < 3; i++) {
            enumerator.handleSplitRequest(0, "localhost");
        }

        assertThat(planner.parallelisms()).containsExactly(3);
        // In the plan's order, ids included: a restored reader's split id keeps naming its query.
        assertThat(context.assignedSplits(0))
                .extracting(QuerySplit::splitId)
                .containsExactly("0", "1");
        assertThat(context.assignedSplits(0))
                .extracting(QuerySplit::getReadTime)
                .containsOnly(ScriptedQueryPlanner.READ_TIME);
        assertThat(context.assignedSplits(0))
                .extracting(split -> split.getRequest().getQuery().getKind(0).getName())
                .containsExactly("a", "b");
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        assertThat(context.counter("readsPlanned")).isOne();
        assertThat(context.counter("splitsAssigned")).isEqualTo(2);
        enumerator.close();
    }

    @Test
    void handsOutTheOneSplitOfAQueryThatCouldNotBeSplit() throws Exception {
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(2);
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        context,
                        config(),
                        ScriptedQueryPlanner.answeringUnsplittable("it has a limit"),
                        null);
        context.registerReader(0);
        context.registerReader(1);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");

        assertThat(context.assignedSplits(0)).extracting(QuerySplit::splitId).containsExactly("0");
        assertThat(context.assignedSplits(1)).isEmpty();
        assertThat(context.readersToldNoMoreSplits()).containsExactly(1);
        enumerator.close();
    }

    @Test
    void aRestoredEnumeratorDoesNotPlanAgain() throws Exception {
        // A second plan would read part of the data at a second read time.
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedQueryPlanner planner = ScriptedQueryPlanner.answering("a");
        QuerySplit held =
                new QuerySplit(
                        "7", ScriptedQueryPlanner.request("held"), ScriptedQueryPlanner.READ_TIME);
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        context,
                        config(),
                        planner,
                        new DatastoreBatchEnumeratorState(true, List.of(held)));
        context.registerReader(0);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(planner.parallelisms()).isEmpty();
        assertThat(context.counter("readsPlanned")).isZero();
        assertThat(context.assignedSplits(0)).containsExactly(held);
        enumerator.close();
    }

    @Test
    void aRestoredEnumeratorWithAnEmptyPlanDoesNotPlanEither() throws Exception {
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedQueryPlanner planner = ScriptedQueryPlanner.answering("a");
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        context,
                        config(),
                        planner,
                        new DatastoreBatchEnumeratorState(true, Collections.emptyList()));
        context.registerReader(0);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(planner.parallelisms()).isEmpty();
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        enumerator.close();
    }

    @Test
    void failsTheJobWhenPlanningFails() throws Exception {
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        context,
                        TestSources.kindConfig(builder -> builder.namespace("tenant")),
                        ScriptedQueryPlanner.failingWith(
                                new IllegalStateException("permission denied")),
                        null);

        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Failed to plan the Datastore read of a query of kind 'Task' in namespace"
                                + " 'tenant' of projects/p/databases/(default)")
                .hasRootCauseMessage("permission denied");
        enumerator.close();
    }

    @Test
    void namesAGqlQueryWhenPlanningItFails() throws Exception {
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        context,
                        TestSources.source(builder -> builder.gqlQuery("SELECT * FROM Task"))
                                .getConfig(),
                        ScriptedQueryPlanner.failingWith(new IllegalStateException("bad GQL")),
                        null);

        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .hasMessageContaining(
                        "Failed to plan the Datastore read of a GQL query in the default"
                                + " namespace");
        enumerator.close();
    }

    @Test
    void closesThePlannerItOwns() throws Exception {
        ScriptedQueryPlanner planner = ScriptedQueryPlanner.answering("a");
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        new FakeSplitEnumeratorContext<>(1), config(), planner, null);

        enumerator.start();
        enumerator.close();

        assertThat(planner.closeCalls()).isOne();
    }

    @Test
    void checkpointsThePlanAndWhatIsLeftOfIt() throws Exception {
        FakeSplitEnumeratorContext<QuerySplit> context = new FakeSplitEnumeratorContext<>(1);
        DatastoreBatchSplitEnumerator enumerator =
                new DatastoreBatchSplitEnumerator(
                        context, config(), ScriptedQueryPlanner.answering("a", "b"), null);
        context.registerReader(0);

        assertThat(enumerator.snapshotState(1L).isPlanned()).isFalse();
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        DatastoreBatchEnumeratorState state = enumerator.snapshotState(2L);
        assertThat(state.isPlanned()).isTrue();
        assertThat(state.getPendingSplits()).extracting(QuerySplit::splitId).containsExactly("1");
        enumerator.close();
    }
}
