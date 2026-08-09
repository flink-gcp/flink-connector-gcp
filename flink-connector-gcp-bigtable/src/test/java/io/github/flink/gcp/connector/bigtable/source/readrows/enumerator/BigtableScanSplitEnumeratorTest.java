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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableScanSplitEnumerator}.
 *
 * <p>Aimed at the assignment protocol rather than at the plan, which {@link
 * RowRangeSplitPlannerTest} covers: assignment and completion is where a hand-written enumerator
 * loses a split without anything reporting it.
 */
@Timeout(30)
class BigtableScanSplitEnumeratorTest {

    private static RowKeySample sample(String key, long offsetBytes) {
        return RowKeySample.of(ByteString.copyFromUtf8(key), offsetBytes);
    }

    private static BigtableSourceConfig<String> configWith(ScriptedRowKeySampler sampler) {
        return TestSources.config(builder -> TestSources.withSampler(builder, sampler));
    }

    private BigtableScanSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<RowRangeSplit> context, ScriptedRowKeySampler sampler) {
        return new BigtableScanSplitEnumerator(context, configWith(sampler), null);
    }

    @Test
    void samplesOnceAndHandsOutOneSplitPerRequest() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(2);
        ScriptedRowKeySampler sampler =
                ScriptedRowKeySampler.answering(sample("m", 100), sample("t", 200));
        BigtableScanSplitEnumerator enumerator = enumerator(context, sampler);
        context.registerReader(0);
        context.registerReader(1);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");

        assertThat(sampler.sampleCalls()).isEqualTo(1);
        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        enumerator.close();
    }

    @Test
    void parksRequestsThatArriveBeforeThePlanAndServesThemInOrder() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(2);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);
        context.registerReader(1);

        enumerator.start();
        enumerator.handleSplitRequest(1, "localhost");
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.events()).isEmpty();

        context.runAsyncCalls();

        assertThat(context.events()).containsExactly("assign:1", "assign:0");
        enumerator.close();
    }

    @Test
    void finishesASubtaskThereIsNoSplitLeftFor() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(2);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering());
        context.registerReader(0);
        context.registerReader(1);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");

        // One unsampled table means one split, so the second subtask has nothing to read.
        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).isEmpty();
        assertThat(context.readersToldNoMoreSplits()).containsExactly(1);
        enumerator.close();
    }

    @Test
    void skipsARequestFromASubtaskThatWentAwayWhileItWasParked() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(2);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.unregisterReader(0);
        context.runAsyncCalls();

        // Assigning to an unregistered subtask is what the fake refuses; the point is that the
        // enumerator checks first, and that the split stays available for whoever comes back.
        assertThat(context.events()).isEmpty();
        assertThat(enumerator.snapshotState(1L).getPendingSplits()).hasSize(2);
        enumerator.close();
    }

    @Test
    void reassignsSplitsAFailedReaderReturned() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        List<RowRangeSplit> assigned = context.assignedSplits(0);

        enumerator.addSplitsBack(assigned, 0);
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(0, "localhost");

        // A returned split goes to the back of the queue, so it is handed out after whatever was
        // already waiting — what matters is that it is handed out at all.
        assertThat(context.assignedSplits(0)).hasSize(3).contains(assigned.get(0));
        assertThat(context.assignedSplits(0).subList(1, 3))
                .extracting(RowRangeSplit::splitId)
                .containsExactly("1", "0");
        enumerator.close();
    }

    @Test
    void assignsEveryPlannedSplitExactlyOnce() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedRowKeySampler.answering(
                                sample("c", 100), sample("m", 200), sample("t", 300)));
        context.registerReader(0);
        enumerator.start();
        context.runAsyncCalls();

        for (int i = 0; i < 5; i++) {
            enumerator.handleSplitRequest(0, "localhost");
        }

        assertThat(context.assignedSplits(0))
                .extracting(RowRangeSplit::splitId)
                .containsExactly("0", "1", "2", "3");
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        enumerator.close();
    }

    @Test
    void aRestoredEnumeratorDoesNotSampleAgain() throws Exception {
        // The invariant that keeps split ids meaning what the readers think they mean: tablets
        // split while a job runs, so a second sampling would renumber everything.
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        ScriptedRowKeySampler sampler = ScriptedRowKeySampler.answering(sample("m", 100));
        BigtableScanEnumeratorState restored =
                new BigtableScanEnumeratorState(
                        true,
                        Collections.singletonList(
                                new RowRangeSplit(
                                        "7",
                                        com.google.cloud.bigtable.data.v2.models.Range
                                                .ByteStringRange.unbounded())));
        BigtableScanSplitEnumerator enumerator =
                new BigtableScanSplitEnumerator(context, configWith(sampler), restored);
        context.registerReader(0);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(sampler.sampleCalls()).isZero();
        assertThat(context.counter(BigtableMetricNames.ROW_KEY_SAMPLES_TAKEN)).isZero();
        assertThat(context.assignedSplits(0))
                .extracting(RowRangeSplit::splitId)
                .containsExactly("7");
        enumerator.close();
    }

    @Test
    void aRestoredEnumeratorWithAnEmptyPlanDoesNotSampleEither() throws Exception {
        // "Planned" is not the same statement as "there is something pending": a plan fully handed
        // out must not be recomputed.
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        ScriptedRowKeySampler sampler = ScriptedRowKeySampler.answering(sample("m", 100));
        BigtableScanSplitEnumerator enumerator =
                new BigtableScanSplitEnumerator(
                        context,
                        configWith(sampler),
                        new BigtableScanEnumeratorState(true, Collections.emptyList()));
        context.registerReader(0);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(sampler.sampleCalls()).isZero();
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        enumerator.close();
    }

    @Test
    void failsTheJobWhenSamplingFails() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedRowKeySampler.failingWith(
                                new IllegalStateException("permission denied")));

        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to sample the row keys of")
                .hasRootCauseMessage("permission denied");
        enumerator.close();
    }

    @Test
    void staysQuietWhenSamplingCompletesAfterTheEnumeratorWasClosed() throws Exception {
        // A teardown must not turn a cancellation into a job failure.
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedRowKeySampler.failingWith(new IllegalStateException("too late")));

        enumerator.start();
        enumerator.close();
        context.runAsyncCalls();

        assertThat(context.events()).isEmpty();
    }

    @Test
    void plansNothingWhenSamplingSucceedsAfterTheEnumeratorWasClosed() throws Exception {
        // The other half of the same guard: a successful sample landing after teardown must not
        // plan splits and serve them into a context that is being torn down.
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.close();
        context.runAsyncCalls();

        assertThat(context.events()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.ROW_KEY_SAMPLES_TAKEN)).isZero();
    }

    @Test
    void closesTheSamplerItOwns() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        ScriptedRowKeySampler sampler = ScriptedRowKeySampler.answering();
        BigtableScanSplitEnumerator enumerator = enumerator(context, sampler);

        enumerator.start();
        enumerator.close();

        assertThat(sampler.closeCalls()).isEqualTo(1);
    }

    @Test
    void reportsWhatItAssignedReturnedAndSampled() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.counter(BigtableMetricNames.SPLITS_ASSIGNED)).isEqualTo(1);
        assertThat(context.counter(BigtableMetricNames.ROW_KEY_SAMPLES_TAKEN)).isEqualTo(1);
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(1L);

        enumerator.addSplitsBack(context.assignedSplits(0), 0);

        assertThat(context.counter(BigtableMetricNames.SPLITS_RETURNED)).isEqualTo(1);
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        enumerator.close();
    }

    @Test
    void checkpointsThePlanAndWhatIsLeftOfIt() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context =
                new FakeSplitEnumeratorContext<RowRangeSplit>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);

        assertThat(enumerator.snapshotState(1L).isPlanned()).isFalse();

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        BigtableScanEnumeratorState state = enumerator.snapshotState(2L);

        assertThat(state.isPlanned()).isTrue();
        assertThat(state.getPendingSplits()).hasSize(1);
        enumerator.close();
    }
}
