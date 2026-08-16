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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.util.FlinkRuntimeException;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What this enumerator adds to the shared protocol: the sampled plan.
 *
 * <p>The assignment protocol itself — parking, serving, no-more-splits, a returned split — is
 * {@code PullAssignmentSplitEnumeratorTest}'s, in {@code flink-connector-gcp-base}, and what the
 * plan contains is {@link RowRangeSplitPlannerTest}'s. The cases here are the ones that would pass
 * in either of those places and still be wrong here: a second sampling, a sampler left open, a
 * sampling failure softened into a plan, a metric under the wrong name.
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
    void samplesTheTableExactlyOnce() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(2);
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
    void assignsEveryPlannedSplitExactlyOnce() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
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

        // In the plan's own order, ids included: a reader's restored split id has to keep naming
        // the range the plan gave it.
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
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedRowKeySampler sampler = ScriptedRowKeySampler.answering(sample("m", 100));
        BigtableScanEnumeratorState restored =
                new BigtableScanEnumeratorState(
                        true,
                        Collections.singletonList(
                                new RowRangeSplit("7", ByteStringRange.unbounded())));
        BigtableScanSplitEnumerator enumerator =
                new BigtableScanSplitEnumerator(context, configWith(sampler), restored);
        context.registerReader(0);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(sampler.sampleCalls()).isZero();
        assertThat(context.counter("rowKeySamplesTaken")).isZero();
        assertThat(context.assignedSplits(0))
                .extracting(RowRangeSplit::splitId)
                .containsExactly("7");
        enumerator.close();
    }

    @Test
    void aRestoredEnumeratorWithAnEmptyPlanDoesNotSampleEither() throws Exception {
        // "Planned" is not the same statement as "there is something pending": a plan fully handed
        // out must not be recomputed.
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
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
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedRowKeySampler.failingWith(
                                new IllegalStateException("permission denied")));

        enumerator.start();

        // Not softened into a single unsplit plan: that would read the whole table on one subtask
        // for reasons nothing reports.
        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to sample the row keys of")
                .hasRootCauseMessage("permission denied");
        enumerator.close();
    }

    @Test
    void closesTheSamplerItOwns() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        ScriptedRowKeySampler sampler = ScriptedRowKeySampler.answering();
        BigtableScanSplitEnumerator enumerator = enumerator(context, sampler);

        enumerator.start();
        enumerator.close();

        assertThat(sampler.closeCalls()).isEqualTo(1);
    }

    @Test
    void reportsWhatItAssignedReturnedAndSampled() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        BigtableScanSplitEnumerator enumerator =
                enumerator(context, ScriptedRowKeySampler.answering(sample("m", 100)));
        context.registerReader(0);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.counter("splitsAssigned")).isEqualTo(1);
        assertThat(context.counter("rowKeySamplesTaken")).isEqualTo(1);
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(1L);

        enumerator.addSplitsBack(context.assignedSplits(0), 0);

        assertThat(context.counter("splitsReturned")).isEqualTo(1);
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        enumerator.close();
    }

    @Test
    void checkpointsThePlanAndWhatIsLeftOfIt() throws Exception {
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
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
