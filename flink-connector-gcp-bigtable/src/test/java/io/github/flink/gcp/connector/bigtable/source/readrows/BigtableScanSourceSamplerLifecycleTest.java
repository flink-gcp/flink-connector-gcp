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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.api.connector.source.SplitEnumerator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySample;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.ScriptedRowKeySampler;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that one sampler belongs to one enumerator.
 *
 * <p>The JobManager keeps one source object for a job's whole life, and a coordinator reset builds
 * the next enumerator from it — so a sampler carried on the source configuration would already have
 * been closed by the enumerator before it. Issue #990 was that in the Spanner batch source, and the
 * scan source had the same shape, held one level down in {@code LazyBigtableDataClient}.
 *
 * <p>These tests therefore do the one thing nothing else does: two enumerators over <em>one</em>
 * source object, with a teardown between them.
 */
class BigtableScanSourceSamplerLifecycleTest {

    private static ScriptedRowKeySampler.Factory samplers() {
        return ScriptedRowKeySampler.Factory.answering(
                RowKeySample.of(ByteString.copyFromUtf8("m"), 100L));
    }

    private static BigtableScanSource<String> source(ScriptedRowKeySampler.Factory samplers) {
        return TestSources.source(builder -> TestSources.withSamplerFactory(builder, samplers));
    }

    @Test
    void aSecondEnumeratorSamplesThroughItsOwnSampler() throws Exception {
        ScriptedRowKeySampler.Factory samplers = samplers();
        BigtableScanSource<String> source = source(samplers);

        FakeSplitEnumeratorContext<RowRangeSplit> firstContext =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }

        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> second =
                source.createEnumerator(context)) {
            second.start();
            context.runAsyncCalls();

            assertThat(second.snapshotState(1L).isPlanned())
                    .as("the second enumerator samples instead of meeting a closed sampler")
                    .isTrue();
        }
        assertThat(samplers.minted()).hasSize(2);
        assertThat(samplers.minted().get(0).sampleCalls()).isOne();
        assertThat(samplers.minted().get(1).sampleCalls()).isOne();
    }

    @Test
    void eachEnumeratorGetsItsOwnSampler() throws Exception {
        ScriptedRowKeySampler.Factory samplers = samplers();
        BigtableScanSource<String> source = source(samplers);

        source.createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();
        SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> second =
                source.createEnumerator(new FakeSplitEnumeratorContext<>(1));

        assertThat(samplers.minted()).hasSize(2);
        assertThat(samplers.minted().get(0)).isNotSameAs(samplers.minted().get(1));
        assertThat(samplers.minted().get(0).isClosed())
                .as("the first enumerator's sampler is the one its teardown ended")
                .isTrue();
        assertThat(samplers.minted().get(1).isClosed())
                .as("the second enumerator's sampler is untouched by that teardown")
                .isFalse();
        assertThat(samplers.minted().get(0).closeCalls())
                .as("one teardown closed it once, rather than twice")
                .isOne();

        second.close();
    }

    @Test
    void aRestoreFromAPlannedCheckpointMintsItsOwnSamplerAndOpensNothing() throws Exception {
        // The call the real restore makes, with the state it really carries. Passing a null
        // checkpoint would make restoreEnumerator behave exactly like createEnumerator, so it
        // would assert nothing the test above does not.
        ScriptedRowKeySampler.Factory samplers = samplers();
        BigtableScanSource<String> source = source(samplers);

        FakeSplitEnumeratorContext<RowRangeSplit> firstContext =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }

        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> restored =
                source.restoreEnumerator(
                        context, new BigtableScanEnumeratorState(true, Collections.emptyList()))) {
            restored.start();
            context.runAsyncCalls();

            assertThat(restored.snapshotState(1L).isPlanned())
                    .as("the restore adopts the checkpointed plan")
                    .isTrue();
        }

        assertThat(samplers.minted())
                .as("the restore mints a sampler of its own rather than reusing a closed one")
                .hasSize(2);
        assertThat(samplers.minted().get(1).sampleCalls())
                .as("and samples through neither: only the first enumerator sampled")
                .isZero();
        assertThat(samplers.minted().get(1).closeCalls())
                .as("the sampler it minted and never used is still closed exactly once")
                .isOne();
    }

    /**
     * Pins the window minting opens: between {@code create()} and the enumerator taking ownership,
     * the source is the only thing that can release the sampler.
     */
    @Test
    void theSourceClosesASamplerItCouldNotHandOver() {
        ScriptedRowKeySampler.Factory samplers = samplers();
        BigtableScanSource<String> source = source(samplers);

        assertThatThrownBy(() -> source.createEnumerator(null))
                .isInstanceOf(NullPointerException.class);

        assertThat(samplers.minted()).hasSize(1);
        assertThat(samplers.only().closeCalls())
                .as("a sampler the enumerator never took is released by the source")
                .isOne();
    }
}
