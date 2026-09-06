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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Finite, checkpointable input that stays idle without blocking barriers until explicitly released.
 */
final class LocalStagedSource extends NumberSequenceSource {
    private static final long serialVersionUID = 1L;
    private final String runId;

    LocalStagedSource(String runId, long count) {
        super(0, count - 1);
        this.runId = runId;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Long, NumberSequenceSplit> createReader(SourceReaderContext context) {
        SourceReader<Long, NumberSequenceSplit> delegate = super.createReader(context);
        return new SourceReader<>() {
            private boolean exhausted;

            @Override
            public void start() {
                delegate.start();
                LocalStagedHarness.run(runId).readersStarted.incrementAndGet();
            }

            @Override
            public InputStatus pollNext(ReaderOutput<Long> output) throws Exception {
                if (!LocalStagedHarness.run(runId).allowInputs.get()) {
                    return InputStatus.NOTHING_AVAILABLE;
                }
                if (!exhausted) {
                    InputStatus status = delegate.pollNext(output);
                    if (status != InputStatus.END_OF_INPUT) {
                        return status;
                    }
                    exhausted = true;
                }
                return LocalStagedHarness.run(runId).finishSource.get()
                        ? InputStatus.END_OF_INPUT
                        : InputStatus.NOTHING_AVAILABLE;
            }

            @Override
            public List<NumberSequenceSplit> snapshotState(long checkpointId) {
                return delegate.snapshotState(checkpointId);
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                if (!LocalStagedHarness.run(runId).allowInputs.get()) {
                    return LocalStagedHarness.run(runId).inputsAllowed;
                }
                return exhausted
                        ? LocalStagedHarness.run(runId).sourceReleased
                        : delegate.isAvailable();
            }

            @Override
            public void addSplits(List<NumberSequenceSplit> splits) {
                exhausted = false;
                delegate.addSplits(splits);
            }

            @Override
            public void notifyNoMoreSplits() {
                delegate.notifyNoMoreSplits();
            }

            @Override
            public void close() throws Exception {
                delegate.close();
            }
        };
    }
}
