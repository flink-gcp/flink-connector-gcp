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

/** Checkpointable sequences admitted until a wall-clock deadline, with ordinary backpressure. */
final class Stage2Source extends NumberSequenceSource {
    private static final long serialVersionUID = 1L;
    private final String runId;

    Stage2Source(String runId, long count) {
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
        Stage2Harness run = (Stage2Harness) LocalStagedHarness.run(runId);
        return new SourceReader<>() {

            @Override
            public void start() {
                delegate.start();
                run.readersStarted.incrementAndGet();
            }

            @Override
            public InputStatus pollNext(ReaderOutput<Long> output) throws Exception {
                if (!run.allowInputs.get()) {
                    return InputStatus.NOTHING_AVAILABLE;
                }
                if (run.windowEnded(System.nanoTime())) {
                    return InputStatus.END_OF_INPUT;
                }
                long before = System.nanoTime();
                InputStatus status = delegate.pollNext(output);
                run.sourcePollNanos.addAndGet(System.nanoTime() - before);
                if (status == InputStatus.END_OF_INPUT) {
                    run.censored.set(true);
                } else if (status == InputStatus.NOTHING_AVAILABLE) {
                    run.sourceStarvations.incrementAndGet();
                }
                return status;
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return !run.allowInputs.get() ? run.inputsAllowed : delegate.isAvailable();
            }

            @Override
            public List<NumberSequenceSplit> snapshotState(long checkpointId) {
                return delegate.snapshotState(checkpointId);
            }

            @Override
            public void addSplits(List<NumberSequenceSplit> splits) {
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
