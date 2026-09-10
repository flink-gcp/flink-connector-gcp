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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
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
        Stage2Admission.Reader credit =
                run.admission == null ? null : run.admission.reader(context.getIndexOfSubtask());
        return new SourceReader<>() {
            private ReaderOutput<Long> originalOutput;
            private ReaderOutput<Long> creditOutput;

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
                    // A full credit window can hide the delegate's final exhaustion poll.
                    if (credit != null && run.ledger.admittedCount() == run.ledger.capacity) {
                        run.censored.set(true);
                    }
                    return InputStatus.END_OF_INPUT;
                }
                if (credit != null && !credit.hasRoom()) {
                    return InputStatus.NOTHING_AVAILABLE;
                }
                if (credit != null && originalOutput != output) {
                    originalOutput = output;
                    creditOutput = new CreditOutput(output, credit);
                }
                long before = System.nanoTime();
                InputStatus status = delegate.pollNext(credit == null ? output : creditOutput);
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
                if (!run.allowInputs.get()) {
                    return run.inputsAllowed;
                }
                return credit == null
                        ? delegate.isAvailable()
                        : credit.available(delegate.isAvailable());
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
                try {
                    delegate.close();
                } finally {
                    if (credit != null) {
                        credit.stop();
                    }
                }
            }
        };
    }

    private static class CreditSourceOutput implements SourceOutput<Long> {
        private final SourceOutput<Long> output;
        private final Stage2Admission.Reader credit;

        CreditSourceOutput(SourceOutput<Long> output, Stage2Admission.Reader credit) {
            this.output = output;
            this.credit = credit;
        }

        @Override
        public void collect(Long record) {
            credit.emitted(record);
            output.collect(record);
        }

        @Override
        public void collect(Long record, long timestamp) {
            credit.emitted(record);
            output.collect(record, timestamp);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            output.emitWatermark(watermark);
        }

        @Override
        public void markIdle() {
            output.markIdle();
        }

        @Override
        public void markActive() {
            output.markActive();
        }
    }

    private static final class CreditOutput extends CreditSourceOutput
            implements ReaderOutput<Long> {
        private final ReaderOutput<Long> output;
        private final Stage2Admission.Reader credit;

        CreditOutput(ReaderOutput<Long> output, Stage2Admission.Reader credit) {
            super(output, credit);
            this.output = output;
            this.credit = credit;
        }

        @Override
        public SourceOutput<Long> createOutputForSplit(String splitId) {
            return new CreditSourceOutput(output.createOutputForSplit(splitId), credit);
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            output.releaseOutputForSplit(splitId);
        }
    }
}
