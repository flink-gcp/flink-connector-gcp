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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.core.io.InputStatus;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(20)
class Stage2AdmissionTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(longs = {0, 1000})
    void fullInventoryRemainsCensoredWhenTheLastCreditWaitsUntilWindowEnd(long warmupMillis)
            throws Exception {
        try (Stage2Harness run =
                        new Stage2Harness(
                                TableDestination.of(
                                        "local-project", "local-instance", "local-table"),
                                "127.0.0.1:1",
                                directory.resolve("last-credit"),
                                1,
                                1024,
                                false,
                                false,
                                1,
                                true,
                                null,
                                1);
                var reader =
                        new Stage2Source(run.id, 1)
                                .createReader(new FakeSourceReaderContext(null))) {
            reader.start();
            reader.addSplits(List.of(new NumberSequenceSplit("0", 0, 0)));
            reader.notifyNoMoreSplits();
            var output = new CollectingReaderOutput<Long>();
            run.startWindow(
                    System.nanoTime(),
                    TimeUnit.MILLISECONDS.toNanos(warmupMillis),
                    TimeUnit.SECONDS.toNanos(5));
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            run.admitted(output.records().get(0));
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            reader.isAvailable().get(10, TimeUnit.SECONDS);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
            assertThat(run.censored.get()).isTrue();
            run.acknowledged(0, System.nanoTime());
            assertThat(run.ledger.acknowledgedCount()).isEqualTo(1);
        }
    }

    @Test
    void actualSourceChargesBeforeSynchronousDownstreamAcknowledgement() throws Exception {
        try (Stage2Harness run =
                        new Stage2Harness(
                                TableDestination.of(
                                        "local-project", "local-instance", "local-table"),
                                "127.0.0.1:1",
                                directory.resolve("synchronous-inventory"),
                                4,
                                1024,
                                false,
                                false,
                                1,
                                true,
                                null,
                                1);
                var reader =
                        new Stage2Source(run.id, 4)
                                .createReader(new FakeSourceReaderContext(null))) {
            reader.start();
            reader.addSplits(List.of(new NumberSequenceSplit("0", 0, 3)));
            reader.notifyNoMoreSplits();
            run.startWindow(System.nanoTime(), 0, TimeUnit.MINUTES.toNanos(1));
            ReaderOutput<Long> output =
                    new ReaderOutput<>() {
                        @Override
                        public void collect(Long value) {
                            run.admitted(value);
                            run.acknowledged(value, System.nanoTime());
                        }

                        @Override
                        public void collect(Long value, long timestamp) {
                            collect(value);
                        }

                        @Override
                        public void emitWatermark(Watermark watermark) {}

                        @Override
                        public void markIdle() {}

                        @Override
                        public void markActive() {}

                        @Override
                        public SourceOutput<Long> createOutputForSplit(String splitId) {
                            return this;
                        }

                        @Override
                        public void releaseOutputForSplit(String splitId) {}
                    };
            for (int input = 0; input < 3; input++) {
                assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
                assertThat(reader.isAvailable()).isCompleted();
            }
            assertThat(run.ledger.admittedCount()).isEqualTo(3);
            assertThat(run.ledger.acknowledgedCount()).isEqualTo(3);
            var sample = new ObjectMapper().readTree(run.admission.sample());
            assertThat(sample.path("peakPendingPerSubtask").asInt()).isEqualTo(1);
            assertThat(sample.path("pendingInputs").asInt()).isZero();
        }
    }

    @Test
    void actualSourcePreservesItsSplitAndWakesAtWindowEndWhileCreditsAreFull() throws Exception {
        try (Stage2Harness run =
                        new Stage2Harness(
                                TableDestination.of(
                                        "local-project", "local-instance", "local-table"),
                                "127.0.0.1:1",
                                directory.resolve("source-inventory"),
                                4,
                                1024,
                                false,
                                false,
                                1,
                                true,
                                null,
                                1);
                var reader =
                        new Stage2Source(run.id, 4)
                                .createReader(new FakeSourceReaderContext(null))) {
            reader.start();
            reader.addSplits(List.of(new NumberSequenceSplit("0", 0, 3)));
            reader.notifyNoMoreSplits();
            var output = new CollectingReaderOutput<Long>();
            run.startWindow(System.nanoTime(), 0, TimeUnit.SECONDS.toNanos(1));
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(reader.snapshotState(1)).hasSize(1);
            assertThat(reader.snapshotState(1).get(0).getIterator().next()).isEqualTo(1L);
            var waiting = reader.isAvailable();
            waiting.get(5, TimeUnit.SECONDS);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
            assertThat(run.censored.get()).isFalse();
            assertThat(
                            new ObjectMapper()
                                    .readTree(run.admission.sample())
                                    .path("pendingInputs")
                                    .asInt())
                    .isEqualTo(1);
        }
    }

    @Test
    void creditsBelongToEachReaderAndSourceAvailabilityMustAlsoComplete() throws Exception {
        try (Stage2Admission gate = new Stage2Admission(1)) {
            var first = gate.reader(0);
            var second = gate.reader(1);
            var ready = CompletableFuture.<Void>completedFuture(null);
            first.emitted(1);
            second.emitted(2);
            assertThat(first.hasRoom()).isFalse();
            assertThat(second.hasRoom()).isFalse();
            var noSplit = new CompletableFuture<Void>();
            var waiting = first.available(noSplit);
            var otherWaiting = second.available(ready);
            gate.acknowledge(1);
            assertThat(waiting).isNotDone();
            assertThat(otherWaiting).isNotDone();
            noSplit.complete(null);
            assertThat(waiting).isCompleted();
            assertThat(otherWaiting).isNotDone();
            assertThat(first.hasRoom()).isTrue();
            first.emitted(3);
            gate.acknowledge(2);
            gate.acknowledge(3);
            var sample = new ObjectMapper().readTree(gate.sample());
            assertThat(sample.path("peakPendingInputs").asInt()).isEqualTo(2);
            assertThat(sample.path("peakPendingPerSubtask").asInt()).isEqualTo(1);
            assertThat(sample.path("pendingInputs").asInt()).isZero();
        }
    }

    @Test
    void changedSplitFutureAndRepeatedAvailabilityCallsDoNotLoseTheWakeup() {
        try (Stage2Admission gate = new Stage2Admission(1)) {
            var reader = gate.reader(0);
            reader.emitted(1);
            var oldSplit = new CompletableFuture<Void>();
            var newSplit = new CompletableFuture<Void>();
            var waiting = reader.available(oldSplit);
            assertThat(reader.available(oldSplit)).isSameAs(waiting);
            assertThat(reader.available(newSplit)).isSameAs(waiting);
            gate.acknowledge(1);
            oldSplit.complete(null);
            assertThat(waiting).isNotDone();
            newSplit.complete(null);
            assertThat(waiting).isCompleted();
        }
    }

    @Test
    void windowEndAndReaderCloseWakeAReaderWithoutAnAcknowledgement() {
        try (Stage2Admission gate = new Stage2Admission(1)) {
            var reader = gate.reader(0);
            reader.emitted(1);
            var waiting = reader.available(new CompletableFuture<>());
            gate.endWindow();
            assertThat(waiting).isCompleted();
            // End of input does not discard an outstanding acknowledgement's credit.
            gate.acknowledge(1);
        }
        try (Stage2Admission gate = new Stage2Admission(1)) {
            var reader = gate.reader(0);
            var waiting = reader.available(new CompletableFuture<>());
            reader.stop();
            assertThat(waiting).isCompleted();
            assertThatThrownBy(() -> reader.emitted(1)).hasMessageContaining("Invalid diagnostic");
        }
    }

    @Test
    void invalidDiagnosticConfigurationDoesNotRetainAHarnessOrCreateItsInventory()
            throws Exception {
        var table =
                TableDestination.of(
                        "local-project",
                        "local-instance",
                        "invalid-" + java.util.UUID.randomUUID());
        Path inventory = directory.resolve("invalid-inventory");
        try {
            assertThatThrownBy(
                            () ->
                                    new Stage2Harness(
                                            table,
                                            "127.0.0.1:1",
                                            inventory,
                                            1,
                                            1024,
                                            false,
                                            false,
                                            1,
                                            true,
                                            null,
                                            2))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(LocalStagedHarness.RUNS.values())
                    .noneMatch(candidate -> candidate.table.equals(table));
            assertThat(inventory).doesNotExist();
        } finally {
            for (LocalStagedHarness candidate : List.copyOf(LocalStagedHarness.RUNS.values())) {
                if (candidate.table.equals(table)) {
                    candidate.close();
                }
            }
        }
    }

    @Test
    void closeWaitsForTheReceiverAcknowledgementBeforeDiscardingCredits() throws Exception {
        var closer = Executors.newSingleThreadExecutor();
        try (Stage2Harness run =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        directory.resolve("closing-inventory"),
                        1,
                        1024,
                        false,
                        false,
                        1,
                        true,
                        null,
                        1)) {
            run.admission.reader(0).emitted(0);
            run.admitted(0);
            java.util.concurrent.Future<?> acknowledged;
            java.util.concurrent.Future<?> closed;
            synchronized (run) {
                var started = new java.util.concurrent.CountDownLatch(1);
                acknowledged =
                        run.receiver.submit(
                                () -> {
                                    started.countDown();
                                    run.acknowledged(0, System.nanoTime());
                                });
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                closed =
                        closer.submit(
                                () -> {
                                    run.close();
                                    return null;
                                });
                io.github.flink.gcp.connector.testutils.Awaits.await(
                        "receiver shutdown while acknowledgement waits for the harness monitor",
                        java.time.Duration.ofSeconds(5),
                        run.receiver::isShutdown);
            }
            acknowledged.get(5, TimeUnit.SECONDS);
            closed.get(5, TimeUnit.SECONDS);
            assertThat(run.ledger.acknowledgedCount()).isEqualTo(1);
        } finally {
            closer.shutdownNow();
            assertThat(closer.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentAcknowledgementCannotStrandAvailability() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (Stage2Admission gate = new Stage2Admission(1)) {
            var reader = gate.reader(0);
            var ready = CompletableFuture.<Void>completedFuture(null);
            for (long sequence = 0; sequence < 100; sequence++) {
                reader.emitted(sequence);
                long current = sequence;
                var ack = executor.submit(() -> gate.acknowledge(current));
                reader.available(ready).get(2, TimeUnit.SECONDS);
                ack.get(2, TimeUnit.SECONDS);
                assertThat(reader.hasRoom()).isTrue();
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void duplicateAcknowledgementDoesNotReleaseAnotherInputAndDeadlineIsScheduled()
            throws Exception {
        try (Stage2Harness run =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        directory.resolve("inventory"),
                        4,
                        1024,
                        false,
                        false,
                        1,
                        true,
                        null,
                        1)) {
            var reader = run.admission.reader(0);
            run.startWindow(System.nanoTime(), 0, TimeUnit.MILLISECONDS.toNanos(100));
            reader.emitted(0);
            run.admitted(0);
            run.acknowledged(0, System.nanoTime());
            reader.emitted(1);
            run.admitted(1);
            run.acknowledged(0, System.nanoTime());
            assertThat(reader.hasRoom()).isFalse();
            var waiting = reader.available(new CompletableFuture<>());
            waiting.get(5, TimeUnit.SECONDS);
            assertThat(run.windowEnded(System.nanoTime())).isTrue();
            assertThat(reader.hasRoom()).isFalse();
            run.acknowledged(1, System.nanoTime());
            assertThat(reader.hasRoom()).isTrue();
        }
    }

    @Test
    void phaseProgressCountsDistinctInputsAndReportsEmptyMeasurementExplicitly() throws Exception {
        try (Stage2Ledger ledger = new Stage2Ledger(directory.resolve("ledger"), 4, 256)) {
            ledger.window(100, 200);
            ledger.admit(0, 90);
            var empty = new ObjectMapper().readTree(ledger.sample());
            assertThat(empty.path("firstMeasuredAdmissionOffsetNanos").asLong()).isEqualTo(-1);
            ledger.admit(1, 100);
            ledger.admit(1, 120);
            ledger.admit(2, 199);
            ledger.admit(3, 200);
            ledger.acknowledge(0, 201);
            ledger.acknowledge(0, 202);
            var sample = new ObjectMapper().readTree(ledger.sample());
            assertThat(sample.path("pendingInputs").asInt()).isEqualTo(3);
            assertThat(sample.path("warmupInputs").asInt()).isEqualTo(1);
            assertThat(sample.path("measuredInputs").asInt()).isEqualTo(2);
            assertThat(sample.path("tailInputs").asInt()).isEqualTo(1);
            assertThat(sample.path("firstMeasuredAdmissionOffsetNanos").asLong()).isZero();
            assertThat(sample.path("lastMeasuredAdmissionOffsetNanos").asLong()).isEqualTo(99);
        }
    }
}
