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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class LocalStagedHarnessTest {
    @Test
    void refusesMissingAndNonLoopbackEndpointsBeforeCreatingClients() {
        for (String endpoint :
                new String[] {
                    null, "", "bigtable.googleapis.com:443", "localhost:9000", "127.0.0.1.evil:9000"
                }) {
            assertThatThrownBy(
                            () ->
                                    new LocalStagedHarness(
                                            TableDestination.of("p", "i", "t"),
                                            endpoint,
                                            1024,
                                            false,
                                            false,
                                            1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("127.0.0.1");
        }
    }

    @Test
    void rejectsUnsafeCheckpointConfigurationsAtGraphConstruction() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 1)) {
            for (int mode = 0; mode < 5; mode++) {
                Configuration config = new Configuration();
                config.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, mode != 4);
                StreamExecutionEnvironment env =
                        StreamExecutionEnvironment.getExecutionEnvironment(config);
                env.setRuntimeMode(
                        mode == 0
                                ? RuntimeExecutionMode.BATCH
                                : mode == 1
                                        ? RuntimeExecutionMode.AUTOMATIC
                                        : RuntimeExecutionMode.STREAMING);
                if (mode != 2) {
                    env.enableCheckpointing(100);
                }
                if (mode == 3) {
                    env.getCheckpointConfig()
                            .setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
                }
                env.fromData(1L).sinkTo(new LocalStagedHarness.StagedSink(run, false));
                assertThatThrownBy(env::getStreamGraph).hasMessageContaining("requires STREAMING");
            }
            assertThat(run.attempts).isEmpty();
        }
    }

    @Test
    void negativeControlDetectsDuplicateSumContributions() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, true, true, 1)) {
            CheckAndMutateRowRequest request = envelopes(run, 1).get(0).wire;
            run.store.apply(request);
            run.store.apply(request);
            assertThat(BigtableLocalStagedJobITCase.totalSum(run)).isEqualTo(1);
            // Preserve the mutation while making the marker predicate miss the existing marker.
            RowFilter.Chain chain =
                    request.getPredicateFilter().getChain().toBuilder()
                            .setFilters(
                                    1,
                                    RowFilter.newBuilder()
                                            .setColumnQualifierRegexFilter(
                                                    ByteString.copyFromUtf8("never-present")))
                            .build();
            run.store.apply(
                    request.toBuilder()
                            .setPredicateFilter(RowFilter.newBuilder().setChain(chain))
                            .build());
            assertThat(BigtableLocalStagedJobITCase.totalSum(run)).isEqualTo(2);
        }
    }

    @Test
    void interruptionCancelsEveryOriginalFutureAndReleasesReceiver() throws Exception {
        LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 2);
        var executor = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (run;
                var committer = new LocalStagedHarness.LocalCommitter(run, false)) {
            run.hang = true;
            var task =
                    executor.submit(
                            () -> {
                                try {
                                    committer.commit(new ArrayList<>(envelopes(run, 3)));
                                } catch (Throwable caught) {
                                    failure.set(caught);
                                }
                            });
            await("bounded pending requests", Duration.ofSeconds(5), () -> run.active.get() == 2);
            assertThat(run.originalFutures).hasSize(2);
            task.cancel(true);
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isInstanceOf(InterruptedException.class);
            assertThat(run.originalFutures)
                    .allSatisfy(future -> assertThat(future.isCancelled()).isTrue());
            assertThat(run.active.get()).isZero();
        } finally {
            executor.shutdownNow();
        }
        assertThat(run.receiver.isTerminated()).isTrue();
        assertThat(LocalStagedHarness.RUNS).doesNotContainKey(run.id);
    }

    @Test
    void restoredDestinationOrProfileMismatchFailsBeforeSubmitting() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 1);
                var committer = new LocalStagedHarness.LocalCommitter(run, false)) {
            Request valid = envelopes(run, 1).get(0);
            for (CheckAndMutateRowRequest wrong :
                    List.of(
                            valid.wire.toBuilder()
                                    .setTableName("projects/p/instances/i/tables/other")
                                    .build(),
                            valid.wire.toBuilder().setAppProfileId("other-profile").build())) {
                assertThatThrownBy(() -> committer.commit(List.of(new Request(wrong))))
                        .hasMessageContaining("destination/profile");
                assertThat(run.originalFutures).isEmpty();
            }
        }
    }

    @Test
    void probeRefusesOversizedRetainedPayloadBeforeStartingResources() {
        for (String arm : List.of("staged", "bulk")) {
            assertThatThrownBy(
                            () ->
                                    BigtableLocalStagedProbe.main(
                                            new String[] {
                                                arm, "513", "65536", "1", "4", "1000", "0", "even"
                                            }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("retained payload limit")
                    .hasMessageContaining("32 MiB");
        }
    }

    static List<Request> envelopes(LocalStagedHarness run, int count) throws Exception {
        StagedMutationTestSink.Writer writer = new StagedMutationTestSink.Writer(100, 64L << 20);
        for (long i = 0; i < count; i++) {
            run.admitted(i);
            writer.write(run.input(i), null);
        }
        List<Request> requests = new ArrayList<>();
        writer.prepareCommit().forEach(wire -> requests.add(new Request(wire)));
        writer.close();
        return requests;
    }

    static final class Request implements Committer.CommitRequest<CheckAndMutateRowRequest> {
        final CheckAndMutateRowRequest wire;
        boolean matched;

        Request(CheckAndMutateRowRequest wire) {
            this.wire = wire;
        }

        @Override
        public CheckAndMutateRowRequest getCommittable() {
            return wire;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable failure) {
            throw new AssertionError(failure);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable failure) {
            throw new AssertionError(failure);
        }

        @Override
        public void retryLater() {
            throw new AssertionError("Unexpected retry");
        }

        @Override
        public void updateAndRetryLater(CheckAndMutateRowRequest request) {
            throw new AssertionError("Unexpected update");
        }

        @Override
        public void signalAlreadyCommitted() {
            matched = true;
        }
    }
}
