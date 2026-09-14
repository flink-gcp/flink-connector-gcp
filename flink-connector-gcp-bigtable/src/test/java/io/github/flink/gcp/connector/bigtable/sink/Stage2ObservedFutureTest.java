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

import com.google.api.core.SettableApiFuture;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2ObservedFutureTest {
    @Test
    void rpcCompletionDoesNotPublishSuccessBeforeMeasurementFinishes() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 1)) {
            SettableApiFuture<Boolean> original = SettableApiFuture.create();
            CountDownLatch measuring = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Stage2ObservedFuture<Boolean> observed =
                    new Stage2ObservedFuture<>(
                            original,
                            run,
                            (value, now) -> {
                                measuring.countDown();
                                if (!release.await(10, TimeUnit.SECONDS)) {
                                    throw new IOException("Measurement was not released");
                                }
                            });
            var executor = Executors.newSingleThreadExecutor();
            try {
                var completion = executor.submit(() -> original.set(false));
                assertThat(measuring.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(original.isDone()).isTrue();
                assertThat(observed.isDone()).isFalse();
                assertThat(run.active.get()).isEqualTo(1);
                release.countDown();
                assertThat(observed.get(10, TimeUnit.SECONDS)).isFalse();
                completion.get(10, TimeUnit.SECONDS);
                assertThat(run.active.get()).isZero();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void cancellationReachesTheOriginalRpcWithoutRecordingAnAcknowledgement() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 1)) {
            SettableApiFuture<Boolean> original = SettableApiFuture.create();
            AtomicBoolean acknowledged = new AtomicBoolean();
            var observed =
                    new Stage2ObservedFuture<>(
                            original, run, (value, now) -> acknowledged.set(true));
            assertThat(observed.cancel(true)).isTrue();
            assertThat(original.isCancelled()).isTrue();
            assertThat(observed.isCancelled()).isTrue();
            assertThatThrownBy(observed::get).isInstanceOf(CancellationException.class);
            assertThat(acknowledged).isFalse();
            assertThat(run.active.get()).isZero();
        }
    }

    @Test
    void measurementFailureAndRpcFailureBothFailTheObservedRequest() throws Exception {
        for (boolean measurement : new boolean[] {true, false}) {
            try (LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 1)) {
                SettableApiFuture<Boolean> original = SettableApiFuture.create();
                IOException failure = new IOException("Injected observation failure");
                var observed =
                        new Stage2ObservedFuture<>(
                                original,
                                run,
                                (value, now) -> {
                                    throw failure;
                                });
                if (measurement) {
                    original.set(false);
                } else {
                    original.setException(failure);
                }
                assertThatThrownBy(observed::get).hasCause(failure);
                assertThat(run.active.get()).isZero();
            }
        }
    }
}
