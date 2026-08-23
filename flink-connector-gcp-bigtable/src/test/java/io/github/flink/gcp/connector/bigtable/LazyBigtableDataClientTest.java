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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the shared data client's lazy construction and close interlock. */
@Timeout(30)
class LazyBigtableDataClientTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final long WAIT_SECONDS = 10;
    private static final long CONSTRUCTION_WAIT_SECONDS = 20;

    @Test
    void returnsTheOneClientItBuildsOnRepeatedCalls() throws Exception {
        LazyBigtableDataClient holder = holder();
        BigtableDataClient first = holder.get(TABLE);

        try {
            assertThat(first).isNotNull();
            BigtableDataClient second =
                    holder.get(
                            TABLE,
                            settings -> {
                                throw new AssertionError("the cached client was rebuilt");
                            });

            assertThat(second).isSameAs(first);
        } finally {
            holder.close();
        }
    }

    @Test
    void concurrentFirstCallsBuildOneClient() throws Exception {
        LazyBigtableDataClient holder = holder();
        BigtableDataClient client = BigtableDataClient.create(holder.settings(TABLE));
        CountDownLatch constructionStarted = new CountDownLatch(1);
        CountDownLatch releaseConstruction = new CountDownLatch(1);
        CountDownLatch secondGetStarted = new CountDownLatch(1);
        AtomicReference<Thread> secondGetThread = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<BigtableDataClient> first =
                    executor.submit(
                            () ->
                                    holder.get(
                                            TABLE,
                                            settings -> {
                                                constructionStarted.countDown();
                                                await(
                                                        releaseConstruction,
                                                        CONSTRUCTION_WAIT_SECONDS);
                                                return client;
                                            }));
            await(constructionStarted);

            Future<BigtableDataClient> second =
                    executor.submit(
                            () -> {
                                secondGetThread.set(Thread.currentThread());
                                secondGetStarted.countDown();
                                return holder.get(
                                        TABLE,
                                        settings -> {
                                            throw new AssertionError(
                                                    "a concurrent first call rebuilt the client");
                                        });
                            });
            await(secondGetStarted);
            awaitBlocked(
                    secondGetThread.get(),
                    second,
                    "a concurrent first call must wait for client construction");

            releaseConstruction.countDown();
            assertThat(first.get(WAIT_SECONDS, TimeUnit.SECONDS)).isSameAs(client);
            assertThat(second.get(WAIT_SECONDS, TimeUnit.SECONDS)).isSameAs(client);
        } finally {
            releaseConstruction.countDown();
            holder.close();
            client.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void closeWaitsForConstructionAndReleasesTheClientItReceives() throws Exception {
        LazyBigtableDataClient holder = holder();
        BigtableDataClient client = BigtableDataClient.create(holder.settings(TABLE));
        CountDownLatch constructionStarted = new CountDownLatch(1);
        CountDownLatch releaseConstruction = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Thread> closeThread = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<BigtableDataClient> getting =
                    executor.submit(
                            () ->
                                    holder.get(
                                            TABLE,
                                            settings -> {
                                                constructionStarted.countDown();
                                                await(
                                                        releaseConstruction,
                                                        CONSTRUCTION_WAIT_SECONDS);
                                                return client;
                                            }));
            await(constructionStarted);

            Future<?> closing =
                    executor.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                closeStarted.countDown();
                                holder.close();
                                return null;
                            });
            await(closeStarted);
            awaitBlocked(closeThread.get(), closing, "close must not overtake client construction");

            releaseConstruction.countDown();
            assertThat(getting.get(WAIT_SECONDS, TimeUnit.SECONDS)).isSameAs(client);
            closing.get(WAIT_SECONDS, TimeUnit.SECONDS);

            // BigtableDataClient exposes no shutdown state. As in
            // DefaultMutationBatcherFactoryTest, a closed client's background executor rejects
            // the delay-threshold task a new batcher schedules.
            assertThatThrownBy(() -> client.newBulkMutationBatcher(TableId.of("orders")))
                    .isInstanceOf(RejectedExecutionException.class);
            assertThatThrownBy(
                            () ->
                                    holder.get(
                                            TABLE,
                                            settings -> {
                                                throw new AssertionError(
                                                        "a closed holder rebuilt its client");
                                            }))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Bigtable test holder")
                    .hasMessageContaining("p.i.orders")
                    .hasMessageContaining("was closed before it was used");
        } finally {
            releaseConstruction.countDown();
            holder.close();
            client.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void closeClearsCredentialsAndRefusesAWaitingInjection() throws Exception {
        LazyBigtableDataClient holder = new LazyBigtableDataClient("test holder", null, null);
        NoCredentialsProvider credentials = NoCredentialsProvider.create();
        holder.useCredentials(credentials);
        assertThat(holder.settings(TABLE).getStubSettings().getCredentialsProvider())
                .isSameAs(credentials);
        CountDownLatch injectionStarted = new CountDownLatch(1);
        AtomicReference<Thread> injectionThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> injection;
            synchronized (holder) {
                injection =
                        executor.submit(
                                () -> {
                                    injectionThread.set(Thread.currentThread());
                                    injectionStarted.countDown();
                                    holder.useCredentials(credentials);
                                });
                await(injectionStarted);
                awaitBlocked(
                        injectionThread.get(),
                        injection,
                        "credential injection must wait for the close monitor");
                holder.close();
            }

            assertThatThrownBy(() -> injection.get(WAIT_SECONDS, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage(
                            "The Bigtable test holder was closed before credentials were supplied.");
            assertThat(holder.settings(TABLE).getStubSettings().getCredentialsProvider())
                    .isNotSameAs(credentials);
        } finally {
            holder.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static LazyBigtableDataClient holder() {
        return new LazyBigtableDataClient(
                "test holder", null, EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));
    }

    private static void await(CountDownLatch latch) {
        await(latch, WAIT_SECONDS);
    }

    private static void await(CountDownLatch latch, long seconds) {
        try {
            assertThat(latch.await(seconds, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while coordinating the lifecycle test", e);
        }
    }

    private static void awaitBlocked(Thread thread, Future<?> operation, String description) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (thread.getState() != Thread.State.BLOCKED
                && !operation.isDone()
                && System.nanoTime() - deadline < 0) {
            Thread.onSpinWait();
        }
        assertThat(operation).as(description).isNotDone();
        assertThat(thread.getState()).as(description).isEqualTo(Thread.State.BLOCKED);
    }
}
