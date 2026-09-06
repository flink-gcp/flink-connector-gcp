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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.flink.gcp.connector.bigtable.table.function.BigtableAsyncSqlITCase.collect;
import static io.github.flink.gcp.connector.bigtable.table.function.BigtableAsyncSqlPlanTest.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Measures the SQL operator's capacity and the real SDK deadline over controlled local RPCs. */
@Timeout(60)
class BigtableAsyncSqlControlITCase {
    @Test
    void sqlCapacityBoundsOutstandingCallsAndReleasedSlotsAdmitTheRemainingInputs()
            throws Exception {
        ControlledService service = new ControlledService();
        Server server = ServerBuilder.forPort(0).addService(service).build().start();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<List<Row>> query =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    TableEnvironment env = environment(server);
                                    env.getConfig()
                                            .set(
                                                    "table.exec.async-scalar.max-concurrent-operations",
                                                    "2");
                                    return collect(
                                            env,
                                            "SELECT BT_CHECK_AND_MUTATE('test', k) FROM "
                                                    + "(VALUES ('0'), ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7')) AS v(k)");
                                } catch (Exception failure) {
                                    throw new CompletionException(failure);
                                }
                            },
                            executor);
            for (int batch = 0; batch < 4; batch++) {
                StreamObserver<CheckAndMutateRowResponse> first = service.next(query);
                StreamObserver<CheckAndMutateRowResponse> second = service.next(query);
                // RPC arrival order need not match the ordered SQL operator's input order.
                service.answer(first);
                service.answer(second);
            }
            assertThat(query.get(30, TimeUnit.SECONDS))
                    .hasSize(8)
                    .allMatch(row -> Boolean.TRUE.equals(row.getField(0)));
            assertThat(service.calls).hasValue(8);
            assertThat(service.peak).hasValue(2);
            assertThat(service.active).hasValue(0);
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void sdkDeadlineCancelsTheRpcAndFailsSqlWithoutAnotherAttempt() throws Exception {
        ControlledService service = new ControlledService();
        Server server = ServerBuilder.forPort(0).addService(service).build().start();
        try {
            TableEnvironment env = environment(server);
            set(env, "request-timeout", "1 s");
            env.getConfig().set("table.exec.async-scalar.timeout", "5 s");
            assertThatThrownBy(() -> collect(env, "SELECT BT_CHECK_AND_MUTATE('test', 'key')"))
                    .hasStackTraceContaining("DEADLINE_EXCEEDED");
            assertThat(service.cancelled.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(service.calls).hasValue(1);
        } finally {
            server.shutdownNow();
            assertThat(server.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static TableEnvironment environment(Server server) {
        TableEnvironment env = BigtableAsyncSqlPlanTest.environment();
        env.getConfig().set("parallelism.default", "1");
        set(env, "emulator-endpoint", "localhost:" + server.getPort());
        set(env, "predicate.type", "row-exists");
        set(env, "then.0.operation", "delete-row");
        return env;
    }

    private static final class ControlledService extends BigtableGrpc.BigtableImplBase {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final CountDownLatch cancelled = new CountDownLatch(1);
        final BlockingQueue<StreamObserver<CheckAndMutateRowResponse>> pending =
                new LinkedBlockingQueue<>();

        @Override
        public void checkAndMutateRow(
                CheckAndMutateRowRequest request,
                StreamObserver<CheckAndMutateRowResponse> response) {
            ((ServerCallStreamObserver<CheckAndMutateRowResponse>) response)
                    .setOnCancelHandler(cancelled::countDown);
            calls.incrementAndGet();
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
            pending.add(response);
        }

        StreamObserver<CheckAndMutateRowResponse> next(CompletableFuture<?> query)
                throws Exception {
            StreamObserver<CheckAndMutateRowResponse> response = pending.poll(30, TimeUnit.SECONDS);
            if (response == null && query.isDone()) {
                query.get();
            }
            assertThat(response).as("next SQL RPC").isNotNull();
            return response;
        }

        void answer(StreamObserver<CheckAndMutateRowResponse> response) {
            active.decrementAndGet();
            response.onNext(
                    CheckAndMutateRowResponse.newBuilder().setPredicateMatched(true).build());
            response.onCompleted();
        }
    }
}
