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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestWriter;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class ConditionalRuntimeTest {
    private static final TableDestination TABLE = TableDestination.of("p", "i", "actual");
    private final ConditionalTestClients client = new ConditionalTestClients();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    @Test
    void sinkCountsBothBranchesAfterCompletionAndSkipsBeforeOpeningClients() throws Exception {
        try (SingleRowRequestWriter<String> writer = writer(EmptyBranchPolicy.IGNORE)) {
            writer.write("skip", TestContexts.NO_OP);
            assertThat(client.opened).isEmpty();
            assertThat(metrics.counterValue("recordsSkipped")).isEqualTo(1);
            writer.write("one", TestContexts.NO_OP);
            writer.write("two", TestContexts.NO_OP);
            client.answers.get(0).set(true);
            client.answers.get(1).set(false);
            writer.flush(false);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(2);
            assertThat(metrics.counterValue("predicatesMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("predicatesNotMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("emptyBranchesSelected")).isEqualTo(1);
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
        assertThat(client.closes).isEqualTo(1);
    }

    @Test
    void successfulEmptyBranchFailsTheJobEvenWithADroppingHandler() throws Exception {
        try (SingleRowRequestWriter<String> writer = writer(EmptyBranchPolicy.FAIL)) {
            writer.write("one", TestContexts.NO_OP);
            client.answers.get(0).set(true);
            mailbox.drain();
            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("EmptyBranchPolicy.FAIL");
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(1);
            assertThat(metrics.counterValue("predicatesMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("emptyBranchesSelected")).isEqualTo(1);
            assertThat(metrics.counterValue("requestsFailed")).isZero();
            assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
    }

    @Test
    void checkpointDrainWaitsForBothConditionalResponses() throws Exception {
        try (SingleRowRequestWriter<String> writer = writer(EmptyBranchPolicy.IGNORE)) {
            writer.write("one", TestContexts.NO_OP);
            writer.write("two", TestContexts.NO_OP);
            mailbox.execute(() -> client.answers.get(0).set(false), "answer first");
            mailbox.execute(() -> client.answers.get(1).set(true), "answer second");
            writer.flush(false);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(2);
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
    }

    @Test
    void closeCancelsOutstandingRequestsAndLateCallbacksCannotCountSuccess() throws Exception {
        SingleRowRequestWriter<String> writer = writer(EmptyBranchPolicy.IGNORE);
        writer.write("one", TestContexts.NO_OP);
        writer.close();
        assertThat(client.answers.get(0).isCancelled()).isTrue();
        client.answers.get(0).set(false);
        mailbox.drain();
        assertThat(metrics.counterValue("requestsCompleted")).isZero();
        assertThat(metrics.counterValue("predicatesNotMatched")).isZero();
    }

    @Test
    void asyncCorrelatesWithoutRepeatingUserCallbacksAndCountsOnClientThreads() throws Exception {
        AtomicInteger resolves = new AtomicInteger();
        AtomicInteger serializes = new AtomicInteger();
        ConditionalConfig<String> config =
                new ConditionalConfig<>(
                        (value, context) -> {
                            assertThat(context).isNull();
                            return TableDestination.of(
                                    "p", "i", "table-" + resolves.incrementAndGet());
                        },
                        (value, context) -> {
                            assertThat(context).isNull();
                            serializes.incrementAndGet();
                            return request(value);
                        },
                        null,
                        BigtableRequestOptions.builder().build(),
                        EmptyBranchPolicy.IGNORE,
                        null,
                        null);
        ConditionalFunction<String> function = open(config);
        try (AutoCloseable closing = function::close) {
            Answer<Tuple2<String, ConditionalResult>> first = new Answer<>();
            Answer<Tuple2<String, ConditionalResult>> second = new Answer<>();
            function.asyncInvoke("one", first.resultFuture);
            function.asyncInvoke("two", second.resultFuture);
            Thread a = new Thread(() -> client.answers.get(0).set(false));
            Thread b = new Thread(() -> client.answers.get(1).set(true));
            a.start();
            b.start();
            a.join();
            b.join();
            Tuple2<String, ConditionalResult> output = first.join().iterator().next();
            assertThat(output.f0).isEqualTo("one");
            assertThat(output.f1.getDestination().getTable()).isEqualTo("table-1");
            assertThat(output.f1.getRowKey().toStringUtf8()).isEqualTo("one");
            assertThat(output.f1.isPredicateMatched()).isFalse();
            assertThat(output.f1.isSelectedBranchHasMutations()).isTrue();
            assertThat(second.join().iterator().next().f1.isSelectedBranchHasMutations()).isFalse();
            assertThat(resolves).hasValue(2);
            assertThat(serializes).hasValue(2);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(2);
            assertThat(metrics.counterValue("predicatesMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("predicatesNotMatched")).isEqualTo(1);
        }
    }

    @Test
    void asyncSkipPolicyFailureAndTimeoutHaveDistinctOutcomes() throws Exception {
        ConditionalFunction<String> function = open(config(EmptyBranchPolicy.FAIL));
        try (AutoCloseable closing = function::close) {
            Answer<Tuple2<String, ConditionalResult>> skipped = new Answer<>();
            function.asyncInvoke("skip", skipped.resultFuture);
            assertThat(skipped.join()).isEmpty();
            assertThat(client.opened).isEmpty();
            Answer<Tuple2<String, ConditionalResult>> empty = new Answer<>();
            function.asyncInvoke("one", empty.resultFuture);
            client.answers.get(0).set(true);
            assertThatThrownBy(empty::join)
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasStackTraceContaining("EmptyBranchPolicy.FAIL");
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(1);
            Answer<Tuple2<String, ConditionalResult>> timedOut = new Answer<>();
            function.asyncInvoke("two", timedOut.resultFuture);
            function.timeout("two", timedOut.resultFuture);
            assertThat(client.answers.get(1).isCancelled()).isTrue();
            assertThatThrownBy(timedOut::join)
                    .hasStackTraceContaining("may or may not have applied");
            assertThat(metrics.counterValue("requestsTimedOut")).isEqualTo(1);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(1);
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
    }

    private SingleRowRequestWriter<String> writer(EmptyBranchPolicy policy) {
        ConditionalConfig<String> config = config(policy);
        return new SingleRowRequestWriter<>(
                new SingleRowRequestConfig<>(
                        config.destinationResolver,
                        config.sinkSerializer(),
                        null,
                        config.requestOptions,
                        FailureHandler.logAndDrop(),
                        null,
                        null),
                client,
                mailbox,
                metrics);
    }

    private ConditionalFunction<String> open(ConditionalConfig<String> config) throws Exception {
        ConditionalFunction<String> function = new ConditionalFunction<>(config, client);
        RuntimeContext context =
                (RuntimeContext)
                        Proxy.newProxyInstance(
                                RuntimeContext.class.getClassLoader(),
                                new Class<?>[] {RuntimeContext.class},
                                (proxy, method, arguments) -> {
                                    switch (method.getName()) {
                                        case "getMetricGroup":
                                            return metrics;
                                        case "getUserCodeClassLoader":
                                            return getClass().getClassLoader();
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
        function.setRuntimeContext(context);
        function.open(DefaultOpenContext.INSTANCE);
        return function;
    }

    private static ConditionalConfig<String> config(EmptyBranchPolicy policy) {
        return new ConditionalConfig<>(
                (input, context) -> TABLE,
                (input, context) -> request(input),
                null,
                BigtableRequestOptions.builder().build(),
                policy,
                null,
                null);
    }

    private static ConditionalRequest request(String key) {
        return key.equals("skip")
                ? null
                : ConditionalRequest.of(
                        ByteString.copyFromUtf8(key),
                        ConditionalFilter.rowExists(),
                        List.of(),
                        List.of(ConditionalMutation.deleteRow()));
    }

    private static final class Answer<T> {
        private final CompletableFuture<Collection<T>> future = new CompletableFuture<>();
        private final ResultFuture<T> resultFuture = proxy();

        @SuppressWarnings("unchecked")
        private ResultFuture<T> proxy() {
            return (ResultFuture<T>)
                    Proxy.newProxyInstance(
                            ResultFuture.class.getClassLoader(),
                            new Class<?>[] {ResultFuture.class},
                            (proxy, method, arguments) -> {
                                switch (method.getName()) {
                                    case "complete":
                                        future.complete((Collection<T>) arguments[0]);
                                        return null;
                                    case "completeExceptionally":
                                        future.completeExceptionally((Throwable) arguments[0]);
                                        return null;
                                    default:
                                        throw new UnsupportedOperationException(method.getName());
                                }
                            });
        }

        Collection<T> join() {
            assertThat(future).as("the callback must complete the result").isDone();
            return future.join();
        }
    }
}
