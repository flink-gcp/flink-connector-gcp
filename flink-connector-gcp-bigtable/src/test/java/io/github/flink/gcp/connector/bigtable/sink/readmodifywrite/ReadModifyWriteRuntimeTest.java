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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestWriter;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class ReadModifyWriteRuntimeTest {
    private static final TableDestination TABLE = TableDestination.of("p", "i", "actual");
    private final FakeReadModifyWriteClients client = new FakeReadModifyWriteClients();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    @Test
    void sinkSkipsBeforeOpeningClientsAndCheckpointDrainsEveryAcceptedRequest() throws Exception {
        try (SingleRowRequestWriter<String> writer = writer()) {
            writer.write("skip", TestContexts.NO_OP);
            assertThat(client.opened).isEmpty();
            assertThat(metrics.counterValue("recordsSkipped")).isEqualTo(1);
            writer.write("one", TestContexts.NO_OP);
            writer.write("two", TestContexts.NO_OP);
            assertThat(metrics.counterValue("requestsAccepted")).isEqualTo(2);
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isEqualTo(2);
            mailbox.execute(() -> client.answers.get(1).set(row("two")), "answer second");
            mailbox.execute(
                    () -> {
                        assertThat(metrics.<Integer>gaugeValue("inFlightRequests"))
                                .isGreaterThan(0);
                        client.answers.get(0).set(row("one"));
                    },
                    "answer first");
            writer.flush(false);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(2);
            assertThat(metrics.counterValue("predicatesMatched")).isZero();
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
        assertThat(client.closes).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(
            value = Status.Code.class,
            names = {"DEADLINE_EXCEEDED", "UNAVAILABLE", "ABORTED", "CANCELLED"})
    void ambiguousResponsesFailEvenWithADroppingHandlerAndNeverRetry(Status.Code code)
            throws Exception {
        try (SingleRowRequestWriter<String> writer = writer()) {
            writer.write("one", TestContexts.NO_OP);
            client.answers.get(0).setException(Status.fromCode(code).asRuntimeException());
            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("ReadModifyWriteRow")
                    .hasMessageContaining("may or may not")
                    .hasMessageContaining("applies it again");
            assertThat(client.sent).hasSize(1);
            assertThat(metrics.counterValue("requestsFailed")).isEqualTo(1);
            assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
            assertThat(metrics.counterValue("requestsTimedOut"))
                    .isEqualTo(code == Status.Code.DEADLINE_EXCEEDED ? 1 : 0);
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
    }

    @Test
    void serviceRejectionIsRoutedAndMissingResourcesRemainFatal() throws Exception {
        try (SingleRowRequestWriter<String> writer = writer()) {
            writer.write("bad", TestContexts.NO_OP);
            client.answers.get(0).setException(Status.INVALID_ARGUMENT.asRuntimeException());
            writer.flush(false);
            assertThat(metrics.counterValue("numRecordsSendErrors")).isEqualTo(1);
            writer.write("missing", TestContexts.NO_OP);
            client.answers.get(1).setException(Status.NOT_FOUND.asRuntimeException());
            assertThatThrownBy(() -> writer.flush(false)).hasMessageContaining("does not exist");
            assertThat(client.sent).hasSize(2);
        }
    }

    @Test
    void closeCancelsOutstandingRequestsAndLateCallbacksCannotCountSuccess() throws Exception {
        SingleRowRequestWriter<String> writer = writer();
        writer.write("one", TestContexts.NO_OP);
        writer.close();
        assertThat(client.answers.get(0).isCancelled()).isTrue();
        client.answers.get(0).set(row("one"));
        mailbox.drain();
        assertThat(metrics.counterValue("requestsCompleted")).isZero();
        assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
    }

    @Test
    void asyncCorrelatesReusedInputWithoutRepeatingCallbacksAndConvertsReturnedCells()
            throws Exception {
        AtomicInteger resolves = new AtomicInteger();
        AtomicInteger serializes = new AtomicInteger();
        ReadModifyWriteConfig<String> config =
                new ReadModifyWriteConfig<>(
                        (input, context) -> {
                            assertThat(context).isNull();
                            return TableDestination.of(
                                    "p", "i", "table-" + resolves.incrementAndGet());
                        },
                        (input, context) -> {
                            assertThat(context).isNull();
                            serializes.incrementAndGet();
                            return request(input);
                        },
                        null,
                        BigtableRequestOptions.builder().build(),
                        null,
                        null);
        ReadModifyWriteFunction<String> function = open(config);
        try (AutoCloseable closing = function::close) {
            Answer<Tuple2<String, ReadModifyWriteResult>> first = new Answer<>();
            Answer<Tuple2<String, ReadModifyWriteResult>> second = new Answer<>();
            String reused = new String("same");
            function.asyncInvoke(reused, first.resultFuture);
            function.asyncInvoke(reused, second.resultFuture);
            Thread answer = new Thread(() -> client.answers.get(1).set(row("same")));
            answer.start();
            answer.join();
            function.timeout(reused, first.resultFuture);
            assertThat(client.answers.get(0).isCancelled()).isTrue();
            assertThatThrownBy(first::join).hasStackTraceContaining("may or may not have applied");
            Tuple2<String, ReadModifyWriteResult> result = second.join().iterator().next();
            assertThat(result.f0).isSameAs(reused);
            assertThat(result.f1.getDestination().getTable()).isEqualTo("table-2");
            assertThat(result.f1.getRow())
                    .isEqualTo(
                            new BigtableRow(
                                    ByteString.copyFromUtf8("same"),
                                    Collections.singletonList(
                                            new BigtableRow.Cell(
                                                    "cf",
                                                    ByteString.EMPTY,
                                                    123000,
                                                    ByteString.copyFrom(new byte[] {-1, 0, 1}),
                                                    Collections.singletonList("label")))));
            assertThat(resolves).hasValue(2);
            assertThat(serializes).hasValue(2);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(1);
            assertThat(metrics.counterValue("requestsTimedOut")).isEqualTo(1);
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
    }

    @Test
    void asyncSkipsAndFailsServiceErrorsWithoutAHandler() throws Exception {
        ReadModifyWriteFunction<String> function = open(config());
        try (AutoCloseable closing = function::close) {
            Answer<Tuple2<String, ReadModifyWriteResult>> skipped = new Answer<>();
            function.asyncInvoke("skip", skipped.resultFuture);
            assertThat(skipped.join()).isEmpty();
            assertThat(client.opened).isEmpty();
            Answer<Tuple2<String, ReadModifyWriteResult>> failed = new Answer<>();
            function.asyncInvoke("bad", failed.resultFuture);
            client.answers.get(0).setException(Status.INVALID_ARGUMENT.asRuntimeException());
            assertThatThrownBy(failed::join).hasStackTraceContaining("INVALID_ARGUMENT");
            assertThat(client.sent).hasSize(1);
        }
    }

    @Test
    void resubmittingAnAppliedButUnacknowledgedRecordCanApplyItAgain() throws Exception {
        AtomicInteger applied = new AtomicInteger();
        try (SingleRowRequestWriter<String> first = writer()) {
            first.write("same", TestContexts.NO_OP);
            applied.addAndGet(
                    (int)
                            client.sent
                                    .get(0)
                                    .toProto(RequestContext.create("p", "i", ""))
                                    .getRules(0)
                                    .getIncrementAmount());
            client.answers.get(0).setException(Status.UNAVAILABLE.asRuntimeException());
            assertThatThrownBy(() -> first.flush(false)).hasMessageContaining("applies it again");
        }
        TestSinkWriterMetricGroup restoredMetrics = TestSinkWriterMetricGroup.create();
        try (SingleRowRequestWriter<String> restored = writer(restoredMetrics)) {
            restored.write("same", TestContexts.NO_OP);
            applied.addAndGet(
                    (int)
                            client.sent
                                    .get(1)
                                    .toProto(RequestContext.create("p", "i", ""))
                                    .getRules(0)
                                    .getIncrementAmount());
            client.answers.get(1).set(row("same"));
            restored.flush(false);
        }
        assertThat(applied).hasValue(2);
        assertThat(client.sent).hasSize(2);
        // A new writer registers fresh counters; applications across recovery are not deduplicated.
        assertThat(metrics.counterValue("requestsAccepted")).isEqualTo(1);
        assertThat(restoredMetrics.counterValue("requestsAccepted")).isEqualTo(1);
    }

    private static Row row(String key) {
        return Row.create(
                ByteString.copyFromUtf8(key),
                List.of(
                        RowCell.create(
                                "cf",
                                ByteString.EMPTY,
                                123000,
                                List.of("label"),
                                ByteString.copyFrom(new byte[] {-1, 0, 1}))));
    }

    private SingleRowRequestWriter<String> writer() {
        return writer(metrics);
    }

    private SingleRowRequestWriter<String> writer(TestSinkWriterMetricGroup writerMetrics) {
        ReadModifyWriteConfig<String> config = config();
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
                writerMetrics);
    }

    private ReadModifyWriteFunction<String> open(ReadModifyWriteConfig<String> config)
            throws Exception {
        ReadModifyWriteFunction<String> function = new ReadModifyWriteFunction<>(config, client);
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

    private static ReadModifyWriteConfig<String> config() {
        return new ReadModifyWriteConfig<>(
                (input, context) -> TABLE,
                (input, context) -> request(input),
                null,
                BigtableRequestOptions.builder().build(),
                null,
                null);
    }

    private static ReadModifyWriteRequest request(String key) {
        return key.equals("skip")
                ? null
                : ReadModifyWriteRequest.of(
                        ByteString.copyFromUtf8(key),
                        List.of(ReadModifyWriteRule.increment("cf", ByteString.EMPTY, 1)));
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
