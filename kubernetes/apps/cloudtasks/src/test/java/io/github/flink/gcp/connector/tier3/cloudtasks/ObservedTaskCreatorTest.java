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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.grpc.Deadline;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservedTaskCreatorTest {
    private static CreateTaskRequest request() {
        return CreateTaskRequest.newBuilder()
                .setParent("projects/test/locations/test/queues/test")
                .setTask(
                        Task.newBuilder()
                                .setHttpRequest(
                                        HttpRequest.newBuilder()
                                                .setBody(MeasurementPayload.create(1024, 7))))
                .build();
    }

    private static FakeObserver observer(Consumer<ObservedTaskCreator.Observation> consumer) {
        return new FakeObserver(consumer);
    }

    @Test
    void retainsRequestAndAbsoluteDeadlineAndObservesBeforeCompletion() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Observation> events = new ArrayList<>();
        AtomicLong clock = new AtomicLong(100);
        ObservedTaskCreator creator =
                new ObservedTaskCreator(delegate, observer(events::add), clock::get, 10);
        var input = request();
        Deadline deadline = Deadline.after(5, TimeUnit.SECONDS);
        ApiFuture<Task> result = creator.createTask(input, deadline);
        assertThat(delegate.request).isSameAs(input);
        assertThat(delegate.deadline).isSameAs(deadline);
        assertThat(events).isEmpty();
        assertThat(result.isDone()).isFalse();
        AtomicBoolean ordered = new AtomicBoolean();
        result.addListener(() -> ordered.set(events.size() == 1), Runnable::run);
        clock.set(150);
        Task response = Task.newBuilder().setName("created").build();
        delegate.future.set(response);
        assertThat(result.get()).isSameAs(response);
        assertThat(ordered).isTrue();
        assertThat(events.get(0).completedNanos() - events.get(0).startedNanos()).isEqualTo(50);
        assertThat(events.get(0).origin().sequence()).isEqualTo(7);
        assertThat(events.get(0).attempt()).isEqualTo(1);
    }

    @Test
    void preservesOriginalAsyncFailureAndUsesNonDeadlineOverload() {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Observation> events = new ArrayList<>();
        var creator = new ObservedTaskCreator(delegate, observer(events::add), () -> 1, 1);
        ApiFuture<Task> result = creator.createTask(request());
        RuntimeException failure = new IllegalStateException("rpc failed");
        delegate.future.setException(failure);
        assertThatThrownBy(result::get).hasCause(failure);
        assertThat(events.get(0).failure()).isSameAs(failure);
        assertThat(delegate.deadline).isNull();
        assertThat(delegate.deadlineCalls).isZero();
    }

    @Test
    void observationFailureCannotPublishSuccess() {
        FakeCreator delegate = new FakeCreator();
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        observer(
                                event -> {
                                    throw new IllegalStateException("evidence full");
                                }),
                        () -> 1,
                        1);
        ApiFuture<Task> result = creator.createTask(request());
        delegate.future.set(Task.getDefaultInstance());
        assertThatThrownBy(result::get)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("evidence full");
    }

    @Test
    void capsRpcAttemptsBeforeCallingTheService() {
        FakeCreator delegate = new FakeCreator();
        var creator = new ObservedTaskCreator(delegate, observer(event -> {}), () -> 1, 1);
        creator.createTask(request());
        assertThatThrownBy(() -> creator.createTask(request())).hasMessageContaining("ceiling");
        assertThat(delegate.calls).isEqualTo(1);
    }

    @Test
    void cancellationReachesTheOriginalCallAndCloseReachesTheClient() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Observation> events = new ArrayList<>();
        var output = new FakeRowsOutput();
        var log =
                new ObservationLog(
                        MeasurementOptions.parse(MeasurementOptionsTest.arguments()),
                        UUID.randomUUID(),
                        output);
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        observer(
                                event -> {
                                    events.add(event);
                                    log.accept(event);
                                }),
                        () -> 1,
                        1);
        var result = creator.createTask(request());
        AtomicLong rowsAtCancellation = new AtomicLong(-1);
        result.addListener(() -> rowsAtCancellation.set(events.size()), Runnable::run);
        assertThat(result.cancel(true)).isTrue();
        assertThat(rowsAtCancellation).hasValue(0);
        assertThat(events).hasSize(1);
        assertThat(delegate.future.isCancelled()).isTrue();
        assertThat(result.isCancelled()).isTrue();
        assertThat(output.rows).hasSize(1);
        String[] fields = output.rows.get(0).split(",", -1);
        assertThat(fields).hasSize(16);
        assertThat(fields[7]).isEqualTo("7");
        assertThat(fields[14]).isEqualTo("CANCELLED");
        creator.close();
        assertThat(delegate.closed).isTrue();
    }

    @Test
    void synchronousFailureIsObservedAndRethrownUnchanged() {
        FakeCreator delegate = new FakeCreator();
        delegate.synchronousFailure = new IllegalArgumentException("synchronous");
        List<ObservedTaskCreator.Observation> events = new ArrayList<>();
        var creator = new ObservedTaskCreator(delegate, observer(events::add), () -> 1, 1);
        assertThatThrownBy(() -> creator.createTask(request()))
                .isSameAs(delegate.synchronousFailure);
        assertThat(events.get(0).failure()).isSameAs(delegate.synchronousFailure);
    }

    @Test
    void terminalCountsAreIndependentOfTheExportedRows() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        List<ObservedTaskCreator.Observation> exported = new ArrayList<>();
        var creator =
                new ObservedTaskCreator(
                        delegate, observer(exported::add), () -> 1, 2, receipts::add);
        creator.createTask(request());
        delegate.future.set(Task.getDefaultInstance());
        creator.close();
        exported.clear(); // Simulate losing the final CSV row after successful local output.
        assertThat(receipts).hasSize(1);
        assertThat(receipts.get(0).attempts()).isEqualTo(1);
        assertThat(receipts.get(0).observations()).isEqualTo(1);
        assertThat(receipts.get(0).complete()).isTrue();
        assertThat(exported.size()).isNotEqualTo(receipts.get(0).observations());
        assertThatThrownBy(() -> creator.createTask(request())).hasMessageContaining("closed");
        creator.close();
        assertThat(receipts).hasSize(1);
    }

    @Test
    void closeBeforeCallbackNeverCertifiesCompleteEvidence() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        var creator =
                new ObservedTaskCreator(delegate, observer(event -> {}), () -> 1, 1, receipts::add);
        creator.createTask(request());
        creator.close();
        assertThat(receipts.get(0).attempts()).isEqualTo(1);
        assertThat(receipts.get(0).completed()).isZero();
        assertThat(receipts.get(0).complete()).isFalse();
        delegate.future.set(Task.getDefaultInstance());
        assertThat(receipts.get(0).complete()).isFalse();
    }

    @Test
    void outputFailureAndAttemptExhaustionInvalidateTheReceipt() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        observer(
                                event -> {
                                    throw new IllegalStateException("output lost");
                                }),
                        () -> 1,
                        1,
                        receipts::add);
        var result = creator.createTask(request());
        delegate.future.set(Task.getDefaultInstance());
        assertThatThrownBy(result::get).hasRootCauseMessage("output lost");
        assertThatThrownBy(() -> creator.createTask(request())).hasMessageContaining("ceiling");
        creator.close();
        assertThat(receipts.get(0).attempts()).isEqualTo(1);
        assertThat(receipts.get(0).completed()).isEqualTo(1);
        assertThat(receipts.get(0).observations()).isZero();
        assertThat(receipts.get(0).evidenceFailed()).isTrue();
        assertThat(receipts.get(0).limitReached()).isTrue();
        assertThat(receipts.get(0).complete()).isFalse();
        assertThat(delegate.calls).isEqualTo(1);
    }

    @Test
    void attemptExhaustionInvalidatesOtherwiseCompleteEvidence() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        var creator =
                new ObservedTaskCreator(delegate, observer(event -> {}), () -> 1, 1, receipts::add);
        creator.createTask(request());
        delegate.future.set(Task.getDefaultInstance());
        assertThatThrownBy(() -> creator.createTask(request())).hasMessageContaining("ceiling");
        creator.close();
        var receipt = receipts.get(0);
        assertThat(receipt.attempts()).isEqualTo(1);
        assertThat(receipt.completed()).isEqualTo(1);
        assertThat(receipt.observations()).isEqualTo(1);
        assertThat(receipt.evidenceFailed()).isFalse();
        assertThat(receipt.limitReached()).isTrue();
        assertThat(receipt.complete()).isFalse();
    }

    @Test
    void terminalExportFailureStillClosesTheRpcClient() {
        FakeCreator delegate = new FakeCreator();
        var observer = observer(event -> {});
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        observer,
                        () -> 1,
                        1,
                        receipt -> {
                            throw new IllegalStateException("receipt lost");
                        });
        assertThatThrownBy(creator::close).hasMessage("receipt lost");
        assertThat(delegate.closed).isTrue();
        assertThat(observer.closed).isTrue();
    }

    @Test
    void observerIsClosedAfterTheClientAndBeforeTheTerminalSnapshot() throws Exception {
        FakeCreator delegate = new FakeCreator();
        var observer = observer(event -> {});
        observer.client = delegate;
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        AtomicBoolean observerClosedAtReceipt = new AtomicBoolean();
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        observer,
                        () -> 1,
                        2,
                        receipt -> {
                            observerClosedAtReceipt.set(observer.closed);
                            receipts.add(receipt);
                        });
        creator.createTask(request());
        delegate.future.set(Task.getDefaultInstance());
        creator.close();
        assertThat(observer.clientClosedAtClose).isTrue();
        assertThat(observerClosedAtReceipt).isTrue();
        // The fake reports rows as durable only after close, like a storage part.
        assertThat(receipts.get(0).rowsExported()).isEqualTo(1);
        assertThat(receipts.get(0).partsClosed()).isEqualTo(1);
        assertThat(receipts.get(0).rowsFlushFailed()).isFalse();
        assertThat(receipts.get(0).csvEnabled()).isTrue();
        assertThat(receipts.get(0).complete()).isTrue();
    }

    @Test
    void rowsFlushFailureInvalidatesTheReceiptAndIsRethrownAfterTheReceipt() throws Exception {
        FakeCreator delegate = new FakeCreator();
        var observer = observer(event -> {});
        observer.closeFailure = new IOException("flush lost");
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        var creator = new ObservedTaskCreator(delegate, observer, () -> 1, 2, receipts::add);
        creator.createTask(request());
        delegate.future.set(Task.getDefaultInstance());
        assertThatThrownBy(creator::close).isSameAs(observer.closeFailure);
        assertThat(delegate.closed).isTrue();
        assertThat(receipts).hasSize(1);
        assertThat(receipts.get(0).rowsFlushFailed()).isTrue();
        assertThat(receipts.get(0).rowsExported()).isZero();
        assertThat(receipts.get(0).complete()).isFalse();
    }

    @Test
    void rowsFlushFailureSuppressesTheClientCloseFailure() {
        FakeCreator delegate = new FakeCreator();
        delegate.closeFailure = new IllegalStateException("client close failed");
        var observer = observer(event -> {});
        observer.closeFailure = new IOException("flush lost");
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        var creator = new ObservedTaskCreator(delegate, observer, () -> 1, 1, receipts::add);
        assertThatThrownBy(creator::close)
                .isSameAs(observer.closeFailure)
                .hasSuppressedException(delegate.closeFailure);
        assertThat(receipts.get(0).clientCloseFailed()).isTrue();
        assertThat(receipts.get(0).rowsFlushFailed()).isTrue();
    }

    @Test
    void completeRequiresExportedRowsToMatchObservationsOnlyWhenCsvIsEnabled() throws Exception {
        for (boolean csvEnabled : new boolean[] {true, false}) {
            FakeCreator delegate = new FakeCreator();
            var observer = observer(event -> {});
            observer.exportsRows = csvEnabled;
            observer.lostRows = 1;
            List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
            var creator = new ObservedTaskCreator(delegate, observer, () -> 1, 2, receipts::add);
            creator.createTask(request());
            delegate.future.set(Task.getDefaultInstance());
            creator.close();
            var receipt = receipts.get(0);
            assertThat(receipt.observations()).isEqualTo(1);
            assertThat(receipt.rowsExported()).isZero();
            assertThat(receipt.csvEnabled()).isEqualTo(csvEnabled);
            assertThat(receipt.complete()).isEqualTo(!csvEnabled);
        }
    }

    private static final class FakeObserver implements ObservedTaskCreator.Observer {
        private final Consumer<ObservedTaskCreator.Observation> consumer;
        private long accepted;
        boolean exportsRows = true;
        long lostRows;
        boolean closed;
        boolean clientClosedAtClose;
        IOException closeFailure;
        FakeCreator client;

        private FakeObserver(Consumer<ObservedTaskCreator.Observation> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void accept(ObservedTaskCreator.Observation observation) {
            consumer.accept(observation);
            accepted++;
        }

        @Override
        public boolean exportsRows() {
            return exportsRows;
        }

        @Override
        public long rowsExported() {
            return closed ? accepted - lostRows : 0;
        }

        @Override
        public long partsClosed() {
            return closed && accepted > 0 ? 1 : 0;
        }

        @Override
        public void close() throws IOException {
            clientClosedAtClose = client != null && client.closed;
            if (closeFailure != null) {
                throw closeFailure;
            }
            closed = true;
        }
    }

    private static final class FakeCreator implements TaskCreator {
        final SettableApiFuture<Task> future = SettableApiFuture.create();
        CreateTaskRequest request;
        Deadline deadline;
        int calls;
        int deadlineCalls;
        boolean closed;
        RuntimeException synchronousFailure;
        RuntimeException closeFailure;

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest input) {
            calls++;
            request = input;
            if (synchronousFailure != null) {
                throw synchronousFailure;
            }
            return future;
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest input, Deadline limit) {
            deadlineCalls++;
            deadline = limit;
            return createTask(input);
        }

        @Override
        public void close() {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
