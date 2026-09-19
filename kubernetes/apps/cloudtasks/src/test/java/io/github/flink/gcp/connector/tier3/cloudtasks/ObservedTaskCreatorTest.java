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
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

    @Test
    void retainsRequestAndAbsoluteDeadlineAndObservesBeforeCompletion() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Observation> events = new ArrayList<>();
        AtomicLong clock = new AtomicLong(100);
        ObservedTaskCreator creator =
                new ObservedTaskCreator(delegate, events::add, clock::get, 10);
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
        var creator = new ObservedTaskCreator(delegate, events::add, () -> 1, 1);
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
                        event -> {
                            throw new IllegalStateException("evidence full");
                        },
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
        var creator = new ObservedTaskCreator(delegate, event -> {}, () -> 1, 1);
        creator.createTask(request());
        assertThatThrownBy(() -> creator.createTask(request())).hasMessageContaining("ceiling");
        assertThat(delegate.calls).isEqualTo(1);
    }

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void cancellationReachesTheOriginalCallAndCloseReachesTheClient() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Observation> events = new ArrayList<>();
        var output = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        var log = new ObservationLog(MeasurementOptions.parse(MeasurementOptionsTest.arguments()));
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        event -> {
                            events.add(event);
                            log.accept(event);
                        },
                        () -> 1,
                        1);
        var result = creator.createTask(request());
        AtomicLong rowsAtCancellation = new AtomicLong(-1);
        result.addListener(() -> rowsAtCancellation.set(events.size()), Runnable::run);
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            assertThat(result.cancel(true)).isTrue();
        } finally {
            System.setOut(previous);
        }
        assertThat(rowsAtCancellation).hasValue(0);
        assertThat(events).hasSize(1);
        assertThat(delegate.future.isCancelled()).isTrue();
        assertThat(result.isCancelled()).isTrue();
        assertThat(output.toString(StandardCharsets.UTF_8).lines()).hasSize(1);
        String[] fields = output.toString(StandardCharsets.UTF_8).strip().split(",", -1);
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
        var creator = new ObservedTaskCreator(delegate, events::add, () -> 1, 1);
        assertThatThrownBy(() -> creator.createTask(request()))
                .isSameAs(delegate.synchronousFailure);
        assertThat(events.get(0).failure()).isSameAs(delegate.synchronousFailure);
    }

    @Test
    void terminalCountsAreIndependentOfTheExportedRows() throws Exception {
        FakeCreator delegate = new FakeCreator();
        List<ObservedTaskCreator.Terminal> receipts = new ArrayList<>();
        List<ObservedTaskCreator.Observation> exported = new ArrayList<>();
        var creator = new ObservedTaskCreator(delegate, exported::add, () -> 1, 2, receipts::add);
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
        var creator = new ObservedTaskCreator(delegate, event -> {}, () -> 1, 1, receipts::add);
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
                        event -> {
                            throw new IllegalStateException("output lost");
                        },
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
        var creator = new ObservedTaskCreator(delegate, event -> {}, () -> 1, 1, receipts::add);
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
        var creator =
                new ObservedTaskCreator(
                        delegate,
                        event -> {},
                        () -> 1,
                        1,
                        receipt -> {
                            throw new IllegalStateException("receipt lost");
                        });
        assertThatThrownBy(creator::close).hasMessage("receipt lost");
        assertThat(delegate.closed).isTrue();
    }

    private static final class FakeCreator implements TaskCreator {
        final SettableApiFuture<Task> future = SettableApiFuture.create();
        CreateTaskRequest request;
        Deadline deadline;
        int calls;
        int deadlineCalls;
        boolean closed;
        RuntimeException synchronousFailure;

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
        }
    }
}
