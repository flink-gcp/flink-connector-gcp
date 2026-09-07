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

package io.github.flink.gcp.connector.cloudtasks.sink.committer;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCommittable;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions.ExpiredEnvelopePolicy;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TimeSource;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import io.grpc.Deadline;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(20)
class CloudTasksStagedCommitterTest {
    private static final String QUEUE = "projects/p/locations/l/queues/q";
    private final AtomicLong wall = new AtomicLong(1_001);
    private final TestSinkCommitterMetricGroup metrics = TestSinkCommitterMetricGroup.create();
    private final FakeCreator creator = new FakeCreator();
    private final CloudTasksStagedOptions defaults = CloudTasksStagedOptions.builder().build();

    @Test
    void commitTimeMetricsIncludeQueuedAndAlreadyCompletedWorkAndUseEffectiveDeadlines()
            throws Exception {
        wall.set(1500);
        creator.script =
                call -> {
                    assertThat(metrics.<Long>gaugeValue("currentCommitOldestTaskAgeMillis"))
                            .isEqualTo(wall.get() - 900);
                    assertThat(metrics.<Long>gaugeValue("currentCommitReplayBudgetMillis"))
                            .isEqualTo(2100 - wall.get());
                    wall.addAndGet(100);
                    return ApiFutures.immediateFuture(call.request.getTask());
                };
        var tighter =
                CloudTasksStagedOptions.builder()
                        .nameRetention(Duration.ofSeconds(2))
                        .clockSkewAllowance(Duration.ZERO)
                        .requestTimeout(Duration.ofMillis(800))
                        .build();
        try (var committer = committer(tighter, 1, 2, 2)) {
            assertThat(metrics.<Long>gaugeValue("currentCommitReplayBudgetMillis")).isEqualTo(-1);
            committer.commit(List.of(request(1, 900, 5000), request(2, 1000, 2500)));
            assertThat(metrics.<Long>gaugeValue("currentCommitOldestTaskAgeMillis")).isEqualTo(-1);
            assertThat(metrics.<Long>gaugeValue("currentCommitReplayBudgetMillis")).isEqualTo(-1);
        }
        assertThat(creator.calls).hasSize(2);
    }

    @Test
    void lostResponseRetriesIdenticalTaskAndSignalsTheCollision() throws Exception {
        creator.script =
                call ->
                        creator.calls.size() == 1
                                ? ApiFutures.immediateFailedFuture(
                                        Status.DEADLINE_EXCEEDED.asRuntimeException())
                                : ApiFutures.immediateFailedFuture(
                                        Status.ALREADY_EXISTS.asRuntimeException());
        Request request = request(1, 1000, 3_281_000);
        try (var committer = committer(defaults, 2, 3, 2)) {
            committer.commit(List.of(request));
        }
        assertThat(creator.calls).hasSize(2);
        assertThat(creator.calls.get(0).request).isEqualTo(creator.calls.get(1).request);
        assertThat(creator.calls.get(0).request.getTask().toByteString())
                .isEqualTo(request.envelope.getTaskBytes());
        assertThat(request.alreadyCommitted).isEqualTo(1);
        assertThat(metrics.counterValue("tasksDeduplicated")).isEqualTo(1);
        assertThat(creator.closes).isEqualTo(1);
        assertThat(metrics.counterValue("errorClass", "DEADLINE_EXCEEDED", "errors")).isEqualTo(1);
        assertThat(metrics.counterValue("destination", QUEUE, "recordsSend")).isEqualTo(1);
        assertThat(metrics.counterValue("destination", QUEUE, "sendErrors")).isZero();
    }

    @Test
    void mixedStatusBudgetsRemainSeparateAndBounded() throws Exception {
        List<Status> statuses =
                List.of(
                        Status.UNAVAILABLE,
                        Status.NOT_FOUND,
                        Status.RESOURCE_EXHAUSTED,
                        Status.NOT_FOUND);
        creator.script =
                call ->
                        ApiFutures.immediateFailedFuture(
                                statuses.get(creator.calls.size() - 1).asRuntimeException());
        try (var committer = committer(defaults, 1, 3, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request(1, 1000, 3_281_000))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("NOT_FOUND")
                    .hasMessageContaining("notFoundFailures=2");
        }
        assertThat(creator.calls).hasSize(4);
    }

    @Test
    void terminalFailureKeepsTheRequestUnsignaledAndSanitizesTheCause() throws Exception {
        creator.script =
                call ->
                        ApiFutures.immediateFailedFuture(
                                Status.INVALID_ARGUMENT
                                        .withDescription("secret-body Authorization: token")
                                        .asRuntimeException());
        Request request = request(1, 1000, 3_281_000);
        try (var committer = committer(defaults, 1, 2, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("INVALID_ARGUMENT")
                    .hasMessageNotContaining("secret-body")
                    .hasNoCause();
        }
        assertThat(request.alreadyCommitted).isZero();
        assertThat(creator.calls).hasSize(1);
    }

    @Test
    void equalityExpiresBeforeTheFirstSend() throws Exception {
        wall.set(2000);
        try (var committer = committer(defaults, 1, 2, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request(1, 1000, 2000))))
                    .hasMessageContaining("expired")
                    .hasMessageContaining("originEpochMillis=1000")
                    .hasMessageContaining("authorizationDeadlineMillis=2000")
                    .hasMessageContaining("now=2000")
                    .hasMessageContaining("expiredEnvelopePolicy");
            assertThat(metrics.counterValue("expiredEnvelopesFailed")).isEqualTo(1);
            assertThat(metrics.<Long>gaugeValue("currentCommitOldestTaskAgeMillis")).isEqualTo(-1);
            assertThat(metrics.<Long>gaugeValue("currentCommitReplayBudgetMillis")).isEqualTo(-1);
        }
        assertThat(creator.calls).isEmpty();
    }

    @Test
    void restoringWithLooserOptionsNeverExtendsThePersistedDeadline() throws Exception {
        wall.set(2000);
        var looser = CloudTasksStagedOptions.builder().nameRetention(Duration.ofDays(9)).build();
        try (var committer = committer(looser, 1, 2, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request(1, 1000, 2000))))
                    .hasMessageContaining("authorizationDeadlineMillis=2000");
        }
        assertThat(creator.calls).isEmpty();
    }

    @Test
    void tighterCurrentSettingsShortenAnOldEnvelope() throws Exception {
        wall.set(3000);
        var tighter =
                CloudTasksStagedOptions.builder()
                        .nameRetention(Duration.ofSeconds(3))
                        .clockSkewAllowance(Duration.ZERO)
                        .requestTimeout(Duration.ofSeconds(1))
                        .build();
        try (var committer = committer(tighter, 1, 2, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request(1, 1000, 3_281_000))))
                    .hasMessageContaining("authorizationDeadlineMillis=3000");
        }
        assertThat(creator.calls).isEmpty();
    }

    @Test
    void retryChecksExpiryAgainAfterAnAmbiguousFailure() throws Exception {
        creator.script =
                call -> {
                    wall.set(2000);
                    return ApiFutures.immediateFailedFuture(
                            Status.DEADLINE_EXCEEDED.asRuntimeException());
                };
        try (var committer = committer(defaults, 1, 3, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request(1, 1000, 2000))))
                    .hasMessageContaining("expired");
        }
        assertThat(creator.calls).hasSize(1);
    }

    @Test
    void queuedWorkExpiresBeforeItsSlotBecomesAvailable() throws Exception {
        creator.script =
                call -> {
                    wall.set(2000);
                    return ApiFutures.immediateFuture(call.request.getTask());
                };
        try (var committer = committer(defaults, 1, 2, 2)) {
            assertThatThrownBy(
                            () ->
                                    committer.commit(
                                            List.of(
                                                    request(1, 1000, 2000),
                                                    request(2, 1000, 2000))))
                    .hasMessageContaining("expired");
        }
        assertThat(creator.calls).hasSize(1);
    }

    @ParameterizedTest
    @EnumSource(ExpiredEnvelopePolicy.class)
    void explicitExpiryPoliciesHaveOnlyTheirDocumentedEffect(ExpiredEnvelopePolicy policy)
            throws Exception {
        wall.set(2000);
        var options = CloudTasksStagedOptions.builder().expiredEnvelopePolicy(policy).build();
        Request request = request(1, 1000, 2000);
        try (var committer = committer(options, 1, 2, 2)) {
            if (policy == ExpiredEnvelopePolicy.FAIL) {
                assertThatThrownBy(() -> committer.commit(List.of(request)))
                        .hasMessageContaining("expired");
            } else {
                committer.commit(List.of(request));
            }
        }
        assertThat(creator.calls).hasSize(policy == ExpiredEnvelopePolicy.CREATE_ANYWAY ? 1 : 0);
        assertThat(request.alreadyCommitted)
                .isEqualTo(policy == ExpiredEnvelopePolicy.ASSUME_COMMITTED ? 1 : 0);
        assertThat(metrics.counterValue("tasksDeduplicated")).isZero();
        assertThat(metrics.counterValue("expiredEnvelopesAssumedCommitted"))
                .isEqualTo(policy == ExpiredEnvelopePolicy.ASSUME_COMMITTED ? 1 : 0);
        assertThat(metrics.counterValue("expiredEnvelopesDropped"))
                .isEqualTo(policy == ExpiredEnvelopePolicy.DROP ? 1 : 0);
        assertThat(metrics.counterValue("expiredEnvelopeCreatesAuthorized"))
                .isEqualTo(policy == ExpiredEnvelopePolicy.CREATE_ANYWAY ? 1 : 0);
    }

    @Test
    void expiredCreateAuthorizationsCountRetriesButNotOrdinarySends() throws Exception {
        creator.script =
                call ->
                        creator.calls.size() == 2
                                ? ApiFutures.immediateFailedFuture(
                                        Status.UNAVAILABLE.asRuntimeException())
                                : ApiFutures.immediateFuture(call.request.getTask());
        var options =
                CloudTasksStagedOptions.builder()
                        .expiredEnvelopePolicy(ExpiredEnvelopePolicy.CREATE_ANYWAY)
                        .build();
        try (var committer = committer(options, 1, 3, 2)) {
            committer.commit(List.of(request(1, 1000, 2000)));
            wall.set(2000);
            committer.commit(List.of(request(2, 1000, 2000)));
        }
        assertThat(creator.calls).hasSize(3);
        assertThat(metrics.counterValue("expiredEnvelopeCreatesAuthorized")).isEqualTo(2);
        assertThat(metrics.counterValue("expiredEnvelopesAssumedCommitted")).isZero();
        assertThat(metrics.counterValue("expiredEnvelopesDropped")).isZero();
        assertThat(metrics.counterValue("tasksDeduplicated")).isZero();
    }

    @ParameterizedTest
    @EnumSource(ExpiredEnvelopePolicy.class)
    void queueMismatchNeverUsesAnExpiryOverride(ExpiredEnvelopePolicy policy) throws Exception {
        wall.set(2000);
        var options = CloudTasksStagedOptions.builder().expiredEnvelopePolicy(policy).build();
        Task task =
                task(1).toBuilder()
                        .setName("projects/p/locations/l/queues/other/tasks/" + "a".repeat(32))
                        .build();
        var request =
                new Request(
                        CloudTasksCommittable.fromTask(
                                "projects/p/locations/l/queues/other", 1000, 2000, task));
        try (var committer = committer(options, 1, 2, 2)) {
            assertThatThrownBy(() -> committer.commit(List.of(request)))
                    .hasMessageContaining("original fixed destination");
        }
        assertThat(request.alreadyCommitted).isZero();
        assertThat(creator.calls).isEmpty();
    }

    @Test
    void concurrentCreatesStayBoundedAndCompletionReleasesOneSlot() throws Exception {
        creator.script = call -> call.response;
        var executor = Executors.newSingleThreadExecutor();
        try (var committer = committer(defaults, 2, 2, 2)) {
            var commit =
                    executor.submit(
                            () -> {
                                committer.commit(
                                        List.of(
                                                request(1, 1000, 3_281_000),
                                                request(2, 1000, 3_281_000),
                                                request(3, 1000, 3_281_000)));
                                return null;
                            });
            Call first = creator.next();
            Call second = creator.next();
            assertThat(creator.calls).hasSize(2);
            second.response.set(second.request.getTask());
            Call third = creator.next();
            assertThat(creator.calls).hasSize(3);
            first.response.set(first.request.getTask());
            third.response.set(third.request.getTask());
            commit.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void closeInterruptsPendingWorkAndNeverDispatchesQueuedRecords() throws Exception {
        creator.script = call -> call.response;
        var executor = Executors.newSingleThreadExecutor();
        var committer = committer(defaults, 1, 2, 2);
        try {
            var commit =
                    executor.submit(
                            () -> {
                                committer.commit(
                                        List.of(
                                                request(1, 1000, 3_281_000),
                                                request(2, 1000, 3_281_000)));
                                return null;
                            });
            Call pending = creator.next();
            committer.close();
            assertThatThrownBy(() -> commit.get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOfAny(InterruptedException.class, IOException.class);
            assertThat(pending.response.isCancelled()).isTrue();
            assertThat(creator.calls).hasSize(1);
            committer.close();
            assertThat(creator.closes).isEqualTo(1);
        } finally {
            committer.close();
            executor.shutdownNow();
        }
    }

    @Test
    void aLateCallbackCannotCompleteOrRetryItsReplacementAttempt() throws Exception {
        var oldResponse = SettableApiFuture.<Task>create();
        creator.script =
                call -> creator.calls.size() == 1 ? uncancellable(oldResponse) : call.response;
        var executor = Executors.newSingleThreadExecutor();
        var options =
                CloudTasksStagedOptions.builder().requestTimeout(Duration.ofMillis(200)).build();
        try (var committer = committer(options, 1, 3, 2)) {
            var committed =
                    executor.submit(
                            () -> {
                                committer.commit(List.of(request(1, 1000, 3_281_000)));
                                return null;
                            });
            Call old = creator.next();
            Call replacement = creator.next();
            assertThat(old.deadline.isExpired()).isTrue();
            oldResponse.setException(Status.UNAVAILABLE.asRuntimeException());
            replacement.response.set(replacement.request.getTask());
            committed.get(10, TimeUnit.SECONDS);
            assertThat(creator.calls).hasSize(2);
            assertThat(metrics.hasMetric("errorClass", "UNAVAILABLE", "errors")).isFalse();
            assertThat(metrics.counterValue("errorClass", "DEADLINE_EXCEEDED", "errors"))
                    .isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void lateServiceAcceptanceAfterCloseCannotAuthorizeAnotherSend() throws Exception {
        var oldResponse = SettableApiFuture.<Task>create();
        creator.script = call -> uncancellable(oldResponse);
        var executor = Executors.newSingleThreadExecutor();
        var committer = committer(defaults, 1, 3, 2);
        try {
            var committed =
                    executor.submit(
                            () -> {
                                committer.commit(
                                        List.of(
                                                request(1, 1000, 3_281_000),
                                                request(2, 1000, 3_281_000)));
                                return null;
                            });
            Call old = creator.next();
            committer.close();
            assertThatThrownBy(() -> committed.get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOfAny(InterruptedException.class, IOException.class);
            // A transport's cancellation cannot retract the service's late acceptance.
            oldResponse.set(old.request.getTask());
            assertThat(creator.calls).hasSize(1);
            assertThat(metrics.counterValue("tasksDeduplicated")).isZero();
        } finally {
            committer.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @EnumSource(ExpiredEnvelopePolicy.class)
    void corruptTaskBytesAndUnknownDeadlinesNeverUseExpiryOverrides(ExpiredEnvelopePolicy policy)
            throws Exception {
        wall.set(2000);
        var options = CloudTasksStagedOptions.builder().expiredEnvelopePolicy(policy).build();
        for (String fieldName : List.of("taskBytes", "authorizationDeadlineMillis")) {
            Request request = request(1, 1000, 2000);
            var field = CloudTasksCommittable.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(
                    request.envelope,
                    fieldName.equals("taskBytes")
                            ? ByteString.copyFrom(new byte[] {(byte) 0xff})
                            : 0L);
            try (var committer = committer(options, 1, 2, 2)) {
                assertThatThrownBy(() -> committer.commit(List.of(request)))
                        .isInstanceOf(IOException.class);
            }
            assertThat(request.alreadyCommitted).isZero();
        }
        assertThat(creator.calls).isEmpty();
    }

    private static ApiFuture<Task> uncancellable(ApiFuture<Task> delegate) {
        return new ApiFuture<>() {
            @Override
            public boolean cancel(boolean interrupt) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return delegate.isDone();
            }

            @Override
            public Task get() throws InterruptedException, java.util.concurrent.ExecutionException {
                return delegate.get();
            }

            @Override
            public Task get(long duration, TimeUnit unit)
                    throws InterruptedException,
                            java.util.concurrent.ExecutionException,
                            java.util.concurrent.TimeoutException {
                return delegate.get(duration, unit);
            }

            @Override
            public void addListener(Runnable listener, java.util.concurrent.Executor executor) {
                delegate.addListener(listener, executor);
            }
        };
    }

    private CloudTasksStagedCommitter committer(
            CloudTasksStagedOptions options,
            int concurrency,
            int transientAttempts,
            int notFoundAttempts) {
        var writer =
                CloudTasksWriterOptions.builder()
                        .maxInFlightTasks(concurrency)
                        .perDestinationMetrics(true)
                        .recoveryInitialBackoff(Duration.ofMillis(1))
                        .recoveryMaxBackoff(Duration.ofMillis(1))
                        .recoveryMaxAttempts(transientAttempts)
                        .notFoundRecoveryInitialBackoff(Duration.ofMillis(1))
                        .notFoundRecoveryMaxBackoff(Duration.ofMillis(1))
                        .notFoundRecoveryMaxAttempts(notFoundAttempts)
                        .build();
        return new CloudTasksStagedCommitter(
                QUEUE,
                options,
                writer,
                creator,
                new TimeSource() {
                    @Override
                    public long currentTimeMillis() {
                        return wall.get();
                    }

                    @Override
                    public void sleep(long millis) {
                        throw new AssertionError("No wall-clock sleeps");
                    }
                },
                metrics);
    }

    private static Request request(int id, long origin, long deadline) throws IOException {
        return new Request(CloudTasksCommittable.fromTask(QUEUE, origin, deadline, task(id)));
    }

    private static Task task(int id) {
        return Task.newBuilder()
                .setName(QUEUE + "/tasks/" + String.format("%032x", id))
                .setHttpRequest(
                        HttpRequest.newBuilder()
                                .setUrl("https://example.com/task")
                                .setHttpMethod(HttpMethod.POST)
                                .setBody(ByteString.copyFromUtf8("body-" + id)))
                .build();
    }

    private static final class Request implements CommitRequest<CloudTasksCommittable> {
        private final CloudTasksCommittable envelope;
        private int alreadyCommitted;

        private Request(CloudTasksCommittable envelope) {
            this.envelope = envelope;
        }

        @Override
        public CloudTasksCommittable getCommittable() {
            return envelope;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted++;
        }

        @Override
        public void retryLater() {
            throw new AssertionError("retryLater loses the bounded schedule");
        }

        @Override
        public void updateAndRetryLater(CloudTasksCommittable value) {
            throw new AssertionError("Cannot replace durable identity");
        }

        @Override
        public void signalFailedWithKnownReason(Throwable failure) {
            throw new AssertionError("Cannot finalize a failed create");
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable failure) {
            throw new AssertionError("Terminal failures must throw");
        }
    }

    private static final class Call {
        private final CreateTaskRequest request;
        private final Deadline deadline;
        private final SettableApiFuture<Task> response = SettableApiFuture.create();

        private Call(CreateTaskRequest request, Deadline deadline) {
            this.request = request;
            this.deadline = deadline;
        }
    }

    private static final class FakeCreator implements TaskCreator {
        private final List<Call> calls = new CopyOnWriteArrayList<>();
        private final BlockingQueue<Call> arrivals = new LinkedBlockingQueue<>();
        private Function<Call, ApiFuture<Task>> script =
                call -> ApiFutures.immediateFuture(call.request.getTask());
        private int closes;

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request) {
            throw new AssertionError("Absolute deadline required");
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
            assertThat(deadline).isNotNull();
            Call call = new Call(request, deadline);
            calls.add(call);
            arrivals.add(call);
            return script.apply(call);
        }

        private Call next() throws InterruptedException {
            Call call = arrivals.poll(10, TimeUnit.SECONDS);
            assertThat(call).as("next authorized create").isNotNull();
            return call;
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
