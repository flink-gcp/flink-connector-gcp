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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerRealGcpJobCleanupTest {
    @Test
    void expectedFailureNeedsNoCancellationOfTheStoppedCluster() throws Exception {
        FakeJobClient job = new FakeJobClient();
        IllegalStateException expected = new IllegalStateException("expected restore refusal");
        job.result.completeExceptionally(expected);
        job.cancellationFailure = new IllegalStateException("cluster already stopped");
        assertThat(SpannerSourceRealGcpITCase.executionFailure(job)).isSameAs(expected);
        assertThat(job.cancelled).isFalse();
    }

    @Test
    void successfulCompletionNeedsNoCancellation() throws Exception {
        FakeJobClient job = new FakeJobClient();
        job.result.complete(null);
        SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close();
        assertThat(job.cancelled).isFalse();
    }

    @Test
    void runningJobIsCancelledAndItsTerminationObserved() throws Exception {
        FakeJobClient job = new FakeJobClient();
        job.completeOnCancel = true;
        SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close();
        assertThat(job.cancelled).isTrue();
        assertThat(job.result).isCompletedExceptionally();
    }

    @Test
    void completionRacingCancellationCanCloseTheClusterFirst() throws Exception {
        FakeJobClient job = new FakeJobClient();
        job.completeOnCancel = true;
        job.cancellationFailure = new IllegalStateException("cluster already stopped");
        SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close();
        assertThat(job.cancelled).isTrue();
        assertThat(job.result).isCompletedExceptionally();
    }

    @Test
    void cancellationFailureIsPreservedWhenTerminationCannotBeEstablished() {
        FakeJobClient job = new FakeJobClient();
        RuntimeException cleanup = new IllegalStateException("cancel failed");
        job.cancellationFailure = cleanup;
        assertThatThrownBy(() -> SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close())
                .isSameAs(cleanup)
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .hasSize(1)
                                        .allMatch(TimeoutException.class::isInstance));
    }

    @Test
    void failedCancellationFutureIsPreservedWhenTerminationCannotBeEstablished() {
        FakeJobClient job = new FakeJobClient();
        RuntimeException cleanup = new IllegalStateException("cancel RPC failed");
        job.cancellation = CompletableFuture.failedFuture(cleanup);
        assertThatThrownBy(() -> SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close())
                .isInstanceOf(ExecutionException.class)
                .hasCause(cleanup)
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .hasSize(1)
                                        .allMatch(TimeoutException.class::isInstance));
    }

    @Test
    void acceptedCancellationStillRequiresTermination() {
        FakeJobClient job = new FakeJobClient();
        assertThatThrownBy(() -> SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close())
                .isInstanceOf(TimeoutException.class);
        assertThat(job.cancelled).isTrue();
    }

    @Test
    void interruptedTerminationWaitRetainsTheCancellationFailure() {
        FakeJobClient job = new FakeJobClient();
        RuntimeException cleanup = new IllegalStateException("cancel failed");
        job.cancellationFailure = cleanup;
        job.interruptOnCancel = true;
        try {
            assertThatThrownBy(() -> SpannerSourceRealGcpITCase.cancelOnClose(job, 1).close())
                    .isInstanceOf(InterruptedException.class)
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed()).containsExactly(cleanup));
        } finally {
            Thread.interrupted();
        }
    }

    private static final class FakeJobClient implements JobClient {
        final CompletableFuture<JobExecutionResult> result = new CompletableFuture<>();
        CompletableFuture<Void> cancellation = CompletableFuture.completedFuture(null);
        RuntimeException cancellationFailure;
        boolean completeOnCancel;
        boolean interruptOnCancel;
        boolean cancelled;

        @Override
        public CompletableFuture<Void> cancel() {
            cancelled = true;
            if (completeOnCancel) {
                result.completeExceptionally(new IllegalStateException("job cancelled"));
            }
            if (interruptOnCancel) {
                Thread.currentThread().interrupt();
            }
            if (cancellationFailure != null) {
                throw cancellationFailure;
            }
            return cancellation;
        }

        @Override
        public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
            return result;
        }

        @Override
        public JobID getJobID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<JobStatus> getJobStatus() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> stopWithSavepoint(
                boolean drain, String directory, SavepointFormatType format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> triggerSavepoint(
                String directory, SavepointFormatType format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Map<String, Object>> getAccumulators() {
            throw new UnsupportedOperationException();
        }
    }
}
