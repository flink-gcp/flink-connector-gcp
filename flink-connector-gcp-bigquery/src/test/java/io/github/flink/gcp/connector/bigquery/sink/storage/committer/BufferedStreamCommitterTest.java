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

package io.github.flink.gcp.connector.bigquery.sink.storage.committer;

import org.apache.flink.api.connector.sink2.Committer;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Any;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.FakeBufferedStreamService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferedStreamCommitter}. */
class BufferedStreamCommitterTest {

    private static final String STREAM = "projects/p/datasets/d/tables/t/streams/s1";
    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    /** Commit request whose retry signals are never expected to be called. */
    private static final class TestCommitRequest
            implements Committer.CommitRequest<BufferedStreamCommittable> {

        private final BufferedStreamCommittable committable;

        TestCommitRequest(BufferedStreamCommittable committable) {
            this.committable = committable;
        }

        @Override
        public BufferedStreamCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {}

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(BufferedStreamCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {}
    }

    private static List<Committer.CommitRequest<BufferedStreamCommittable>> requests(
            BufferedStreamCommittable... committables) {
        return java.util.Arrays.stream(committables)
                .map(
                        c ->
                                (Committer.CommitRequest<BufferedStreamCommittable>)
                                        new TestCommitRequest(c))
                .collect(java.util.stream.Collectors.toList());
    }

    private static BufferedStreamCommittable committable(
            String streamName, long flushOffset, int subtaskId) {
        return new BufferedStreamCommittable(streamName, flushOffset, subtaskId);
    }

    private static BufferedStreamCommitter committer(FakeBufferedStreamService service) {
        return committer(service, CreateDisposition.CREATE_IF_NEEDED);
    }

    private static BufferedStreamCommitter committer(
            FakeBufferedStreamService service, CreateDisposition createDisposition) {
        return new BufferedStreamCommitter(
                service.asFactory(),
                null,
                BufferedStreamOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(1))
                        .recoveryMaxBackoff(Duration.ofMillis(1))
                        .recoveryMaxAttempts(3)
                        .build(),
                createDisposition);
    }

    /** The failure real BigQuery masks a missing table behind, naming the given permission. */
    private static StatusRuntimeException maskedAsPermissionDenied(String permission) {
        return new StatusRuntimeException(
                Status.PERMISSION_DENIED.withDescription(
                        "Permission '"
                                + permission
                                + "' denied on resource"
                                + " 'projects/p/datasets/d/tables/t' (or it may not exist)."));
    }

    @Test
    void flushesEachCommittablesStreamToItsOffset() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamCommitter committer = committer(service);

        committer.commit(
                requests(committable(STREAM, 41, 0), committable(STREAM + "-other", 7, 1)));

        assertThat(service.flushes).hasSize(2);
        assertThat(service.flushes.get(0).streamName).isEqualTo(STREAM);
        assertThat(service.flushes.get(0).offset).isEqualTo(41);
        assertThat(service.flushes.get(1).streamName).isEqualTo(STREAM + "-other");
        assertThat(service.flushes.get(1).offset).isEqualTo(7);
    }

    @Test
    void emptyCommitDoesNothing() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamCommitter committer = committer(service);

        committer.commit(List.of());

        assertThat(service.flushes).isEmpty();
    }

    @Test
    void alreadyExistsMeansAlreadyFlushed() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                (Exception)
                        Exceptions.toStorageException(
                                com.google.rpc.Status.newBuilder()
                                        .setCode(Status.Code.ALREADY_EXISTS.value())
                                        .setMessage("already flushed")
                                        .addDetails(
                                                Any.pack(
                                                        StorageError.newBuilder()
                                                                .setCode(
                                                                        StorageError
                                                                                .StorageErrorCode
                                                                                .OFFSET_ALREADY_EXISTS)
                                                                .setEntity(STREAM)
                                                                .setErrorMessage("already flushed")
                                                                .build()))
                                        .build(),
                                null));
        BufferedStreamCommitter committer = committer(service);

        committer.commit(requests(committable(STREAM, 41, 0)));

        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void bareAlreadyExistsCodeAlsoMeansAlreadyFlushed() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false));
        BufferedStreamCommitter committer = committer(service);

        committer.commit(requests(committable(STREAM, 41, 0)));

        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void transientFlushFailureIsRetriedInPlace() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.UNAVAILABLE), true));
        BufferedStreamCommitter committer = committer(service);

        committer.commit(requests(committable(STREAM, 41, 0)));

        assertThat(service.flushes).hasSize(2);
        assertThat(service.flushes.get(1).offset).isEqualTo(41);
    }

    @Test
    void exhaustedTransientRetryBudgetPropagatesAsIOException() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        for (int i = 0; i < 5; i++) {
            service.flushResults.add(
                    ApiExceptionFactory.createException(
                            null, GrpcStatusCode.of(Status.Code.UNAVAILABLE), true));
        }
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(() -> committer.commit(requests(committable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("retry budget is exhausted");
        assertThat(service.flushes).hasSize(3);
    }

    @Test
    void flushFailurePropagatesAsIOException() {
        // INVALID_ARGUMENT, not PERMISSION_DENIED: the latter is no longer unambiguously terminal
        // here, because the service masks a table that has not propagated behind it.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT), false));
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(() -> committer.commit(requests(committable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush BigQuery stream");
        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void theMaskedPermissionDeniedIsWaitedOutUnderCreateIfNeeded() throws Exception {
        // The propagation window after the writer auto-creates a table reaches FlushRows too, and
        // it masks exactly as CreateWriteStream did. Without this the first commit of a job that
        // created its own table fails the checkpoint — measured once in five runs, 2026-08-08.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(maskedAsPermissionDenied("TABLES_UPDATE_DATA"));
        BufferedStreamCommitter committer = committer(service);

        committer.commit(requests(committable(STREAM, 41, 0)));

        assertThat(service.flushes).hasSize(2);
        assertThat(service.flushes.get(1).offset).isEqualTo(41);
    }

    @Test
    void aNotFoundStaysTerminalEvenUnderCreateIfNeeded() {
        // The committer's verdict is deliberately narrower than the writers': FlushRows names a
        // write stream, and a stream aged out of its seven-day TTL answers permanently. Taking the
        // writers' whole isMissingTable here would spend the recovery budget — serially, per
        // committable — on a failure that cannot succeed.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false));
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(() -> committer.commit(requests(committable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush BigQuery stream");
        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void theMaskedPermissionDeniedIsTerminalUnderCreateNever() {
        // Nothing in a CREATE_NEVER job creates a table, so the same code can only be a genuine
        // denial or a table that disappeared. Failing at once beats spending the budget first.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(maskedAsPermissionDenied("TABLES_UPDATE_DATA"));
        BufferedStreamCommitter committer = committer(service, CreateDisposition.CREATE_NEVER);

        assertThatThrownBy(() -> committer.commit(requests(committable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush BigQuery stream");
        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void anUnendingPropagationWindowStillExhaustsTheBudget() {
        // The allowance is bounded by the same recovery schedule the transient one is: a genuine
        // denial under CREATE_IF_NEEDED surfaces late, never not at all.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        for (int i = 0; i < 5; i++) {
            service.flushResults.add(maskedAsPermissionDenied("TABLES_UPDATE_DATA"));
        }
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(() -> committer.commit(requests(committable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("retry budget is exhausted");
        assertThat(service.flushes).hasSize(3);
    }

    @Test
    void flushedOffsetMismatchIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(40L);
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(() -> committer.commit(requests(committable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("although offset");
    }

    @Test
    void closeClosesTheService() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamCommitter committer = committer(service);
        committer.commit(requests(committable(STREAM, 1, 0)));

        committer.close();

        assertThat(service.closed).isTrue();
    }
}
