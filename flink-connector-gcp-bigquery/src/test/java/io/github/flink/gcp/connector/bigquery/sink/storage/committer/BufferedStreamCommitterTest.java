/*
 * Copyright 2026 laughingman7743
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
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.FakeBufferedStreamService;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferedStreamCommitter}. */
class BufferedStreamCommitterTest {

    private static final String STREAM = "projects/p/datasets/d/tables/t/streams/s1";

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

    private static BufferedStreamCommitter committer(FakeBufferedStreamService service) {
        return new BufferedStreamCommitter(
                service.asFactory(),
                null,
                BufferedStreamOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(1))
                        .recoveryMaxBackoff(Duration.ofMillis(1))
                        .recoveryMaxAttempts(3)
                        .build());
    }

    @Test
    void flushesEachCommittablesStreamToItsOffset() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamCommitter committer = committer(service);

        committer.commit(
                requests(
                        new BufferedStreamCommittable(STREAM, 41, 0),
                        new BufferedStreamCommittable(STREAM + "-other", 7, 1)));

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

        committer.commit(requests(new BufferedStreamCommittable(STREAM, 41, 0)));

        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void bareAlreadyExistsCodeAlsoMeansAlreadyFlushed() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false));
        BufferedStreamCommitter committer = committer(service);

        committer.commit(requests(new BufferedStreamCommittable(STREAM, 41, 0)));

        assertThat(service.flushes).hasSize(1);
    }

    @Test
    void transientFlushFailureIsRetriedInPlace() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.UNAVAILABLE), true));
        BufferedStreamCommitter committer = committer(service);

        committer.commit(requests(new BufferedStreamCommittable(STREAM, 41, 0)));

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

        assertThatThrownBy(
                        () ->
                                committer.commit(
                                        requests(new BufferedStreamCommittable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("retry budget is exhausted");
        assertThat(service.flushes).hasSize(3);
    }

    @Test
    void flushFailurePropagatesAsIOException() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(
                ApiExceptionFactory.createException(
                        null, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false));
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(
                        () ->
                                committer.commit(
                                        requests(new BufferedStreamCommittable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush BigQuery stream");
    }

    @Test
    void flushedOffsetMismatchIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.flushResults.add(40L);
        BufferedStreamCommitter committer = committer(service);

        assertThatThrownBy(
                        () ->
                                committer.commit(
                                        requests(new BufferedStreamCommittable(STREAM, 41, 0))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("although offset");
    }

    @Test
    void closeClosesTheService() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BufferedStreamCommitter committer = committer(service);
        committer.commit(requests(new BufferedStreamCommittable(STREAM, 1, 0)));

        committer.close();

        assertThat(service.closed).isTrue();
    }
}
