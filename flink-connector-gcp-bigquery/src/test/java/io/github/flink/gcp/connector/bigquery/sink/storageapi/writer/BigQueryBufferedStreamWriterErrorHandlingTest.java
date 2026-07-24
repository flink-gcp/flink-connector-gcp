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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Any;
import io.github.flink.gcp.connector.bigquery.sink.storageapi.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storageapi.BufferedStreamOptions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.CONTEXT;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.RecordingHandler;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.StringSerializer;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.config;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.fastOptions;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.onlyCommittable;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.rowsOf;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.writer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the buffered-stream writer's append failure handling. */
class BigQueryBufferedStreamWriterErrorHandlingTest {

    private static Exceptions.AppendSerializtionError rowLevelError(Map<Integer, String> rows) {
        return new Exceptions.AppendSerializtionError(
                Status.Code.INVALID_ARGUMENT.value(), "bad rows", "stream", rows);
    }

    private static Throwable storageError(StorageError.StorageErrorCode code, Status.Code grpc) {
        return Exceptions.toStorageException(
                com.google.rpc.Status.newBuilder()
                        .setCode(grpc.value())
                        .setMessage("synthesized " + code)
                        .addDetails(
                                Any.pack(
                                        StorageError.newBuilder()
                                                .setCode(code)
                                                .setEntity("stream")
                                                .setErrorMessage("synthesized " + code)
                                                .build()))
                        .build(),
                null);
    }

    @Test
    void transientFailureIsReappendedAtTheSameOffset() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(new StatusRuntimeException(Status.UNAVAILABLE)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(2);
        assertThat(service.appends.get(0).offset).isEqualTo(0);
        assertThat(service.appends.get(1).offset).isEqualTo(0);
        assertThat(rowsOf(service.appends.get(1).rows)).containsExactly("a", "b");
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(1);
    }

    @Test
    void offsetAlreadyExistsOnResendMeansTheOriginalLanded() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new StatusRuntimeException(Status.DEADLINE_EXCEEDED)));
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS,
                                Status.Code.ALREADY_EXISTS)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(2);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(0);
    }

    @Test
    void exhaustedRetryBudgetIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        for (int i = 0; i < 10; i++) {
            service.appendResults.add(
                    FakeBufferedStreamService.failure(
                            new StatusRuntimeException(Status.UNAVAILABLE)));
        }
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(2),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("retry budget is exhausted");
    }

    @Test
    void rowLevelRejectionRoutesFailingRowsAndReplaysWithRecomputedOffsets() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        // Batch A (a0, a1, a2) is rejected with a row-level error on a1; batch B (b0), already
        // appended behind it, fails the offset cascade.
        service.appendResults.add(
                FakeBufferedStreamService.failure(rowLevelError(Map.of(1, "bad row"))));
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.OFFSET_OUT_OF_RANGE,
                                Status.Code.OUT_OF_RANGE)));
        RecordingHandler handler = new RecordingHandler();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .maxAppendRequestBytes(6)
                        .retryInitialBackoff(Duration.ofMillis(1))
                        .retryMaxBackoff(Duration.ofMillis(1))
                        .retryMaxAttempts(3)
                        .build();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        options,
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        writer.write("a0", CONTEXT);
        writer.write("a1", CONTEXT);
        writer.write("a2", CONTEXT);
        writer.write("b0", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(handler.rows.get(0).getRowBytes().toStringUtf8()).isEqualTo("a1");

        // Original appends: A at 0, B at 3. Replay: survivors (a0, a2) at 0, then B at 2.
        assertThat(service.appends).hasSize(4);
        assertThat(service.appends.get(0).offset).isEqualTo(0);
        assertThat(service.appends.get(1).offset).isEqualTo(3);
        assertThat(service.appends.get(2).offset).isEqualTo(0);
        assertThat(rowsOf(service.appends.get(2).rows)).containsExactly("a0", "a2");
        assertThat(service.appends.get(3).offset).isEqualTo(2);
        assertThat(rowsOf(service.appends.get(3).rows)).containsExactly("b0");

        BufferedStreamCommittable committable = onlyCommittable(writer.prepareCommit());
        assertThat(committable.getFlushOffset()).isEqualTo(2);
    }

    @Test
    void laterAcknowledgedAppendAfterARejectionIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(rowLevelError(Map.of(0, "bad row"))));
        service.appendResults.add(FakeBufferedStreamService.success(3));
        RecordingHandler handler = new RecordingHandler();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .maxAppendRequestBytes(6)
                        .retryInitialBackoff(Duration.ofMillis(1))
                        .retryMaxBackoff(Duration.ofMillis(1))
                        .retryMaxAttempts(3)
                        .build();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        options,
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a0", CONTEXT);
                            writer.write("a1", CONTEXT);
                            writer.write("a2", CONTEXT);
                            writer.write("b0", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("inconsistent");
    }

    @Test
    void offsetAlreadyExistsDuringAReplayIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(rowLevelError(Map.of(0, "bad row"))));
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS,
                                Status.Code.ALREADY_EXISTS)));
        RecordingHandler handler = new RecordingHandler();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(new StringSerializer(), handler, null),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a0", CONTEXT);
                            writer.write("a1", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Replaying an append");
    }

    @Test
    void midRunStreamFinalizedIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.STREAM_FINALIZED,
                                Status.Code.INVALID_ARGUMENT)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed");
    }

    @Test
    void schemaMismatchIsTerminalWithGuidance() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.SCHEMA_MISMATCH_EXTRA_FIELDS,
                                Status.Code.INVALID_ARGUMENT)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("schema evolution is not supported");
    }

    @Test
    void acknowledgedOffsetMismatchIsTerminal() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(FakeBufferedStreamService.success(41));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN);

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("although offset");
    }
}
