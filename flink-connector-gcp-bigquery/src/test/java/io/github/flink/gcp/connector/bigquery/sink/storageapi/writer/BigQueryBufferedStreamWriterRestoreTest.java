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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.CONTEXT;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.config;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.fastOptions;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.onlyCommittable;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.rowsOf;
import static io.github.flink.gcp.connector.bigquery.sink.storageapi.writer.BigQueryBufferedStreamWriterTest.writer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the restore probe and stream-abandonment protocol. */
class BigQueryBufferedStreamWriterRestoreTest {

    private static final String RESTORED_STREAM = "projects/p/datasets/d/tables/t/streams/old";

    private static Throwable storageError(StorageError.StorageErrorCode code, Status.Code grpc) {
        return Exceptions.toStorageException(
                com.google.rpc.Status.newBuilder()
                        .setCode(grpc.value())
                        .setMessage("synthesized " + code)
                        .addDetails(
                                Any.pack(
                                        StorageError.newBuilder()
                                                .setCode(code)
                                                .setEntity(RESTORED_STREAM)
                                                .setErrorMessage("synthesized " + code)
                                                .build()))
                        .build(),
                null);
    }

    @Test
    void probeSuccessReusesTheRestoredStream() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.write("b", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).isEmpty();
        assertThat(service.openedAppenders).containsExactly(RESTORED_STREAM);
        assertThat(service.appends).hasSize(1);
        assertThat(service.appends.get(0).streamName).isEqualTo(RESTORED_STREAM);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
        assertThat(rowsOf(service.appends.get(0).rows)).containsExactly("a", "b");

        BufferedStreamCommittable committable = onlyCommittable(writer.prepareCommit());
        assertThat(committable.getStreamName()).isEqualTo(RESTORED_STREAM);
        assertThat(committable.getFlushOffset()).isEqualTo(6);
        assertThat(service.finalizedStreams).isEmpty();

        // Later appends are pipelined on the adopted stream.
        writer.write("c", CONTEXT);
        writer.flush(false);
        assertThat(service.appends.get(1).offset).isEqualTo(7);
    }

    @Test
    void quietRestoreEmitsNothing() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.flush(false);

        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(service.openedAppenders).isEmpty();
        assertThat(writer.snapshotState(4))
                .containsExactly(new BufferedStreamWriterState(RESTORED_STREAM, 5, 4));
    }

    @Test
    void offsetAlreadyExistsAbandonsTheStream() throws Exception {
        assertAbandonOn(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS,
                                Status.Code.ALREADY_EXISTS)));
    }

    @Test
    void offsetOutOfRangeAbandonsTheStream() throws Exception {
        assertAbandonOn(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.OFFSET_OUT_OF_RANGE,
                                Status.Code.OUT_OF_RANGE)));
    }

    @Test
    void finalizedStreamAbandonsTheStream() throws Exception {
        assertAbandonOn(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.STREAM_FINALIZED,
                                Status.Code.INVALID_ARGUMENT)));
    }

    @Test
    void unknownStreamAbandonsTheStream() throws Exception {
        assertAbandonOn(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.STREAM_NOT_FOUND,
                                Status.Code.NOT_FOUND)));
    }

    private void assertAbandonOn(
            com.google.api.core.ApiFuture<com.google.cloud.bigquery.storage.v1.AppendRowsResponse>
                    probeResult)
            throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(probeResult);
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.finalizedStreams).containsExactly(RESTORED_STREAM);
        assertThat(service.createdStreams).hasSize(1);
        String fresh = service.createdStreams.get(0);
        // The probe batch is replayed onto the fresh stream from offset zero.
        FakeBufferedStreamService.AppendCall replay =
                service.appends.get(service.appends.size() - 1);
        assertThat(replay.streamName).isEqualTo(fresh);
        assertThat(replay.offset).isEqualTo(0);
        assertThat(rowsOf(replay.rows)).containsExactly("a");

        BufferedStreamCommittable committable = onlyCommittable(writer.prepareCommit());
        assertThat(committable.getStreamName()).isEqualTo(fresh);
        assertThat(committable.getFlushOffset()).isEqualTo(0);
    }

    @Test
    void appenderOpenFailureAbandonsTheStream() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.openAppenderFailures.add(new IOException("stream is gone"));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.finalizedStreams).containsExactly(RESTORED_STREAM);
        assertThat(service.createdStreams).hasSize(1);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(0);
    }

    @Test
    void transientProbeFailureIsRetriedOnTheSameStream() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(new StatusRuntimeException(Status.UNAVAILABLE)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(2);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
        assertThat(service.appends.get(1).offset).isEqualTo(5);
        assertThat(service.createdStreams).isEmpty();
        assertThat(service.finalizedStreams).isEmpty();
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(5);
    }

    @Test
    void terminalProbeFailurePropagates() {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new StatusRuntimeException(Status.PERMISSION_DENIED)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Probing restored BigQuery stream");
        assertThat(service.finalizedStreams).isEmpty();
    }

    @Test
    void finalizeFailureDuringAbandonmentIsBestEffort() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        storageError(
                                StorageError.StorageErrorCode.STREAM_FINALIZED,
                                Status.Code.INVALID_ARGUMENT)));
        service.finalizeFailures.add(new IOException("finalize hiccup"));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.finalizedStreams).isEmpty();
        assertThat(service.createdStreams).hasSize(1);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(0);
    }

    @Test
    void scaleDownRestoreAdoptsTheLatestStateAndFinalizesTheRest() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        String olderStream = "projects/p/datasets/d/tables/t/streams/older";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(olderStream, 9, 2),
                        new BufferedStreamWriterState(RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.finalizedStreams).containsExactly(olderStream);
        assertThat(service.appends.get(0).streamName).isEqualTo(RESTORED_STREAM);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
    }
}
