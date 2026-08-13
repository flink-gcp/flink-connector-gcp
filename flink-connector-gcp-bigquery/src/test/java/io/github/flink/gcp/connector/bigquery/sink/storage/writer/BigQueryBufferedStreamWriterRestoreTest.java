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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Any;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;

import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.CONTEXT;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.DESTINATION;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.config;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.fastOptions;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.onlyCommittable;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.rowsOf;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.writer;
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
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

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
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        writer.flush(false);

        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(service.openedAppenders).isEmpty();
        assertThat(writer.snapshotState(4))
                .containsExactly(new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 4));
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
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        // The abandoned stream is left open (never finalized): a restored-but-uncommitted
        // committable may still have to flush it, and BigQuery rejects FlushRows on a
        // finalized stream.
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
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

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
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(2);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
        assertThat(service.appends.get(1).offset).isEqualTo(5);
        assertThat(service.createdStreams).isEmpty();
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(5);
    }

    @Test
    void closedWriterDuringProbeIsReopenedOnTheSameRestoredStream() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(FakeBufferedStreamService.failure(writerClosed()));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.openedAppenders).containsExactly(RESTORED_STREAM, RESTORED_STREAM);
        assertThat(service.closedAppenders).containsExactly(RESTORED_STREAM);
        assertThat(service.createdStreams).isEmpty();
        assertThat(service.appends).hasSize(2);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
        assertThat(service.appends.get(1).offset).isEqualTo(5);
        assertThat(onlyCommittable(writer.prepareCommit()).getFlushOffset()).isEqualTo(5);
    }

    @Test
    void terminalProbeFailurePropagates() {
        // INVALID_ARGUMENT, not PERMISSION_DENIED: the latter is a missing-table verdict elsewhere
        // in this module, so it is no longer an unambiguous terminal example anywhere here.
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new StatusRuntimeException(Status.INVALID_ARGUMENT)));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        assertThatThrownBy(
                        () -> {
                            writer.write("a", CONTEXT);
                            writer.flush(false);
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Probing restored BigQuery stream");
    }

    @Test
    void scaleDownRestoreAdoptsTheLatestState() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        String olderStream = "projects/p/datasets/d/tables/t/streams/older";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(DESTINATION, olderStream, 9, 2),
                        new BufferedStreamWriterState(DESTINATION, RESTORED_STREAM, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        // The unadopted sibling's stream is dropped but left open — a restored pending
        // committable of it must stay flushable.
        assertThat(service.appends.get(0).streamName).isEqualTo(RESTORED_STREAM);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
    }

    @Test
    void equalCheckpointIdsUseTheLexicographicallyFirstStreamPerDestination() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        String laterName = DESTINATION.toTablePath() + "/streams/z";
        String firstName = DESTINATION.toTablePath() + "/streams/a";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        config(),
                        fastOptions(3),
                        service,
                        BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                        new BufferedStreamWriterState(DESTINATION, laterName, 9, 3),
                        new BufferedStreamWriterState(DESTINATION, firstName, 5, 3));

        writer.write("a", CONTEXT);
        writer.flush(false);

        assertThat(service.openedAppenders).containsExactly(firstName);
        assertThat(service.appends.get(0).offset).isEqualTo(5);
    }

    /** The SDK's constructor is protected in a final class; tests synthesize via reflection. */
    private static Exceptions.StreamWriterClosedException writerClosed() throws Exception {
        Constructor<Exceptions.StreamWriterClosedException> constructor =
                Exceptions.StreamWriterClosedException.class.getDeclaredConstructor(
                        Status.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(Status.FAILED_PRECONDITION, "stream", "writer-id");
    }
}
