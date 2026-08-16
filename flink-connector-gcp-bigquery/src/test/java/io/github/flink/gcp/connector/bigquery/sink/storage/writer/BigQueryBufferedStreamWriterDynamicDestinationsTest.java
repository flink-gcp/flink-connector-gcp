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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.CONTEXT;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.DESTINATION;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.rowsOf;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests destination isolation and cleanup in the exactly-once writer. */
class BigQueryBufferedStreamWriterDynamicDestinationsTest {

    private static final TableDestination OTHER_DESTINATION =
            TableDestination.of("p", "d", "other");

    @Test
    void keepsIndependentStreamsOffsetsStateAndCommittables() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        BigQueryBufferedStreamWriter<String> writer = writer(service);

        writer.write("a-1", CONTEXT);
        writer.write("b-1", CONTEXT);
        writer.write("a-2", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams)
                .containsExactly(
                        DESTINATION.toTablePath() + "/streams/fake-0",
                        OTHER_DESTINATION.toTablePath() + "/streams/fake-1");
        assertThat(service.appends).hasSize(2);
        assertThat(rowsOf(service.appends.get(0).rows)).containsExactly("a-1", "a-2");
        assertThat(service.appends.get(0).offset).isZero();
        assertThat(rowsOf(service.appends.get(1).rows)).containsExactly("b-1");
        assertThat(service.appends.get(1).offset).isZero();

        assertThat(writer.prepareCommit())
                .extracting(
                        BufferedStreamCommittable::getStreamName,
                        BufferedStreamCommittable::getFlushOffset)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                DESTINATION.toTablePath() + "/streams/fake-0", 1L),
                        org.assertj.core.groups.Tuple.tuple(
                                OTHER_DESTINATION.toTablePath() + "/streams/fake-1", 0L));
        assertThat(writer.snapshotState(7))
                .extracting(
                        BufferedStreamWriterState::getDestination,
                        BufferedStreamWriterState::getNextOffset)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(DESTINATION, 2L),
                        org.assertj.core.groups.Tuple.tuple(OTHER_DESTINATION, 1L));
    }

    @Test
    void restoresAndProbesOneStreamPerDestination() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        String first = DESTINATION.toTablePath() + "/streams/first";
        String second = OTHER_DESTINATION.toTablePath() + "/streams/second";
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        service,
                        new BufferedStreamWriterState(DESTINATION, first, 4, 3),
                        new BufferedStreamWriterState(OTHER_DESTINATION, second, 9, 3));

        writer.write("a-next", CONTEXT);
        writer.write("b-next", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).isEmpty();
        assertThat(service.openedAppenders).containsExactly(first, second);
        assertThat(service.appends).extracting(call -> call.offset).containsExactly(4L, 9L);
    }

    @Test
    void aClosedAppenderIsReopenedOnlyForItsDestination() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        service.appendResults.add(FakeBufferedStreamService.failure(writerClosed()));
        service.appendResults.add(FakeBufferedStreamService.success(0));
        service.appendResults.add(FakeBufferedStreamService.success(0));
        BigQueryBufferedStreamWriter<String> writer = writer(service);

        writer.write("a-1", CONTEXT);
        writer.write("b-1", CONTEXT);
        writer.flush(false);

        String first = service.createdStreams.get(0);
        String second = service.createdStreams.get(1);
        assertThat(service.openedAppenders).containsExactly(first, second, first);
        assertThat(service.closedAppenders).containsExactly(first);
        assertThat(service.appends)
                .extracting(call -> call.streamName)
                .containsExactly(first, second, first);
    }

    @Test
    void evictsOnlyCleanDestinationsStrictlyPastTheIdleTimeout() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        AtomicLong now = new AtomicLong();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .destinationIdleTimeout(Duration.ofNanos(10))
                        .build();
        BigQueryBufferedStreamWriter<String> writer = writer(service, options, now);

        writer.write("a-1", CONTEXT);
        writer.flush(false);
        String oldStream = service.createdStreams.get(0);

        now.set(20);
        writer.flush(false);
        assertThat(service.closedAppenders).isEmpty();
        assertThat(writer.snapshotState(1)).hasSize(1);

        writer.write("a-2", CONTEXT);
        writer.flush(false);
        writer.snapshotState(2);

        now.set(30);
        writer.flush(false);
        assertThat(service.closedAppenders).isEmpty();

        now.set(31);
        writer.flush(false);
        assertThat(service.closedAppenders).containsExactly(oldStream);
        assertThat(writer.snapshotState(3)).isEmpty();

        writer.write("a-3", CONTEXT);
        writer.flush(false);
        assertThat(service.createdStreams).hasSize(2);
        assertThat(service.createdStreams.get(1)).isNotEqualTo(oldStream);
    }

    @Test
    void endOfInputDoesNotEvictAnIdleDestination() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        AtomicLong now = new AtomicLong();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .destinationIdleTimeout(Duration.ofNanos(10))
                        .build();
        BigQueryBufferedStreamWriter<String> writer = writer(service, options, now);

        writer.write("a-1", CONTEXT);
        writer.flush(false);
        writer.snapshotState(1);
        now.set(11);
        writer.flush(true);

        assertThat(service.closedAppenders).isEmpty();
        assertThat(writer.snapshotState(2)).hasSize(1);
    }

    @Test
    void appenderCloseFailureDoesNotFailIdleEviction() throws Exception {
        FakeBufferedStreamService service = new FakeBufferedStreamService();
        AtomicLong now = new AtomicLong();
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .destinationIdleTimeout(Duration.ofNanos(10))
                        .build();
        BigQueryBufferedStreamWriter<String> writer = writer(service, options, now);

        writer.write("a-1", CONTEXT);
        writer.flush(false);
        writer.snapshotState(1);
        service.appenderCloseFailure = new IllegalStateException("close failed");
        now.set(11);

        writer.flush(false);

        assertThat(writer.snapshotState(2)).isEmpty();
    }

    private static BigQueryBufferedStreamWriter<String> writer(
            FakeBufferedStreamService service, BufferedStreamWriterState... restored) {
        return writer(service, BufferedStreamOptions.builder().build(), new AtomicLong(), restored);
    }

    private static BigQueryBufferedStreamWriter<String> writer(
            FakeBufferedStreamService service,
            BufferedStreamOptions options,
            AtomicLong now,
            BufferedStreamWriterState... restored) {
        return new BigQueryBufferedStreamWriter<>(
                dynamicConfig(),
                options,
                service.asFactory(),
                BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                TestSinkWriterMetricGroup.create(),
                0,
                List.of(restored),
                now::get);
    }

    @SuppressWarnings("unchecked")
    private static BigQuerySinkConfig<String> dynamicConfig() {
        return ((BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                element.startsWith("a")
                                                        ? DESTINATION
                                                        : OTHER_DESTINATION)
                                .serializer(new BigQueryBufferedStreamWriterTest.StringSerializer())
                                .build())
                .getConfig();
    }

    private static Exceptions.StreamWriterClosedException writerClosed() throws Exception {
        Constructor<Exceptions.StreamWriterClosedException> constructor =
                Exceptions.StreamWriterClosedException.class.getDeclaredConstructor(
                        Status.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(Status.FAILED_PRECONDITION, "stream", "writer-id");
    }
}
