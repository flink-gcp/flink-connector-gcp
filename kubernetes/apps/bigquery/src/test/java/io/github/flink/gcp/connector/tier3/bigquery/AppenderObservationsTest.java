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

package io.github.flink.gcp.connector.tier3.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamService;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.OffsetRowAppender;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.RowAppender;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppenderObservationsTest {
    private static final TableDestination TABLE = TableDestination.of("p", "d", "t");
    private static final ProtoRows ROWS =
            ProtoRows.newBuilder()
                    .addSerializedRows(ByteString.copyFromUtf8("payload-must-not-be-logged"))
                    .addSerializedRows(ByteString.copyFromUtf8("second"))
                    .build();

    @Test
    void observesDefaultCallsAndReturnsTheOriginalFutureWithoutWaiting() throws Exception {
        List<AppenderObservations.Event> events = new ArrayList<>();
        AtomicLong clock = new AtomicLong();
        var observations = new AppenderObservations("writer=one", clock::get, events::add);
        var raw = new FakeAppender();
        raw.onAppend = () -> clock.addAndGet(7);
        var appender =
                observations
                        .observe(
                                (destination, descriptor, location) -> {
                                    assertThat(destination).isSameAs(TABLE);
                                    assertThat(descriptor).isSameAs(Empty.getDescriptor());
                                    assertThat(location).isEqualTo("US");
                                    clock.addAndGet(5);
                                    return raw;
                                })
                        .create(TABLE, Empty.getDescriptor(), "US");
        assertThat(appender.append(ROWS)).isSameAs(raw.future);
        assertThat(raw.rows).isSameAs(ROWS);
        assertThat(raw.future.isDone()).isFalse();
        assertThat(events)
                .extracting(AppenderObservations.Event::operation)
                .containsExactly("writer", "open", "append");
        var sent = events.get(2);
        assertThat(sent.rows()).isEqualTo(2);
        assertThat(sent.protoRowsBytes()).isEqualTo(ROWS.getSerializedSize());
        assertThat(sent.offset()).isEqualTo(-1);
        assertThat(sent.callNanos()).isEqualTo(7);
        assertThat(sent.elapsedNanos()).isEqualTo(12);
        assertThat(sent.openAppenders()).isEqualTo(1);
        assertThat(sent.stream()).isEqualTo(TABLE.toTablePath() + "/streams/_default");
        assertThat(sent.format()).doesNotContain("payload-must-not-be-logged");
        assertThat(raw.future.cancel(true)).isTrue();
        appender.close();
        assertThat(raw.closes).isEqualTo(1);
        assertThat(events.get(3).openAppenders()).isZero();
    }

    @Test
    void forwardsBufferedStreamOperationsAndOffsetsWithoutObservingCommitVisibility()
            throws Exception {
        List<AppenderObservations.Event> events = new ArrayList<>();
        var raw = new FakeService();
        var observed = new AppenderObservations("writer=two", () -> 0, events::add).observe(raw);
        assertThat(observed.createBufferedStream(TABLE)).isEqualTo("buffered-stream");
        assertThat(raw.destination).isSameAs(TABLE);
        var appender = observed.openAppender("buffered-stream", Empty.getDescriptor());
        assertThat(raw.stream).isEqualTo("buffered-stream");
        assertThat(raw.descriptor).isSameAs(Empty.getDescriptor());
        var future = appender.append(ROWS, 123);
        assertThat(future).isSameAs(raw.appender.future);
        assertThat(raw.appender.rows).isSameAs(ROWS);
        assertThat(raw.appender.offset).isEqualTo(123);
        assertThat(events.get(2).offset()).isEqualTo(123);
        var asynchronous = new IOException("async failure");
        raw.appender.future.setException(asynchronous);
        assertThatThrownBy(future::get).hasCause(asynchronous);
        assertThat(events).hasSize(3);
        assertThat(observed.flushRows("buffered-stream", 124)).isEqualTo(125);
        assertThat(raw.offset).isEqualTo(124);
        appender.close();
        observed.close();
        assertThat(raw.appender.closes).isEqualTo(1);
        assertThat(raw.closes).isEqualTo(1);
        assertThat(events)
                .extracting(AppenderObservations.Event::operation)
                .containsExactly("writer", "open", "append", "close");
    }

    @Test
    void recordsFailedOpenWithoutAnActiveHandleAndSeparatesReopenedHandles() throws Exception {
        List<AppenderObservations.Event> events = new ArrayList<>();
        var observations = new AppenderObservations("writer=three", () -> 0, events::add);
        var failure = new IOException("private-error-text");
        assertThatThrownBy(
                        () ->
                                observations
                                        .observe(
                                                (table, descriptor, location) -> {
                                                    throw failure;
                                                })
                                        .create(TABLE, Empty.getDescriptor(), null))
                .isSameAs(failure);
        assertThat(events.get(1).openAppenders()).isZero();
        assertThat(events.get(1).outcome()).isEqualTo("threw");
        assertThat(events.get(1).format()).doesNotContain("private-error-text");
        var first =
                observations
                        .observe((table, descriptor, location) -> new FakeAppender())
                        .create(TABLE, Empty.getDescriptor(), null);
        first.close();
        var second =
                observations
                        .observe((table, descriptor, location) -> new FakeAppender())
                        .create(TABLE, Empty.getDescriptor(), null);
        second.close();
        assertThat(events)
                .filteredOn(e -> e.operation().equals("open"))
                .extracting(AppenderObservations.Event::appender)
                .containsExactly(1L, 2L, 3L);
        assertThat(events.get(events.size() - 1).openAppenders()).isZero();
    }

    @Test
    void preservesSynchronousAppendAndCloseFailuresAndDoesNotDoubleRetire() throws Exception {
        List<AppenderObservations.Event> events = new ArrayList<>();
        var raw = new FakeAppender();
        raw.appendFailure = new IllegalStateException("append rejected");
        raw.closeFailure = new IllegalStateException("close uncertain");
        var appender =
                new AppenderObservations("writer=four", () -> 0, events::add)
                        .observe((table, descriptor, location) -> raw)
                        .create(TABLE, Empty.getDescriptor(), null);
        assertThatThrownBy(() -> appender.append(ROWS)).isSameAs(raw.appendFailure);
        assertThat(events.get(2).outcome()).isEqualTo("threw");
        assertThat(events.get(2).rows()).isEqualTo(2);
        assertThatThrownBy(appender::close).isSameAs(raw.closeFailure);
        assertThat(events.get(3).openAppenders()).isEqualTo(1);
        raw.closeFailure = null;
        appender.close();
        appender.close();
        assertThat(raw.closes).isEqualTo(3);
        assertThat(events.get(4).openAppenders()).isZero();
        assertThat(events.get(5).openAppenders()).isZero();
    }

    @Test
    void observerFailureLeavesTheSendOutcomeUnchangedAndReportsTheGap() throws Exception {
        List<AppenderObservations.Event> events = new ArrayList<>();
        var raw = new FakeAppender();
        var observations =
                new AppenderObservations(
                        "writer=five",
                        () -> 0,
                        event -> {
                            if (event.operation().equals("append")) {
                                throw new IllegalStateException("log unavailable");
                            }
                            events.add(event);
                        });
        var appender =
                observations
                        .observe((table, descriptor, location) -> raw)
                        .create(TABLE, Empty.getDescriptor(), null);
        assertThat(appender.append(ROWS)).isSameAs(raw.future);
        raw.appendFailure = new IllegalArgumentException("delegate rejected");
        assertThatThrownBy(() -> appender.append(ROWS)).isSameAs(raw.appendFailure);
        appender.close();
        assertThat(events)
                .extracting(AppenderObservations.Event::sequence)
                .containsExactly(1L, 2L, 5L);
        assertThat(events.get(2).lostEvents()).isEqualTo(2);
    }

    @Test
    void bufferedOpenAndAppendFailuresKeepTheirExactExceptions() throws Exception {
        List<AppenderObservations.Event> events = new ArrayList<>();
        var raw = new FakeService();
        raw.openFailure = new IOException("cannot open");
        var observed = new AppenderObservations("writer=six", () -> 0, events::add).observe(raw);
        assertThatThrownBy(() -> observed.openAppender("buffered-stream", Empty.getDescriptor()))
                .isSameAs(raw.openFailure);
        assertThat(events.get(1).openAppenders()).isZero();
        raw.openFailure = null;
        var appender = observed.openAppender("buffered-stream", Empty.getDescriptor());
        raw.appender.appendFailure = new IllegalStateException("cannot append");
        assertThatThrownBy(() -> appender.append(ROWS, 4)).isSameAs(raw.appender.appendFailure);
        assertThat(events.get(3).offset()).isEqualTo(4);
        appender.close();
        observed.close();
    }

    private static final class FakeAppender implements RowAppender, OffsetRowAppender {
        final SettableApiFuture<AppendRowsResponse> future = SettableApiFuture.create();
        ProtoRows rows;
        long offset;
        int closes;
        RuntimeException appendFailure;
        RuntimeException closeFailure;
        Runnable onAppend = () -> {};

        @Override
        public ApiFuture<AppendRowsResponse> append(ProtoRows value) {
            return append(value, -1);
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(ProtoRows value, long suppliedOffset) {
            rows = value;
            offset = suppliedOffset;
            onAppend.run();
            if (appendFailure != null) {
                throw appendFailure;
            }
            return future;
        }

        @Override
        public void close() {
            closes++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class FakeService implements BufferedStreamService {
        final FakeAppender appender = new FakeAppender();
        TableDestination destination;
        String stream;
        Descriptors.Descriptor descriptor;
        long offset;
        int closes;
        IOException openFailure;

        @Override
        public String createBufferedStream(TableDestination value) {
            destination = value;
            return "buffered-stream";
        }

        @Override
        public OffsetRowAppender openAppender(String value, Descriptors.Descriptor schema)
                throws IOException {
            if (openFailure != null) {
                throw openFailure;
            }
            stream = value;
            descriptor = schema;
            return appender;
        }

        @Override
        public long flushRows(String value, long suppliedOffset) {
            stream = value;
            offset = suppliedOffset;
            return suppliedOffset + 1;
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
