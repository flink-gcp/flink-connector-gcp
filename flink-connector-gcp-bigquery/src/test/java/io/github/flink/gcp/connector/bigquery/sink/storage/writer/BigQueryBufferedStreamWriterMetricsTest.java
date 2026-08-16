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

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.CONTEXT;
import static io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriterTest.DESTINATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the metrics {@link BigQueryBufferedStreamWriter} registers, against the same fake
 * Storage Write API service its behavioural tests use.
 *
 * <p>Every assertion goes through the name a metric registered under, so renaming one — or failing
 * to register it — fails here.
 */
class BigQueryBufferedStreamWriterMetricsTest {

    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final FakeBufferedStreamService service = new FakeBufferedStreamService();
    private final BigQueryBufferedStreamWriterTest.RecordingHandler handler =
            new BigQueryBufferedStreamWriterTest.RecordingHandler();

    @Test
    void countsEveryRowHandedToTheClientWithItsPayloadBytes() throws Exception {
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.write("bbb", CONTEXT);
        writer.flush(false);

        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(counter("numBytesSend")).isEqualTo(5);
        assertThat(counter("numRecordsSendErrors")).isZero();
        // A clean run registers no error class at all — not even UNCLASSIFIED, which is what an
        // appendFailed call above the success early-return would produce on every acknowledgement.
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    @Test
    void countsAResentBatchOnlyOnceAndNamesTheStatusItFailedWith() throws Exception {
        // The resend at the same offset is a retry of rows already counted, so it advances
        // appendRetries rather than numRecordsSend. A mutant counting the send inside syncAppend
        // unconditionally dies here.
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE))));
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(2);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numBytesSend")).isEqualTo(2);
        assertThat(counter("appendRetries")).isEqualTo(1);
        assertThat(errors("UNAVAILABLE")).isEqualTo(1);
    }

    @Test
    void countsARecordTheSerializerRejectedAsASendError() throws Exception {
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("poison", CONTEXT);

        assertThat(handler.rows).hasSize(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void skipsRecordsTheSerializerReturnsNullFor() throws Exception {
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("skip-me", CONTEXT);
        writer.write("aa", CONTEXT);
        writer.flush(false);

        // Skipped, not failed: appended nowhere, and never offered to the handler.
        assertThat(service.appends).hasSize(1);
        assertThat(handler.rows).isEmpty();
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("numRecordsSendErrors")).isZero();
        assertThat(counter("recordsSkipped")).isEqualTo(1);
    }

    @Test
    void createsNoStreamForACheckpointInWhichEveryRecordWasSkipped() throws Exception {
        // The claim the writer's own comment makes: a skip costs no per-destination state. With a
        // second record in the run the stream is created anyway, so only an all-skipped one shows
        // it — and an empty checkpoint must still produce no committable to flush.
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("skip-me", CONTEXT);
        writer.flush(false);

        assertThat(service.createdStreams).isEmpty();
        assertThat(service.appends).isEmpty();
        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(counter("recordsSkipped")).isEqualTo(1);
    }

    @Test
    void countsTheRowsTheServiceRejectedByIndexAsSendErrors() throws Exception {
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new Exceptions.AppendSerializtionError(
                                Status.Code.INVALID_ARGUMENT.value(),
                                "bad rows",
                                "stream",
                                Map.of(0, "bad row"))));
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(handler.rows).hasSize(1);
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        // Both rows reached the client; only the survivor was replayed, and that replay is a
        // retry rather than a second send.
        assertThat(counter("numRecordsSend")).isEqualTo(2);
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(counter("appendRetries")).isEqualTo(1);
        // The replay that carried the survivors succeeded, and a success is not an error class.
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    @Test
    void countsTheOffsetProbeOfARestoredStreamAsASend() throws Exception {
        BigQueryBufferedStreamWriter<String> writer =
                writer(new BufferedStreamWriterState(DESTINATION, "p/d/t/streams/restored", 7, 1));

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(service.appends).hasSize(1);
        assertThat(counter("numRecordsSend")).isEqualTo(1);
        assertThat(counter("appendRetries")).isZero();
    }

    @Test
    void gaugeReportsTheWritersInFlightAppends() throws Exception {
        // A one-byte batching cap sends each record's batch as the next record arrives, so the
        // pipelined append is issued without a flush and the test stays on one thread.
        service.appendResults.add(SettableApiFuture.create());
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        BufferedStreamOptions.builder()
                                .maxAppendRequestBytes(1)
                                .recoveryMaxAttempts(2)
                                .build());

        assertThat(this.<Integer>gauge("inFlightAppends")).isZero();

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);

        assertThat(this.<Integer>gauge("inFlightAppends")).isEqualTo(1);
    }

    @Test
    void registersNoPerDestinationCountersAtAll() throws Exception {
        // This write method takes a fixed destination, so the opt-in has no meaning here and the
        // subgroup must never appear.
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);
        writer.flush(false);

        assertThat(metrics.hasMetric("destination", "p.d.t", "recordsSend")).isFalse();
    }

    @Test
    void doesNotCountTheAppendsCascadingBehindARejectedOffset() throws Exception {
        // A row-level rejection strands every append issued behind it: those failures are the
        // rejection's consequence, not statuses of their own, so counting them would multiply one
        // incident by the depth of the pipeline (the Pub/Sub sink skips cascade cancellations for
        // the same reason).
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new Exceptions.AppendSerializtionError(
                                Status.Code.INVALID_ARGUMENT.value(),
                                "bad rows",
                                "stream",
                                Map.of(0, "bad row"))));
        service.appendResults.add(
                FakeBufferedStreamService.failure(
                        new StatusRuntimeException(
                                Status.fromCode(Status.Code.OUT_OF_RANGE)
                                        .withDescription("offset out of range"))));
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        BufferedStreamOptions.builder()
                                .maxAppendRequestBytes(1)
                                .recoveryInitialBackoff(Duration.ofMillis(1))
                                .recoveryMaxBackoff(Duration.ofMillis(1))
                                .recoveryMaxAttempts(3)
                                .build());

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        writer.flush(false);

        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(metrics.hasMetric("errorClass", "OUT_OF_RANGE", "errors")).isFalse();
    }

    @Test
    void countsNothingAsSentWhenTheStreamCannotBeOpened() throws Exception {
        service.openAppenderFailures.add(new IOException("cannot open"));
        BigQueryBufferedStreamWriter<String> writer = writer();

        writer.write("aa", CONTEXT);

        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(IOException.class);
        assertThat(counter("numRecordsSend")).isZero();
    }

    @Test
    void theInFlightGaugeIsClearedWhenTheWriterIsClosed() throws Exception {
        service.appendResults.add(SettableApiFuture.create());
        BigQueryBufferedStreamWriter<String> writer =
                writer(
                        BufferedStreamOptions.builder()
                                .maxAppendRequestBytes(1)
                                .recoveryMaxAttempts(2)
                                .build());

        writer.write("aa", CONTEXT);
        writer.write("bb", CONTEXT);
        assertThat(this.<Integer>gauge("inFlightAppends")).isEqualTo(1);

        writer.close();

        assertThat(this.<Integer>gauge("inFlightAppends")).isZero();
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private long counter(String... identifier) {
        return metrics.counterValue(identifier);
    }

    private long errors(String errorClass) {
        return counter("errorClass", errorClass, "errors");
    }

    private <T> T gauge(String name) {
        return metrics.gaugeValue(name);
    }

    private BigQueryBufferedStreamWriter<String> writer(BufferedStreamWriterState... restored) {
        return writer(BigQueryBufferedStreamWriterTest.fastOptions(3), restored);
    }

    private BigQueryBufferedStreamWriter<String> writer(
            BufferedStreamOptions options, BufferedStreamWriterState... restored) {
        BigQuerySinkConfig<String> config =
                BigQueryBufferedStreamWriterTest.config(
                        new BigQueryBufferedStreamWriterTest.StringSerializer(), handler, null);
        return new BigQueryBufferedStreamWriter<>(
                config,
                options,
                service.asFactory(),
                BigQueryDefaultStreamWriterTest.NOOP_ADMIN,
                metrics,
                0,
                restored.length == 0 ? Collections.emptyList() : List.of(restored));
    }
}
