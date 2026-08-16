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

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.grpc.Status;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Canary for the goccy emulator's missing-table deviation: a Storage Write API RPC naming a table
 * that is not there answers {@code UNKNOWN}, a status the connector's missing-table verdict cannot
 * act on — {@link AppendErrorClassifier#isMissingTable} keys on {@code NOT_FOUND} and the masked
 * {@code PERMISSION_DENIED} the real service answers (goccy/bigquery-emulator#504; #326). The
 * deviation is what {@link StreamWriterRowAppenderFactory}'s emulator branch rewrites on the
 * default-stream path, and what keeps buffered {@code CREATE_IF_NEEDED} auto-creation out of reach
 * of emulator tests — {@link BigQueryBufferedStreamMissingTableITCase} covers it on the real
 * service instead.
 *
 * <p>These tests pin the deviation so its upstream fix is detected rather than waited for: the
 * emulator-image bump that delivers it fails this class (the fix merged for
 * goccy/bigquery-emulator#342 covers the default-stream <em>name forms</em> only and leaves this
 * status untouched, so no #342-carrying release retires anything here), and that failure is the
 * trigger to remove the {@code UNKNOWN} rewrite in {@code StreamWriterRowAppenderFactory} (the
 * schedule is {@code docs/adr/0029}), delete the two canary tests, and enable the {@code @Disabled}
 * post-fix case they guard. Note the fix alone does not make a buffered emulator round trip
 * possible: the emulator also assigns its own append offsets and keeps no flush cursor
 * (goccy/bigquery-emulator#505), which {@code BigQueryBufferedStreamWriter}'s consistency check
 * fails on.
 */
class BigQueryEmulatorMissingTableDeviationITCase extends AbstractBigQueryEmulatorITCase {

    private static final TableDestination MISSING =
            TableDestination.of(PROJECT, DATASET, "missing_table");

    @Test
    void createBufferedStreamOnAMissingTableAnswersUnknown() throws Exception {
        try (BufferedStreamService service =
                new WriteClientBufferedStreamService(
                        null,
                        BufferedStreamOptions.builder().build(),
                        EmulatorEndpoint.parse(grpcEndpoint()))) {
            Throwable failure = catchThrowable(() -> service.createBufferedStream(MISSING));

            assertThat(failure).isNotNull();
            assertThat(AppendErrorClassifier.hasCode(failure, Status.Code.UNKNOWN))
                    .as(
                            "CreateWriteStream on a missing table no longer answers UNKNOWN — if"
                                    + " this fails after an emulator-image bump, the"
                                    + " goccy/bigquery-emulator#504 fix arrived: retire per"
                                    + " docs/adr/0029 (see class javadoc). Answered: %s",
                            failure)
                    .isTrue();
            // The consequence the canary exists for: the writer's routing cannot see a missing
            // table in this response, so create-if-needed never fires against the emulator.
            assertThat(AppendErrorClassifier.isMissingTable(failure))
                    .as("the missing-table verdict can now see this response: %s", failure)
                    .isFalse();
        }
    }

    @Test
    void getWriteStreamOnAMissingTablesDefaultStreamAnswersUnknown() throws Exception {
        try (BigQueryWriteClient client =
                BigQueryWriteClients.forEmulator(EmulatorEndpoint.parse(grpcEndpoint()))) {
            Throwable failure =
                    catchThrowable(
                            () ->
                                    client.getWriteStream(
                                            GetWriteStreamRequest.newBuilder()
                                                    .setName(
                                                            MISSING.toTablePath()
                                                                    + "/streams/_default")
                                                    .build()));

            assertThat(failure).isNotNull();
            // The status StreamWriterRowAppenderFactory.createAgainstEmulator rewrites to
            // NOT_FOUND; anything else there is passed through untranslated.
            assertThat(AppendErrorClassifier.hasCode(failure, Status.Code.UNKNOWN))
                    .as(
                            "GetWriteStream on a missing table's default stream no longer answers"
                                    + " UNKNOWN — if this fails after an emulator-image bump, the"
                                    + " goccy/bigquery-emulator#504 fix arrived: retire per"
                                    + " docs/adr/0029 (see class javadoc). Answered: %s",
                            failure)
                    .isTrue();
        }
    }

    /**
     * The post-fix shape of the two canaries above: what both RPCs answer once the pinned image
     * carries the goccy/bigquery-emulator#504 fix, and the verdict the writer's {@code
     * CREATE_IF_NEEDED} routing then reaches without the {@code createAgainstEmulator} rewrite.
     * Enable it when the canaries fail on an image bump, in the same change that retires the
     * rewrite and deletes them ({@code docs/adr/0029}). {@code NOT_FOUND} is the emulator's
     * post-fix answer, not the real service's — real BigQuery masks a missing table as {@code
     * PERMISSION_DENIED} ({@code docs/adr/0030}), and {@link
     * BigQueryBufferedStreamMissingTableITCase} is what verifies the connector against that.
     */
    @Disabled("goccy/bigquery-emulator#504 — enable when the pinned emulator image carries the fix")
    @Test
    void aMissingTableAnswersNotFoundOnceTheEmulatorFixArrives() throws Exception {
        try (BufferedStreamService service =
                new WriteClientBufferedStreamService(
                        null,
                        BufferedStreamOptions.builder().build(),
                        EmulatorEndpoint.parse(grpcEndpoint()))) {
            Throwable failure = catchThrowable(() -> service.createBufferedStream(MISSING));

            assertThat(failure).isNotNull();
            assertThat(AppendErrorClassifier.hasCode(failure, Status.Code.NOT_FOUND)).isTrue();
            assertThat(AppendErrorClassifier.isMissingTable(failure)).isTrue();
        }
        try (BigQueryWriteClient client =
                BigQueryWriteClients.forEmulator(EmulatorEndpoint.parse(grpcEndpoint()))) {
            Throwable failure =
                    catchThrowable(
                            () ->
                                    client.getWriteStream(
                                            GetWriteStreamRequest.newBuilder()
                                                    .setName(
                                                            MISSING.toTablePath()
                                                                    + "/streams/_default")
                                                    .build()));

            assertThat(failure).isNotNull();
            assertThat(AppendErrorClassifier.hasCode(failure, Status.Code.NOT_FOUND)).isTrue();
        }
    }
}
