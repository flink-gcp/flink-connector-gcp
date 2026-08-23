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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Does each storage sink wrap the admin its writer creates tables through, and on which budget?
 *
 * <p>A seam worth its own test for the reason {@code BigQueryBufferedStreamSinkCommitterTest} gives
 * about the create disposition: every writer test injects its own admin, so a {@code createWriter}
 * that stopped wrapping — or wrapped on the wrong schedule — would leave every unit and emulator
 * test green, and the only thing to notice would be a real-GCP job losing a creation race it
 * usually wins (#383).
 *
 * <p>The attempt budget is set away from its default so the assertion discriminates: it is the one
 * field of the wired schedule that names <em>which</em> schedule was taken.
 */
class BigQueryStorageSinkTableAdminWiringTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final int RECOVERY_ATTEMPTS = 7;

    @TempDir Path tempDir;

    @Test
    void theAtLeastOnceSinkPassesCredentialsToItsWriterAndAdmin() {
        String missingPath = tempDir.resolve("missing-at-least-once-key.json").toString();
        BigQueryDefaultStreamSink<String> sink = atLeastOnceSink(missingPath);

        assertSanitizedCredentialFailure(
                () ->
                        sink.createRowAppenderFactory()
                                .create(DESTINATION, Empty.getDescriptor(), null),
                missingPath);
        assertSanitizedCredentialFailure(
                () -> sink.createTableAdmin().getSchema(DESTINATION), missingPath);
    }

    @Test
    void theExactlyOnceSinkPassesCredentialsToItsWriterAndAdmin() {
        String missingPath = tempDir.resolve("missing-exactly-once-key.json").toString();
        BigQueryBufferedStreamSink<String> sink = exactlyOnceSink(missingPath);

        assertSanitizedCredentialFailure(
                () ->
                        sink.getServiceFactory()
                                .create(null, BufferedStreamOptions.builder().build()),
                missingPath);
        assertSanitizedCredentialFailure(
                () -> sink.createTableAdmin().getSchema(DESTINATION), missingPath);
    }

    @Test
    void theAtLeastOnceSinkWrapsOnItsRecoveryBudget() {
        BigQueryDefaultStreamSink<String> sink = atLeastOnceSink(null);

        assertWrappedOnRecovery(sink.createTableAdmin());
    }

    @Test
    void theExactlyOnceSinkWrapsOnItsRecoveryBudget() {
        BigQueryBufferedStreamSink<String> sink = exactlyOnceSink(null);

        assertWrappedOnRecovery(sink.createTableAdmin());
    }

    private static void assertWrappedOnRecovery(TableAdmin admin) {
        assertThat(admin)
                .isInstanceOf(RetryingTableAdmin.class)
                .asInstanceOf(
                        org.assertj.core.api.InstanceOfAssertFactories.type(
                                RetryingTableAdmin.class))
                .extracting(RetryingTableAdmin::getSchedule)
                .extracting(schedule -> schedule.maxAttempts())
                .isEqualTo(RECOVERY_ATTEMPTS);
    }

    private static BigQueryDefaultStreamSink<String> atLeastOnceSink(String keyFile) {
        io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                        .table(DESTINATION)
                        .serializer(new NameValueRowSerializer())
                        .defaultStreamOptions(
                                DefaultStreamOptions.builder()
                                        .recoveryMaxAttempts(RECOVERY_ATTEMPTS)
                                        .recoveryMaxBackoff(Duration.ofSeconds(3))
                                        .build());
        if (keyFile != null) {
            builder.serviceAccountKeyFile(keyFile);
        }
        return (BigQueryDefaultStreamSink<String>) builder.build();
    }

    private static BigQueryBufferedStreamSink<String> exactlyOnceSink(String keyFile) {
        io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .table(DESTINATION)
                        .serializer(new NameValueRowSerializer())
                        .bufferedStreamOptions(
                                BufferedStreamOptions.builder()
                                        .recoveryMaxAttempts(RECOVERY_ATTEMPTS)
                                        .recoveryMaxBackoff(Duration.ofSeconds(3))
                                        .build());
        if (keyFile != null) {
            builder.serviceAccountKeyFile(keyFile);
        }
        return (BigQueryBufferedStreamSink<String>) builder.build();
    }

    private static void assertSanitizedCredentialFailure(
            ThrowingOperation operation, String missingPath) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured BigQuery service-account key file.")
                .hasNoCause()
                .hasMessageNotContaining(missingPath);
    }

    @FunctionalInterface
    private interface ThrowingOperation {
        void run() throws Exception;
    }
}
