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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void theAtLeastOnceSinkWrapsOnItsRecoveryBudget() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                                .destination(DESTINATION)
                                .serializer(new NameValueRowSerializer())
                                .defaultStreamOptions(
                                        DefaultStreamOptions.builder()
                                                .recoveryMaxAttempts(RECOVERY_ATTEMPTS)
                                                .recoveryMaxBackoff(Duration.ofSeconds(3))
                                                .build())
                                .build();

        assertWrappedOnRecovery(sink.createTableAdmin());
    }

    @Test
    void theExactlyOnceSinkWrapsOnItsRecoveryBudget() {
        BigQueryBufferedStreamSink<String> sink =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .destination(DESTINATION)
                                .serializer(new NameValueRowSerializer())
                                .bufferedStreamOptions(
                                        BufferedStreamOptions.builder()
                                                .recoveryMaxAttempts(RECOVERY_ATTEMPTS)
                                                .recoveryMaxBackoff(Duration.ofSeconds(3))
                                                .build())
                                .build();

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
}
