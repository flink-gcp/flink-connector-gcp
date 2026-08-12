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

import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Credential-loading tests for {@link WriteClientBufferedStreamService}. */
class WriteClientBufferedStreamServiceCredentialsTest {

    @TempDir Path tempDir;

    @Test
    void configuredCredentialsAreLoadedWhenTheProductionClientOpens() {
        String missingPath = tempDir.resolve("missing-buffered-stream-secret.json").toString();

        assertThatThrownBy(
                        () ->
                                new WriteClientBufferedStreamService(
                                        null,
                                        BufferedStreamOptions.builder().build(),
                                        missingPath,
                                        null))
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Failed to load the configured BigQuery service-account key file.")
                .hasNoCause()
                .hasMessageNotContaining(missingPath);
    }
}
