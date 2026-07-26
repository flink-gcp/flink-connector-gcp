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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferedStreamOptions}. */
class BufferedStreamOptionsTest {

    @Test
    void defaultsAreValid() {
        BufferedStreamOptions options = BufferedStreamOptions.builder().build();

        assertThat(options.getMaxAppendRequestBytes())
                .isEqualTo(BufferedStreamOptions.DEFAULT_MAX_APPEND_REQUEST_BYTES);
        assertThat(options.getRetryInitialBackoff())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_INITIAL_BACKOFF);
        assertThat(options.getRetryMaxBackoff())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_MAX_BACKOFF);
        assertThat(options.getRetryMaxAttempts())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_MAX_ATTEMPTS);
    }

    @Test
    void carriesConfiguredValues() {
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .maxAppendRequestBytes(1024)
                        .retryInitialBackoff(Duration.ofMillis(100))
                        .retryMaxBackoff(Duration.ofSeconds(5))
                        .retryMaxAttempts(3)
                        .build();

        assertThat(options.getMaxAppendRequestBytes()).isEqualTo(1024);
        assertThat(options.getRetryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(options.getRetryMaxBackoff()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getRetryMaxAttempts()).isEqualTo(3);
    }

    @Test
    void rejectsNonPositiveValues() {
        assertThatThrownBy(() -> BufferedStreamOptions.builder().maxAppendRequestBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMaxBackoffBelowInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .retryInitialBackoff(Duration.ofSeconds(5))
                                        .retryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxBackoff");
    }
}
