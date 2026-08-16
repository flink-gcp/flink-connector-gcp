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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.base.source.StartPosition;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeStreamStartPositionMapperTest {

    @Test
    void mapsEveryStartupMode() {
        Configuration config = new Configuration();

        assertThat(ChangeStreamStartPositionMapper.startup(config))
                .isEqualTo(StartPosition.latest());

        config.set(SpannerConnectorOptions.SCAN_STARTUP_MODE, ChangeStreamStartMode.EARLIEST);
        assertThat(ChangeStreamStartPositionMapper.startup(config))
                .isEqualTo(StartPosition.earliest());

        config.set(SpannerConnectorOptions.SCAN_STARTUP_MODE, ChangeStreamStartMode.TIMESTAMP);
        config.set(SpannerConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS, 1_000L);
        assertThat(ChangeStreamStartPositionMapper.startup(config))
                .isEqualTo(StartPosition.at(Instant.ofEpochMilli(1_000L)));
    }

    @Test
    void mapsOptionalResumeFallbackModes() {
        Configuration config = new Configuration();

        assertThat(ChangeStreamStartPositionMapper.resumeFallback(config)).isNull();

        config.set(SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE, ChangeStreamStartMode.LATEST);
        assertThat(ChangeStreamStartPositionMapper.resumeFallback(config))
                .isEqualTo(StartPosition.latest());

        config.set(
                SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE, ChangeStreamStartMode.EARLIEST);
        assertThat(ChangeStreamStartPositionMapper.resumeFallback(config))
                .isEqualTo(StartPosition.earliest());

        config.set(
                SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE, ChangeStreamStartMode.TIMESTAMP);
        config.set(SpannerConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS, 2_000L);
        assertThat(ChangeStreamStartPositionMapper.resumeFallback(config))
                .isEqualTo(StartPosition.at(Instant.ofEpochMilli(2_000L)));
    }
}
