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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.source.StartPosition;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Map;

/** Maps Table API startup options to the shared change-stream start position. */
@Internal
public final class ChangeStreamStartPositionMapper {
    private ChangeStreamStartPositionMapper() {}

    public static StartPosition startup(ReadableConfig config) {
        return map(
                config.get(SpannerConnectorOptions.SCAN_STARTUP_MODE),
                config.getOptional(SpannerConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)
                        .orElse(null));
    }

    @Nullable
    public static StartPosition resumeFallback(ReadableConfig config) {
        ChangeStreamStartMode mode =
                config.getOptional(SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE).orElse(null);
        return mode == null
                ? null
                : map(
                        mode,
                        config.getOptional(
                                        SpannerConnectorOptions
                                                .SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS)
                                .orElse(null));
    }

    public static void validate(ReadableConfig config, Map<String, String> supplied) {
        validatePair(
                config.get(SpannerConnectorOptions.SCAN_STARTUP_MODE),
                config.getOptional(SpannerConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)
                        .orElse(null),
                SpannerConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                supplied);
        ChangeStreamStartMode fallback =
                config.getOptional(SpannerConnectorOptions.SCAN_RESUME_FALLBACK_MODE).orElse(null);
        Long fallbackTimestamp =
                config.getOptional(SpannerConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS)
                        .orElse(null);
        if (fallback == null && fallbackTimestamp != null) {
            throw new ValidationException(
                    "scan.resume-fallback.timestamp-millis requires scan.resume-fallback.mode=timestamp.");
        }
        if (fallback != null) {
            validatePair(
                    fallback,
                    fallbackTimestamp,
                    SpannerConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS,
                    supplied);
        }
    }

    private static void validatePair(
            ChangeStreamStartMode mode,
            @Nullable Long timestamp,
            ConfigOption<Long> timestampOption,
            Map<String, String> supplied) {
        if (mode == ChangeStreamStartMode.TIMESTAMP && timestamp == null) {
            throw new ValidationException(
                    timestampOption.key()
                            + " is required when the corresponding mode is timestamp.");
        }
        if (mode != ChangeStreamStartMode.TIMESTAMP
                && supplied.containsKey(timestampOption.key())) {
            throw new ValidationException(
                    timestampOption.key()
                            + " may be set only when the corresponding mode is timestamp.");
        }
    }

    private static StartPosition map(ChangeStreamStartMode mode, @Nullable Long timestamp) {
        switch (mode) {
            case EARLIEST:
                return StartPosition.earliest();
            case LATEST:
                return StartPosition.latest();
            case TIMESTAMP:
                return StartPosition.at(Instant.ofEpochMilli(timestamp));
            default:
                throw new IllegalStateException("Unhandled start mode " + mode + ".");
        }
    }
}
