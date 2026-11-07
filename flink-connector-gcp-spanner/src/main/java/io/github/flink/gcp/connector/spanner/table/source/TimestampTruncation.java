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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;

import com.google.cloud.Timestamp;

import javax.annotation.Nullable;

/**
 * Truncation of a Spanner {@code TIMESTAMP} to a declared {@code TIMESTAMP_LTZ(p)} precision.
 *
 * <p>A read value is truncated to {@code p}, so every stored value in the bucket {@code [floor,
 * floor + unit)} reads as {@code floor}. The scan converter, the filter pushdown and the lookup
 * range read all reason in those buckets, and share them here.
 */
@Internal
final class TimestampTruncation {

    /** The nanosecond unit a {@code TIMESTAMP_LTZ(p)} value keeps, indexed by {@code p}. */
    private static final int[] NANOS_UNIT = {
        1_000_000_000, 100_000_000, 10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10, 1
    };

    private TimestampTruncation() {}

    /** The nanosecond unit a value of {@code type} keeps: {@code 1} keeps every digit. */
    static int unit(LocalZonedTimestampType type) {
        return NANOS_UNIT[type.getPrecision()];
    }

    /**
     * The start of the bucket holding {@code timestamp}. A timestamp's nanoseconds are never
     * negative, even before the epoch, so truncating them rounds down.
     */
    static Timestamp floor(Timestamp timestamp, int unit) {
        int nanos = timestamp.getNanos();
        return Timestamp.ofTimeSecondsAndNanos(timestamp.getSeconds(), nanos - nanos % unit);
    }

    /**
     * The start of the bucket after the one starting at {@code floor}, or null when it would lie
     * past {@link Timestamp#MAX_VALUE}.
     */
    @Nullable
    static Timestamp nextBucket(Timestamp floor, int unit) {
        long nanos = (long) floor.getNanos() + unit;
        long seconds = floor.getSeconds() + nanos / 1_000_000_000L;
        if (seconds > Timestamp.MAX_VALUE.getSeconds()) {
            return null;
        }
        return Timestamp.ofTimeSecondsAndNanos(seconds, (int) (nanos % 1_000_000_000L));
    }
}
