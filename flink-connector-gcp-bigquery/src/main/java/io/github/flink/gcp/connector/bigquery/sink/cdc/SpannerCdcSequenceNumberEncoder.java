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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Transcodes Spanner change-stream coordinates into BigQuery CDC hexadecimal sections.
 *
 * <p>Both the Debezium Spanner envelope and this repository's native change-stream source reach
 * BigQuery through {@link #sequenceNumber(long, String, int)}, so equivalent records from the two
 * routes encode to the same sequence.
 */
@Internal
public final class SpannerCdcSequenceNumberEncoder {

    private static final String CONNECTOR = "spanner";

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private SpannerCdcSequenceNumberEncoder() {}

    /**
     * Encodes one Spanner mod whose commit timestamp is an instant.
     *
     * <p>Both routes that hold a commit timestamp as a wall-clock value reach the sections through
     * here, so the representable range is stated once.
     */
    public static String sequenceNumber(
            Instant commitTimestamp, @Nullable String recordSequence, int modNumber) {
        requireNonNull(commitTimestamp, "commitTimestamp must not be null");
        long commitTimestampNanos;
        try {
            commitTimestampNanos =
                    Math.addExact(
                            Math.multiplyExact(commitTimestamp.getEpochSecond(), NANOS_PER_SECOND),
                            commitTimestamp.getNano());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "A Spanner commit timestamp must be representable as nanoseconds in a 64-bit"
                            + " value but found "
                            + commitTimestamp);
        }
        return sequenceNumber(commitTimestampNanos, recordSequence, modNumber);
    }

    /**
     * Encodes the commit timestamp, record sequence, and mod number of one Spanner change.
     *
     * @param commitTimestampNanos nanoseconds since the epoch at which Spanner committed the change
     * @param recordSequence the change record's sequence within its transaction and partition
     * @param modNumber the zero-based position of the mod within its change record
     */
    public static String sequenceNumber(
            long commitTimestampNanos, @Nullable String recordSequence, int modNumber) {
        if (commitTimestampNanos < 0) {
            throw new IllegalArgumentException(
                    "A Spanner commit timestamp must not precede 1970-01-01T00:00:00Z");
        }
        if (modNumber < 0) {
            throw new IllegalArgumentException(
                    "A Spanner mod number must not be negative but found " + modNumber);
        }
        return CdcSequenceNumberSections.format(
                commitTimestampNanos, parseRecordSequence(recordSequence), modNumber);
    }

    /**
     * Encodes the four relevant Debezium Spanner source properties.
     *
     * <p>The timestamp is read from {@code source.ts_ns}, which Debezium derives from the Spanner
     * commit timestamp. The sibling {@code payload.ts_ns} carries the connector's own processing
     * time and never reaches a source-properties map.
     */
    public static String debeziumSequenceNumber(
            @Nullable String connector,
            @Nullable String commitTimestampNanos,
            @Nullable String recordSequence,
            @Nullable String modNumber) {
        if (connector == null || connector.isEmpty()) {
            throw new IllegalArgumentException(
                    "The Debezium source properties must contain a non-empty 'connector' property");
        }
        if (!CONNECTOR.equals(connector)) {
            throw new IllegalArgumentException(
                    "Expected Debezium connector '"
                            + CONNECTOR
                            + "' but found '"
                            + connector
                            + "'");
        }
        return sequenceNumber(
                parseCommitTimestampNanos(commitTimestampNanos),
                recordSequence,
                parseModNumber(modNumber));
    }

    private static long parseCommitTimestampNanos(@Nullable String value) {
        long nanos;
        try {
            nanos = CdcSequenceNumberSections.parseUnsignedDecimal(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Debezium Spanner 'ts_ns' must be an unsigned decimal number of nanoseconds"
                            + " since the epoch but found '"
                            + value
                            + "'");
        }
        if (nanos < 0) {
            throw new IllegalArgumentException(
                    "Debezium Spanner 'ts_ns' must not exceed "
                            + Long.MAX_VALUE
                            + " nanoseconds since the epoch but found '"
                            + value
                            + "'");
        }
        return nanos;
    }

    private static int parseModNumber(@Nullable String value) {
        long modNumber;
        try {
            modNumber = CdcSequenceNumberSections.parseUnsignedDecimal(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Debezium Spanner 'mod_number' must be an unsigned decimal value but found '"
                            + value
                            + "'");
        }
        if (modNumber < 0 || modNumber > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Debezium Spanner 'mod_number' must not exceed "
                            + Integer.MAX_VALUE
                            + " but found '"
                            + value
                            + "'");
        }
        return (int) modNumber;
    }

    private static long parseRecordSequence(@Nullable String recordSequence) {
        try {
            return CdcSequenceNumberSections.parseUnsignedDecimal(recordSequence);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "A Spanner record sequence must be an unsigned 64-bit decimal value but found '"
                            + recordSequence
                            + "'");
        }
    }
}
