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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/** Transcodes Debezium MySQL GTID coordinates into BigQuery CDC sequence sections. */
@Internal
public final class DebeziumMySqlCdcSequenceNumberEncoder implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern UUID =
            Pattern.compile(
                    "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}");

    private final Map<String, Long> sourceUuidEpochs;

    /** Creates an encoder whose source UUID order defines the immutable SID epochs. */
    public DebeziumMySqlCdcSequenceNumberEncoder(List<String> sourceUuids) {
        requireNonNull(sourceUuids, "sourceUuids must not be null");
        if (sourceUuids.isEmpty()) {
            throw new IllegalArgumentException(
                    "Debezium MySQL sequence generation requires at least one source UUID");
        }
        Map<String, Long> epochs = new LinkedHashMap<>();
        long epoch = 0L;
        for (String sourceUuid : sourceUuids) {
            String normalized = normalizeSourceUuid(sourceUuid);
            epoch = nextEpoch(epoch);
            if (epochs.putIfAbsent(normalized, epoch) != null) {
                throw new IllegalArgumentException(
                        "Debezium MySQL source UUID list contains duplicate UUID '"
                                + sourceUuid
                                + "'");
            }
        }
        this.sourceUuidEpochs = Collections.unmodifiableMap(epochs);
    }

    /** Encodes the Debezium MySQL source properties used by the built-in profile. */
    public String sequenceNumber(
            @Nullable String connector,
            @Nullable String snapshot,
            @Nullable String gtid,
            @Nullable String position,
            @Nullable String row) {
        if (!"mysql".equals(connector)) {
            throw new IllegalArgumentException(
                    "Expected Debezium connector 'mysql' but found '" + connector + "'");
        }
        if ("true".equals(snapshot) || "last".equals(snapshot)) {
            return CdcSequenceNumberSections.format(0L, 0L, 0L, 0L);
        }
        if ("incremental".equals(snapshot)) {
            throw new IllegalArgumentException(
                    "Debezium MySQL incremental snapshots cannot generate BigQuery CDC sequences");
        }
        if (snapshot != null && !"false".equals(snapshot)) {
            throw new IllegalArgumentException(
                    "Debezium MySQL 'snapshot' must be 'true', 'last', 'false', or null");
        }

        Gtid parsedGtid = parseGtid(gtid);
        Long epoch = sourceUuidEpochs.get(parsedGtid.sourceUuid);
        if (epoch == null) {
            throw new IllegalArgumentException(
                    "Debezium MySQL GTID uses unknown source UUID '" + parsedGtid.sourceUuid + "'");
        }
        long binlogPosition = parseCoordinate(position, "'pos'");
        long rowWithinEvent = parseCoordinate(row, "'row'");
        return CdcSequenceNumberSections.format(
                epoch, parsedGtid.transactionId, binlogPosition, rowWithinEvent);
    }

    static long nextEpoch(long epoch) {
        if (epoch == -1L) {
            throw new IllegalArgumentException("Debezium MySQL source UUID epoch overflow");
        }
        return epoch + 1L;
    }

    private static Gtid parseGtid(@Nullable String gtid) {
        if (gtid == null) {
            throw invalidGtid();
        }
        int separator = gtid.indexOf(':');
        if (separator <= 0 || separator != gtid.lastIndexOf(':')) {
            throw invalidGtid();
        }
        String sourceUuid = normalizeSourceUuid(gtid.substring(0, separator));
        long transactionId;
        try {
            transactionId =
                    CdcSequenceNumberSections.parseUnsignedDecimal(gtid.substring(separator + 1));
        } catch (NumberFormatException e) {
            throw invalidGtid();
        }
        return new Gtid(sourceUuid, transactionId);
    }

    private static String normalizeSourceUuid(@Nullable String sourceUuid) {
        if (sourceUuid == null || !UUID.matcher(sourceUuid).matches()) {
            throw new IllegalArgumentException(
                    "Debezium MySQL source UUIDs must use the canonical UUID form");
        }
        return sourceUuid.toLowerCase(Locale.ROOT);
    }

    private static long parseCoordinate(@Nullable String value, String name) {
        try {
            return CdcSequenceNumberSections.parseUnsignedDecimal(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Debezium MySQL " + name + " must be an unsigned 64-bit decimal value");
        }
    }

    private static IllegalArgumentException invalidGtid() {
        return new IllegalArgumentException(
                "Debezium MySQL 'gtid' must be an untagged UUID:transaction_id value");
    }

    private static final class Gtid {
        private final String sourceUuid;
        private final long transactionId;

        private Gtid(String sourceUuid, long transactionId) {
            this.sourceUuid = sourceUuid;
            this.transactionId = transactionId;
        }
    }
}
