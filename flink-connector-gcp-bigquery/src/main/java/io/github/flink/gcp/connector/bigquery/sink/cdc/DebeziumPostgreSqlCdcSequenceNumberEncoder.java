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

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.JsonToken;
import com.google.api.client.json.gson.GsonFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** Transcodes Debezium PostgreSQL sequence metadata into BigQuery CDC hexadecimal sections. */
@Internal
public final class DebeziumPostgreSqlCdcSequenceNumberEncoder {

    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    private DebeziumPostgreSqlCdcSequenceNumberEncoder() {}

    /** Encodes the three relevant Debezium PostgreSQL source properties. */
    public static String sequenceNumber(
            @Nullable String connector, @Nullable String sequence, @Nullable String lsn) {
        if (connector == null || connector.isEmpty()) {
            throw new IllegalArgumentException(
                    "The Debezium source properties must contain a non-empty 'connector' property");
        }
        if (!"postgresql".equals(connector)) {
            throw new IllegalArgumentException(
                    "Expected Debezium connector 'postgresql' but found '" + connector + "'");
        }
        if (sequence == null) {
            throw new IllegalArgumentException(
                    "Debezium PostgreSQL source properties must contain a non-null 'sequence'"
                            + " property");
        }

        long[] positions = parsePostgreSqlSequence(sequence);
        if (lsn != null && parseLsn(lsn, "'lsn'") != positions[1]) {
            throw new IllegalArgumentException(
                    "Debezium PostgreSQL 'lsn' does not match the current LSN in 'sequence'");
        }
        return formatPostgreSqlSequence(positions[0], positions[1]);
    }

    private static String formatPostgreSqlSequence(long lastCommitted, long current) {
        char[] encoded = new char[33];
        encoded[16] = '/';
        writeUnsignedHex(lastCommitted, encoded, 0);
        writeUnsignedHex(current, encoded, 17);
        return new String(encoded);
    }

    private static void writeUnsignedHex(long value, char[] target, int offset) {
        for (int i = 15; i >= 0; i--) {
            target[offset + i] = HEX_DIGITS[(int) (value & 0x0F)];
            value >>>= 4;
        }
    }

    private static long[] parsePostgreSqlSequence(String sequence) {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(sequence)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw malformedSequence();
            }
            long lastCommitted = readSequenceLsn(parser, "last committed LSN", true);
            long current = readSequenceLsn(parser, "current LSN", false);
            if (parser.nextToken() != JsonToken.END_ARRAY || parser.nextToken() != null) {
                throw malformedSequence();
            }
            return new long[] {lastCommitted, current};
        } catch (IOException e) {
            throw malformedSequence();
        }
    }

    private static long readSequenceLsn(JsonParser parser, String name, boolean nullable)
            throws IOException {
        JsonToken token = parser.nextToken();
        if (token == JsonToken.VALUE_NULL && nullable) {
            return 0L;
        }
        if (token != JsonToken.VALUE_STRING) {
            throw malformedSequence();
        }
        return parseLsn(parser.getText(), "'sequence' " + name);
    }

    private static long parseLsn(String value, String name) {
        if (!isDecimal(value)) {
            throw invalidLsn(name);
        }
        try {
            return value.charAt(0) == '-' ? Long.parseLong(value) : Long.parseUnsignedLong(value);
        } catch (NumberFormatException e) {
            throw invalidLsn(name);
        }
    }

    private static boolean isDecimal(String value) {
        if (value.isEmpty()) {
            return false;
        }
        int firstDigit = value.charAt(0) == '-' ? 1 : 0;
        if (firstDigit == value.length()) {
            return false;
        }
        for (int i = firstDigit; i < value.length(); i++) {
            if (value.charAt(i) < '0' || value.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }

    private static IllegalArgumentException malformedSequence() {
        return new IllegalArgumentException(
                "Debezium PostgreSQL 'sequence' must be a two-element JSON array containing a"
                        + " nullable last committed LSN and a required current LSN");
    }

    private static IllegalArgumentException invalidLsn(String name) {
        return new IllegalArgumentException(
                "Debezium PostgreSQL "
                        + name
                        + " must be a signed or unsigned 64-bit decimal value");
    }
}
