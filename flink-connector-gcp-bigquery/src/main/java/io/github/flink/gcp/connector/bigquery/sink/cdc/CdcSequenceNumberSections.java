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

/** Parses and formats the unsigned 64-bit sections used by BigQuery CDC sequences. */
@Internal
final class CdcSequenceNumberSections {

    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    private CdcSequenceNumberSections() {}

    static long parseUnsignedDecimal(String value) {
        if (!isUnsignedDecimal(value)) {
            throw new NumberFormatException("not an unsigned decimal value");
        }
        return Long.parseUnsignedLong(value);
    }

    static long parseSignedOrUnsignedDecimal(String value) {
        if (!isSignedDecimal(value)) {
            throw new NumberFormatException("not a signed or unsigned decimal value");
        }
        return value.charAt(0) == '-' ? Long.parseLong(value) : Long.parseUnsignedLong(value);
    }

    static String format(long... sections) {
        if (sections.length == 0 || sections.length > 4) {
            throw new IllegalArgumentException("A BigQuery CDC sequence has one to four sections");
        }
        char[] encoded = new char[sections.length * 16 + sections.length - 1];
        for (int section = 0; section < sections.length; section++) {
            int offset = section * 17;
            if (section > 0) {
                encoded[offset - 1] = '/';
            }
            writeUnsignedHex(sections[section], encoded, offset);
        }
        return new String(encoded);
    }

    private static boolean isUnsignedDecimal(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < '0' || value.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }

    private static boolean isSignedDecimal(String value) {
        if (value == null || value.isEmpty()) {
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

    private static void writeUnsignedHex(long value, char[] target, int offset) {
        for (int i = 15; i >= 0; i--) {
            target[offset + i] = HEX_DIGITS[(int) (value & 0x0F)];
            value >>>= 4;
        }
    }
}
