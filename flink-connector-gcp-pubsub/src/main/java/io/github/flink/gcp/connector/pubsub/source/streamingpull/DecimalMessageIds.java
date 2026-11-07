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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;

/**
 * Packs a message id made of at most {@value #MAX_DIGITS} ASCII digits into one {@code long}, so
 * that the restore-deduplication ledger can sort and hold the ids the service was measured
 * assigning without a {@code String} object per id.
 *
 * <p>Message ids stay opaque: an id of any other shape is not packed, and callers keep it as a
 * string. The packing is a bijection on the ids it accepts, leading zeros included. The digits are
 * left-aligned to {@value #MAX_DIGITS} places and the length sits in the low five bits, so packed
 * values compare as signed longs in the same order as the ids' UTF-8 bytes compare unsigned.
 */
@Internal
public final class DecimalMessageIds {

    /** The longest id that packs; 10<sup>17</sup> &times; 32 still fits a positive long. */
    static final int MAX_DIGITS = 17;

    /** Returned by {@link #pack(String)} for an id that does not pack. */
    public static final long NOT_PACKED = -1L;

    private static final long[] POWERS = new long[MAX_DIGITS + 1];

    static {
        POWERS[0] = 1;
        for (int i = 1; i <= MAX_DIGITS; i++) {
            POWERS[i] = POWERS[i - 1] * 10;
        }
    }

    private DecimalMessageIds() {}

    /**
     * Packs an id.
     *
     * @param id the message id
     * @return the packed id, a non-negative value, or {@link #NOT_PACKED} when the id is empty,
     *     longer than {@value #MAX_DIGITS} characters, or holds anything but ASCII digits
     */
    public static long pack(String id) {
        int length = id.length();
        if (length == 0 || length > MAX_DIGITS) {
            return NOT_PACKED;
        }
        long digits = 0;
        for (int i = 0; i < length; i++) {
            int d = id.charAt(i) - '0';
            if (d < 0 || d > 9) {
                return NOT_PACKED;
            }
            digits = digits * 10 + d;
        }
        return (digits * POWERS[MAX_DIGITS - length]) << 5 | length;
    }

    /**
     * Writes the digits of a packed id into {@code target}.
     *
     * @param packed a value {@link #pack(String)} returned for some id
     * @param target receives the id's ASCII digits from index 0; at least {@value #MAX_DIGITS} long
     * @return the id's length
     */
    static int unpack(long packed, byte[] target) {
        int length = (int) (packed & 0x1F);
        long digits = (packed >>> 5) / POWERS[MAX_DIGITS - length];
        for (int i = length - 1; i >= 0; i--) {
            target[i] = (byte) ('0' + digits % 10);
            digits /= 10;
        }
        return length;
    }
}
