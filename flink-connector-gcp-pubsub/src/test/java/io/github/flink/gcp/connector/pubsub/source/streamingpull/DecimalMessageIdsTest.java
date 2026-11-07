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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DecimalMessageIds}. */
class DecimalMessageIdsTest {

    @Test
    void roundTripsEveryLengthIncludingLeadingZeros() {
        List<String> ids =
                Arrays.asList(
                        "0",
                        "7",
                        "00",
                        "007",
                        "10",
                        "17045123456789012",
                        "00000000000000000",
                        "99999999999999999",
                        "01234567890123456");
        byte[] buffer = new byte[DecimalMessageIds.MAX_DIGITS];
        for (String id : ids) {
            long packed = DecimalMessageIds.pack(id);
            assertThat(packed).as(id).isNotNegative();
            int length = DecimalMessageIds.unpack(packed, buffer);
            assertThat(new String(buffer, 0, length, StandardCharsets.US_ASCII)).isEqualTo(id);
        }
    }

    @Test
    void doesNotPackAnythingButShortAsciiDigitStrings() {
        for (String id :
                Arrays.asList(
                        "", "170451234567890123", "12a", "-1", "+1", " 1", "1.0", "１２", "١٢")) {
            assertThat(DecimalMessageIds.pack(id)).as(id).isEqualTo(DecimalMessageIds.NOT_PACKED);
        }
    }

    @Test
    void distinctIdsPackToDistinctValuesInTheirByteOrder() {
        // Ids that are prefixes of one another or differ only in trailing zeros are the cases a
        // plain numeric value would conflate or misorder.
        List<String> ids =
                new ArrayList<>(
                        Arrays.asList("1", "10", "100", "01", "001", "1000", "2", "19", "0", "00"));
        Random random = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            int length = 1 + random.nextInt(DecimalMessageIds.MAX_DIGITS);
            StringBuilder id = new StringBuilder(length);
            for (int j = 0; j < length; j++) {
                // A narrow alphabet makes shared prefixes and trailing zeros common.
                id.append((char) ('0' + random.nextInt(3)));
            }
            ids.add(id.toString());
        }
        for (int i = 1; i < ids.size(); i++) {
            String a = ids.get(i - 1);
            String b = ids.get(i);
            int bytes =
                    Integer.signum(
                            Arrays.compareUnsigned(
                                    a.getBytes(StandardCharsets.UTF_8),
                                    b.getBytes(StandardCharsets.UTF_8)));
            int packed =
                    Integer.signum(
                            Long.compare(DecimalMessageIds.pack(a), DecimalMessageIds.pack(b)));
            assertThat(packed).as("%s vs %s", a, b).isEqualTo(bytes);
        }
    }
}
