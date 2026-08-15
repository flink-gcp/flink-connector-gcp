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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the native Spanner change-stream entry point against the Debezium route. */
class SpannerCdcSequenceNumberTest {

    /** The commit timestamp of the envelope in Debezium's Spanner connector documentation. */
    private static final Instant DOCUMENTED_COMMIT_TIMESTAMP =
            Instant.ofEpochSecond(1670955531L, 785000000);

    @Test
    void encodesTheDocumentedChangeRecord() {
        assertThat(SpannerCdcSequenceNumber.of(DOCUMENTED_COMMIT_TIMESTAMP, "00000001", 0))
                .isEqualTo("17306D33FB84D440/0000000000000001/0000000000000000");
    }

    @Test
    void agreesWithTheDebeziumRouteOnEquivalentRecords() {
        Instant commitTimestamp = Instant.ofEpochSecond(1670955531L, 785000123);

        assertThat(SpannerCdcSequenceNumber.of(commitTimestamp, "00000042", 3))
                .isEqualTo(
                        new DebeziumSpannerCdcSequenceNumberProvider()
                                .getSequenceNumber(
                                        sourceProperties("1670955531785000123", "42", "3")));
    }

    @Test
    void preservesNanosecondsBelowTheMillisecond() {
        Instant millisecond = Instant.ofEpochSecond(1670955531L, 785000000);
        Instant nanosecondLater = Instant.ofEpochSecond(1670955531L, 785000001);

        assertThat(SpannerCdcSequenceNumber.of(millisecond, "1", 0))
                .isLessThan(SpannerCdcSequenceNumber.of(nanosecondLater, "1", 0));
    }

    @Test
    void rejectsCommitTimestampsOutsideTheRepresentableRange() {
        assertThatThrownBy(() -> SpannerCdcSequenceNumber.of(Instant.EPOCH.minusNanos(1), "1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1970-01-01T00:00:00Z");
        assertThatThrownBy(() -> SpannerCdcSequenceNumber.of(Instant.MAX, "1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("64-bit");
    }

    @Test
    void rejectsMissingCoordinates() {
        assertThatThrownBy(() -> SpannerCdcSequenceNumber.of(null, "1", 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("commitTimestamp");
        assertThatThrownBy(() -> SpannerCdcSequenceNumber.of(DOCUMENTED_COMMIT_TIMESTAMP, null, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SpannerCdcSequenceNumber.of(DOCUMENTED_COMMIT_TIMESTAMP, "1", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Map<String, String> sourceProperties(
            String tsNs, String sequence, String modNumber) {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "spanner");
        properties.put("ts_ns", tsNs);
        properties.put("sequence", sequence);
        properties.put("mod_number", modNumber);
        return properties;
    }
}
