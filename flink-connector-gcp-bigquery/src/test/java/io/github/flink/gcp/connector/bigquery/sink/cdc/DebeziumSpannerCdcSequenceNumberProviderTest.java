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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumSpannerCdcSequenceNumberProviderTest {

    private final DebeziumSpannerCdcSequenceNumberProvider provider =
            new DebeziumSpannerCdcSequenceNumberProvider();

    @Test
    void encodesTheDocumentedSpannerEnvelope() {
        assertThat(provider.getSequenceNumber(sourceProperties("1670955531785000000", "1", "0")))
                .isEqualTo("17306D33FB84D440/0000000000000001/0000000000000000");
    }

    /**
     * Debezium writes the connector's own processing time as {@code ts_ms} beside {@code source} in
     * the payload, and the time it read the change as {@code read_at_timestamp} inside {@code
     * source}. Only the commit timestamp in {@code ts_ns} may order the change.
     */
    @Test
    void readsTheCommitTimestampRatherThanAProcessingTimestamp() {
        Map<String, String> properties = sourceProperties("1670955531785000000", "1", "0");
        properties.put("ts_ms", "1670955531785");
        properties.put("read_at_timestamp", "1670955531791");

        assertThat(provider.getSequenceNumber(properties))
                .isEqualTo("17306D33FB84D440/0000000000000001/0000000000000000");
    }

    @Test
    void ignoresTheSnapshotPropertyTheChangeStreamNeverVaries() {
        Map<String, String> properties = sourceProperties("1670955531785000000", "1", "0");
        properties.put("snapshot", "false");

        assertThat(provider.getSequenceNumber(properties))
                .isEqualTo("17306D33FB84D440/0000000000000001/0000000000000000");
    }

    /**
     * A low-watermark stamp carries no record sequence or mod number, and Debezium substitutes the
     * connector's clock for a missing timestamp. Rejecting it keeps processing time out of the
     * sequence.
     */
    @Test
    void rejectsAStampThatCarriesNoChangeCoordinates() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "spanner");
        properties.put("ts_ns", "1670955531785000000");
        properties.put("low_watermark", "1670955471635");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'mod_number'");
    }

    @Test
    void rejectsAnEnvelopeFromAnotherDebeziumConnector() {
        Map<String, String> properties = sourceProperties("1670955531785000000", "1", "0");
        properties.put("connector", "postgresql");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'spanner'");
    }

    @Test
    void rejectsMalformedCoordinatesTheTableProfileAlsoRejects() {
        assertThatThrownBy(
                        () ->
                                provider.getSequenceNumber(
                                        sourceProperties("1670955531785000000", "1", "-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'mod_number'");
        assertThatThrownBy(() -> provider.getSequenceNumber(sourceProperties("-1", "1", "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'ts_ns'");
    }

    @Test
    void rejectsAMissingSourcePropertiesMap() {
        assertThatThrownBy(() -> provider.getSequenceNumber(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sourceProperties");
    }

    private static Map<String, String> sourceProperties(
            String tsNs, String sequence, String modNumber) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "spanner");
        properties.put("ts_ns", tsNs);
        properties.put("sequence", sequence);
        properties.put("mod_number", modNumber);
        return properties;
    }
}
