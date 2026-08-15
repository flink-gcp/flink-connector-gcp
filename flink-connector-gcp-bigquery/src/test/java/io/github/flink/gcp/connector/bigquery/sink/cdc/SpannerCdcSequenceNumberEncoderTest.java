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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Spanner to BigQuery CDC sequence encoding contract directly. */
class SpannerCdcSequenceNumberEncoderTest {

    /** The commit timestamp of the envelope in Debezium's Spanner connector documentation. */
    private static final String DOCUMENTED_TIMESTAMP_NS = "1670955531785000000";

    private static final String DOCUMENTED_TIMESTAMP_SECTION = "17306D33FB84D440";

    @ParameterizedTest(name = "{0}")
    @MethodSource("spannerSourceShapes")
    void encodesDocumentedSpannerSourceShapes(
            String description, String tsNs, String sequence, String modNumber, String expected) {
        assertThat(debeziumSequenceNumber(tsNs, sequence, modNumber)).isEqualTo(expected);
    }

    private static Stream<Arguments> spannerSourceShapes() {
        return Stream.of(
                Arguments.of(
                        "the documented create envelope",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1",
                        "0",
                        DOCUMENTED_TIMESTAMP_SECTION + "/0000000000000001/0000000000000000"),
                Arguments.of(
                        "the epoch instant with a zero record sequence",
                        "0",
                        "0",
                        "0",
                        "0000000000000000/0000000000000000/0000000000000000"),
                Arguments.of(
                        "a second mod of the same change record",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1",
                        "1",
                        DOCUMENTED_TIMESTAMP_SECTION + "/0000000000000001/0000000000000001"));
    }

    @Test
    void distinguishesCommitTimestampsOneNanosecondApart() {
        String earlier = debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "0");
        String later = debeziumSequenceNumber("1670955531785000001", "1", "0");

        assertThat(earlier)
                .isEqualTo(DOCUMENTED_TIMESTAMP_SECTION + "/0000000000000001/0000000000000000");
        assertThat(later).isEqualTo("17306D33FB84D441/0000000000000001/0000000000000000");
        assertThat(earlier).isLessThan(later);
    }

    @Test
    void ordersSeveralRecordsOfOneTransactionByRecordSequence() {
        String first = debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "0");
        String second = debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "2", "0");

        assertThat(first).isLessThan(second);
    }

    @Test
    void ordersSeveralModsOfOneRecordByModNumber() {
        String first = debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "0");
        String second = debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "1");

        assertThat(first).isLessThan(second);
    }

    @Test
    void readsAZeroPaddedRecordSequenceAsItsNumericValue() {
        assertThat(debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "00000001", "0"))
                .isEqualTo(debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "0"));
        assertThat(debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "00000010", "0"))
                .isGreaterThan(debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "9", "0"));
    }

    @Test
    void encodesTheBoundariesOfEachSection() {
        assertThat(
                        debeziumSequenceNumber(
                                "9223372036854775807", "18446744073709551615", "2147483647"))
                .isEqualTo("7FFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/000000007FFFFFFF");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedSourceProperties")
    void rejectsMalformedSourceProperties(
            String description, String connector, String tsNs, String sequence, String modNumber) {
        assertThatThrownBy(
                        () ->
                                SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                        connector, tsNs, sequence, modNumber))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Stream<Arguments> malformedSourceProperties() {
        return Stream.of(
                Arguments.of("a missing connector", null, DOCUMENTED_TIMESTAMP_NS, "1", "0"),
                Arguments.of("an empty connector", "", DOCUMENTED_TIMESTAMP_NS, "1", "0"),
                Arguments.of(
                        "another Debezium connector",
                        "postgresql",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1",
                        "0"),
                Arguments.of("a missing timestamp", "spanner", null, "1", "0"),
                Arguments.of("an empty timestamp", "spanner", "", "1", "0"),
                Arguments.of(
                        "a signed timestamp, which is not the unsigned form",
                        "spanner",
                        "-1",
                        "1",
                        "0"),
                Arguments.of("a nonnumeric timestamp", "spanner", "2026-08-15", "1", "0"),
                Arguments.of(
                        "a timestamp past the representable range",
                        "spanner",
                        "9223372036854775808",
                        "1",
                        "0"),
                Arguments.of(
                        "a timestamp too long to be a 64-bit value",
                        "spanner",
                        "18446744073709551616",
                        "1",
                        "0"),
                Arguments.of(
                        "a missing record sequence", "spanner", DOCUMENTED_TIMESTAMP_NS, null, "0"),
                Arguments.of(
                        "a negative record sequence",
                        "spanner",
                        DOCUMENTED_TIMESTAMP_NS,
                        "-1",
                        "0"),
                Arguments.of(
                        "a fractional record sequence",
                        "spanner",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1.5",
                        "0"),
                Arguments.of(
                        "a record sequence too long to be a 64-bit value",
                        "spanner",
                        DOCUMENTED_TIMESTAMP_NS,
                        "18446744073709551616",
                        "0"),
                Arguments.of("a missing mod number", "spanner", DOCUMENTED_TIMESTAMP_NS, "1", null),
                Arguments.of(
                        "a signed mod number, which is not the unsigned form",
                        "spanner",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1",
                        "-1"),
                Arguments.of(
                        "a mod number past the signed 32-bit range",
                        "spanner",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1",
                        "2147483648"),
                Arguments.of(
                        "a mod number whose low 32 bits look like a valid position",
                        "spanner",
                        DOCUMENTED_TIMESTAMP_NS,
                        "1",
                        "4294967296"),
                Arguments.of(
                        "a nonnumeric mod number", "spanner", DOCUMENTED_TIMESTAMP_NS, "1", "x"));
    }

    /**
     * Each guard has its own message, and several of them reject overlapping inputs: a value past
     * the signed range is also negative once parsed, and an absent connector is also not {@code
     * spanner}. Pinning the messages is what keeps one guard's removal from being covered by the
     * next guard throwing the same exception type.
     */
    @Test
    void namesTheRejectedSpannerSourceProperty() {
        assertThatThrownBy(() -> debeziumSequenceNumber("-1", "1", "0"))
                .hasMessageContaining("'ts_ns'")
                .hasMessageContaining("unsigned decimal");
        assertThatThrownBy(() -> debeziumSequenceNumber("9223372036854775808", "1", "0"))
                .hasMessageContaining("must not exceed 9223372036854775807");
        assertThatThrownBy(
                        () ->
                                SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                        null, DOCUMENTED_TIMESTAMP_NS, "1", "0"))
                .hasMessageContaining("non-empty 'connector'");
        assertThatThrownBy(
                        () ->
                                SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                        "", DOCUMENTED_TIMESTAMP_NS, "1", "0"))
                .hasMessageContaining("non-empty 'connector'");
        assertThatThrownBy(() -> debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "x", "0"))
                .hasMessageContaining("record sequence");
        assertThatThrownBy(() -> debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "-1"))
                .hasMessageContaining("'mod_number'");
        assertThatThrownBy(() -> debeziumSequenceNumber(DOCUMENTED_TIMESTAMP_NS, "1", "4294967296"))
                .hasMessageContaining("must not exceed 2147483647");
        assertThatThrownBy(
                        () ->
                                SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                        "mysql", DOCUMENTED_TIMESTAMP_NS, "1", "0"))
                .hasMessageContaining("'spanner'");
    }

    @Test
    void rejectsTypedCoordinatesOutsideTheirDomain() {
        assertThatThrownBy(() -> SpannerCdcSequenceNumberEncoder.sequenceNumber(-1L, "1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1970-01-01T00:00:00Z");
        assertThatThrownBy(
                        () ->
                                SpannerCdcSequenceNumberEncoder.sequenceNumber(
                                        Long.MIN_VALUE, "1", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SpannerCdcSequenceNumberEncoder.sequenceNumber(0L, "1", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mod number");
        assertThatThrownBy(() -> SpannerCdcSequenceNumberEncoder.sequenceNumber(0L, null, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static String debeziumSequenceNumber(String tsNs, String sequence, String modNumber) {
        return SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                "spanner", tsNs, sequence, modNumber);
    }
}
