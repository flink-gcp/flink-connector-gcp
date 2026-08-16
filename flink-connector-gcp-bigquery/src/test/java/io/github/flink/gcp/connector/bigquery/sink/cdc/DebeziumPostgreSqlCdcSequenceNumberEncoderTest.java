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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Debezium PostgreSQL to BigQuery CDC sequence encoding contract directly. */
class DebeziumPostgreSqlCdcSequenceNumberEncoderTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("postgresqlSourceShapes")
    void encodesPostgreSqlSourceShapes(
            String description, String sequence, String lsn, String expected) {
        assertThat(sequenceNumber(sequence, lsn)).isEqualTo(expected);
    }

    private static Stream<Arguments> postgresqlSourceShapes() {
        return Stream.of(
                Arguments.of(
                        "snapshot", "[null,\"16\"]", "16", "0000000000000000/0000000000000010"),
                Arguments.of(
                        "streaming create",
                        "[\"16\",\"17\"]",
                        "17",
                        "0000000000000010/0000000000000011"),
                Arguments.of(
                        "streaming update",
                        "[\"17\",\"18\"]",
                        "18",
                        "0000000000000011/0000000000000012"),
                Arguments.of(
                        "streaming delete",
                        "[\"18\",\"19\"]",
                        "19",
                        "0000000000000012/0000000000000013"),
                Arguments.of(
                        "primary-key change delete",
                        "[\"19\",\"20\"]",
                        "20",
                        "0000000000000013/0000000000000014"),
                Arguments.of(
                        "primary-key change create",
                        "[\"19\",\"21\"]",
                        "21",
                        "0000000000000013/0000000000000015"));
    }

    @Test
    void replayAndContinuousSlotFailoverKeepSequenceOrdering() {
        String beforeFailover = sequenceNumber("[\"16\",\"32\"]", "32");
        String afterFailover = sequenceNumber("[\"32\",\"33\"]", "33");

        assertThat(sequenceNumber("[\"16\",\"32\"]", "32")).isEqualTo(beforeFailover);
        assertThat(afterFailover).isGreaterThan(beforeFailover);
    }

    @Test
    void encodesEveryUnsignedBoundaryInIncreasingOrder() {
        String zero = sequenceForCurrent("0");
        String signedMaximum = sequenceForCurrent("9223372036854775807");
        String unsignedHighBit = sequenceForCurrent("-9223372036854775808");
        String unsignedMaximum = sequenceForCurrent("-1");

        assertThat(zero).isEqualTo("0000000000000000/0000000000000000");
        assertThat(signedMaximum).isEqualTo("0000000000000000/7FFFFFFFFFFFFFFF");
        assertThat(unsignedHighBit).isEqualTo("0000000000000000/8000000000000000");
        assertThat(unsignedMaximum).isEqualTo("0000000000000000/FFFFFFFFFFFFFFFF");
        assertThat(zero).isLessThan(signedMaximum);
        assertThat(signedMaximum).isLessThan(unsignedHighBit);
        assertThat(unsignedHighBit).isLessThan(unsignedMaximum);
    }

    @Test
    void acceptsEquivalentSignedAndUnsignedHighBitRepresentations() {
        assertThat(sequenceForCurrent("9223372036854775808"))
                .isEqualTo(sequenceForCurrent("-9223372036854775808"));
        assertThat(sequenceForCurrent("18446744073709551615")).isEqualTo(sequenceForCurrent("-1"));
        assertThat(sequenceForPositions("9223372036854775808", "0"))
                .isEqualTo(sequenceForPositions("-9223372036854775808", "0"))
                .isEqualTo("8000000000000000/0000000000000000");
        assertThat(sequenceForPositions("18446744073709551615", "0"))
                .isEqualTo(sequenceForPositions("-1", "0"))
                .isEqualTo("FFFFFFFFFFFFFFFF/0000000000000000");
        assertThat(sequenceForCurrentAndLsn("-9223372036854775808", "9223372036854775808"))
                .isEqualTo(sequenceForCurrent("-9223372036854775808"));
        assertThat(sequenceForCurrentAndLsn("18446744073709551615", "-1"))
                .isEqualTo(sequenceForCurrent("-1"));
    }

    @ParameterizedTest(name = "malformed sequence: {0}")
    @MethodSource("malformedPostgreSqlSequences")
    void rejectsMissingOrMalformedPostgreSqlSequences(String description, String sequence) {
        assertThatThrownBy(() -> sequenceNumber(sequence, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PostgreSQL");
    }

    private static Stream<Arguments> malformedPostgreSqlSequences() {
        return Stream.of(
                Arguments.of("missing", null),
                Arguments.of("not JSON", "not-json"),
                Arguments.of("not an array", "{}"),
                Arguments.of("empty array", "[]"),
                Arguments.of("one element", "[\"1\"]"),
                Arguments.of("three elements", "[\"1\",\"2\",\"3\"]"),
                Arguments.of("numeric last committed LSN", "[1,\"2\"]"),
                Arguments.of("numeric current LSN", "[\"1\",2]"),
                Arguments.of("null current LSN", "[\"1\",null]"),
                Arguments.of("trailing JSON", "[\"1\",\"2\"] true"),
                Arguments.of("unsigned overflow", "[\"1\",\"18446744073709551616\"]"),
                Arguments.of("signed underflow", "[\"1\",\"-9223372036854775809\"]"));
    }

    @Test
    void validatesTheOptionalLsnAgainstTheCurrentSequencePosition() {
        assertThat(sequenceNumber("[null,\"32\"]", null))
                .isEqualTo("0000000000000000/0000000000000020");

        assertThatThrownBy(() -> sequenceNumber("[null,\"32\"]", "33"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'lsn' does not match");
        assertThatThrownBy(() -> sequenceNumber("[null,\"32\"]", "not-an-lsn"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'lsn' must be a signed or unsigned 64-bit decimal value");
    }

    @ParameterizedTest
    @MethodSource("unsupportedConnectors")
    void rejectsMissingOrUnsupportedConnectors(String connector) {
        assertThatThrownBy(
                        () ->
                                DebeziumPostgreSqlCdcSequenceNumberEncoder.sequenceNumber(
                                        connector, "[\"1\",\"2\"]", "2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector");
    }

    private static Stream<String> unsupportedConnectors() {
        return Stream.of(null, "", "PostgreSQL", "mysql");
    }

    private static String sequenceForCurrent(String current) {
        return sequenceForCurrentAndLsn(current, current);
    }

    private static String sequenceForCurrentAndLsn(String current, String lsn) {
        return sequenceForPositions(null, current, lsn);
    }

    private static String sequenceForPositions(String lastCommitted, String current) {
        return sequenceForPositions(lastCommitted, current, current);
    }

    private static String sequenceForPositions(String lastCommitted, String current, String lsn) {
        return sequenceNumber(
                "["
                        + (lastCommitted == null ? "null" : "\"" + lastCommitted + "\"")
                        + ",\""
                        + current
                        + "\"]",
                lsn);
    }

    private static String sequenceNumber(String sequence, String lsn) {
        return DebeziumPostgreSqlCdcSequenceNumberEncoder.sequenceNumber(
                "postgresql", sequence, lsn);
    }
}
