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
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the TiCDC to BigQuery CDC sequence encoding contract directly. */
class TiCdcSequenceNumberEncoderTest {

    private static final String CLUSTER_ID = "test_cluster";

    @ParameterizedTest(name = "{0}")
    @MethodSource("tiCdcSourceShapes")
    void encodesTiCdcSourceShapes(String description, String commitTs, String expected) {
        assertThat(sequenceNumber(commitTs)).isEqualTo(expected);
    }

    private static Stream<Arguments> tiCdcSourceShapes() {
        return Stream.of(
                Arguments.of("the documented envelope's commit_ts", "1", "0000000000000001"),
                Arguments.of("a logical counter of zero", "449574614268116992", "063D35BACF7C0000"),
                Arguments.of(
                        "a logical counter within one millisecond",
                        "449574614268182531",
                        "063D35BACF7D0003"),
                Arguments.of("the next logical counter", "449574614268182532", "063D35BACF7D0004"),
                Arguments.of(
                        "the next physical millisecond", "449574614281027585", "063D35BAD0410001"));
    }

    @Test
    void aLargerCommitTsoAlwaysProducesALargerSequence() {
        String earlier = sequenceNumber("449574614268182531");
        String logicalSuccessor = sequenceNumber("449574614268182532");
        String physicalSuccessor = sequenceNumber("449574614281027585");

        assertThat(earlier).isLessThan(logicalSuccessor);
        assertThat(logicalSuccessor).isLessThan(physicalSuccessor);
    }

    @Test
    void encodesEveryUnsignedBoundaryInIncreasingOrder() {
        String smallest = sequenceNumber("1");
        String logicalCarry = sequenceNumber("262144");
        String signedMaximum = sequenceNumber("9223372036854775807");
        String unsignedHighBit = sequenceNumber("9223372036854775808");
        String unsignedMaximum = sequenceNumber("18446744073709551615");

        assertThat(smallest).isEqualTo("0000000000000001");
        assertThat(logicalCarry).isEqualTo("0000000000040000");
        assertThat(signedMaximum).isEqualTo("7FFFFFFFFFFFFFFF");
        assertThat(unsignedHighBit).isEqualTo("8000000000000000");
        assertThat(unsignedMaximum).isEqualTo("FFFFFFFFFFFFFFFF");
        assertThat(smallest).isLessThan(logicalCarry);
        assertThat(logicalCarry).isLessThan(signedMaximum);
        assertThat(signedMaximum).isLessThan(unsignedHighBit);
        assertThat(unsignedHighBit).isLessThan(unsignedMaximum);
    }

    @Test
    void anEventArrivingAfterALaterOneStillEncodesItsOwnCommitTso() {
        // Arrival order reversed: the later transaction is encoded first, and a replay of the
        // earlier one afterwards must still sort below it.
        String later = sequenceNumber("449574614281027585");
        String earlier = sequenceNumber("449574614268182531");

        assertThat(earlier).isLessThan(later);
        assertThat(sequenceNumber("449574614281027585")).isEqualTo(later);
        assertThat(sequenceNumber("449574614268182531")).isEqualTo(earlier);
    }

    @ParameterizedTest(name = "invalid commit_ts: {0}")
    @MethodSource("invalidCommitTimestamps")
    void rejectsMissingMalformedAndOverflowingCommitTimestamps(
            String description, String commitTs) {
        assertThatThrownBy(() -> sequenceNumber(commitTs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'commit_ts'");
    }

    private static Stream<Arguments> invalidCommitTimestamps() {
        return Stream.of(
                Arguments.of("missing", null),
                Arguments.of("empty", ""),
                Arguments.of("not a number", "not-a-tso"),
                Arguments.of("negative", "-1"),
                Arguments.of("fractional", "1.0"),
                Arguments.of("hexadecimal", "0x1"),
                Arguments.of("unsigned overflow", "18446744073709551616"));
    }

    /**
     * Zero is what the protocol's unused MySQL-inherited coordinates carry, so it reads as a
     * malformed envelope rather than as an unparsable value.
     */
    @Test
    void reportsAZeroCommitTsoAsAnImpossibleTimestampRatherThanAsAParseFailure() {
        assertThatThrownBy(() -> sequenceNumber("0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive timestamp oracle value");
    }

    @ParameterizedTest
    @MethodSource("unsupportedConnectors")
    void rejectsMissingAndUnsupportedConnectors(String connector) {
        assertThatThrownBy(
                        () ->
                                encoder()
                                        .sequenceNumber(
                                                connector,
                                                "false",
                                                "449574614268182531",
                                                CLUSTER_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector");
    }

    private static Stream<String> unsupportedConnectors() {
        return Stream.of(null, "", "ticdc", "TICDC", "mysql", "postgresql");
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "last", "incremental", "False"})
    void rejectsSnapshotStatesTheProtocolNeverEmits(String snapshot) {
        assertThatThrownBy(
                        () ->
                                encoder()
                                        .sequenceNumber(
                                                "TiCDC",
                                                snapshot,
                                                "449574614268182531",
                                                CLUSTER_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'snapshot'");
    }

    @Test
    void acceptsAnAbsentSnapshotState() {
        assertThat(encoder().sequenceNumber("TiCDC", null, "449574614268182531", CLUSTER_ID))
                .isEqualTo("063D35BACF7D0003");
    }

    @ParameterizedTest
    @MethodSource("foreignClusterIds")
    void rejectsAnEventFromAnotherCluster(String clusterId) {
        assertThatThrownBy(
                        () ->
                                encoder()
                                        .sequenceNumber(
                                                "TiCDC", "false", "449574614268182531", clusterId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(clusterId)
                .hasMessageContaining(CLUSTER_ID);
    }

    private static Stream<String> foreignClusterIds() {
        return Stream.of("other_cluster", "Test_cluster", "tidb_prod");
    }

    /**
     * A TiCDC release before v8.0.0 sends no {@code cluster_id} at all, which is a different
     * problem from an event of another cluster and reads as one.
     */
    @ParameterizedTest
    @NullAndEmptySource
    void reportsAnAbsentClusterIdAsAMissingFieldRatherThanAMismatch(String clusterId) {
        assertThatThrownBy(
                        () ->
                                encoder()
                                        .sequenceNumber(
                                                "TiCDC", "false", "449574614268182531", clusterId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty 'cluster_id'");
    }

    @Test
    void rejectsAnAbsentOrEmptyConfiguredClusterId() {
        assertThatThrownBy(() -> new TiCdcSequenceNumberEncoder(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clusterId");
        assertThatThrownBy(() -> new TiCdcSequenceNumberEncoder(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty cluster ID");
    }

    private static String sequenceNumber(String commitTs) {
        return encoder().sequenceNumber("TiCDC", "false", commitTs, CLUSTER_ID);
    }

    private static TiCdcSequenceNumberEncoder encoder() {
        return new TiCdcSequenceNumberEncoder(CLUSTER_ID);
    }
}
