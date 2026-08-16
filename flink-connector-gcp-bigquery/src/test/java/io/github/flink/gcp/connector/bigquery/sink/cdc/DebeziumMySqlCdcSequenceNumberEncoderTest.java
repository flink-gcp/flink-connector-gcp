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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Debezium MySQL GTID to BigQuery CDC sequence contract directly. */
class DebeziumMySqlCdcSequenceNumberEncoderTest {

    private static final String FIRST_SID = "24bc7850-2c16-11e6-a073-0242ac110002";
    private static final String SECOND_SID = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    @ParameterizedTest(name = "{0}")
    @MethodSource("mysqlSourceShapes")
    void encodesOfficialMySqlSourceShapes(
            String description,
            String snapshot,
            String gtid,
            String position,
            String row,
            String expected) {
        assertThat(sequence(singleSid(), snapshot, gtid, position, row)).isEqualTo(expected);
    }

    private static Stream<Arguments> mysqlSourceShapes() {
        return Stream.of(
                Arguments.of(
                        "snapshot row",
                        "true",
                        null,
                        "0",
                        "0",
                        "0000000000000000/0000000000000000/0000000000000000/0000000000000000"),
                Arguments.of(
                        "last snapshot row",
                        "last",
                        FIRST_SID + ":15",
                        "1000",
                        "0",
                        "0000000000000000/0000000000000000/0000000000000000/0000000000000000"),
                Arguments.of(
                        "streaming create",
                        "false",
                        FIRST_SID + ":16",
                        "1081",
                        "0",
                        "0000000000000001/0000000000000010/0000000000000439/0000000000000000"),
                Arguments.of(
                        "streaming update",
                        "false",
                        FIRST_SID + ":17",
                        "1090",
                        "0",
                        "0000000000000001/0000000000000011/0000000000000442/0000000000000000"),
                Arguments.of(
                        "streaming delete",
                        null,
                        FIRST_SID + ":18",
                        "1100",
                        "0",
                        "0000000000000001/0000000000000012/000000000000044C/0000000000000000"));
    }

    @Test
    void ordersRowsWithinOneBinlogEventAndReplaysDeterministically() {
        String first = sequence(singleSid(), "false", FIRST_SID + ":20", "1200", "0");
        String second = sequence(singleSid(), "false", FIRST_SID + ":20", "1200", "1");

        assertThat(second).isGreaterThan(first);
        assertThat(sequence(singleSid(), "false", FIRST_SID + ":20", "1200", "1"))
                .isEqualTo(second);
    }

    @Test
    void anAppendedFailoverSidOutranksEveryCoordinateFromAnEarlierSid() {
        List<String> failoverOrder = Arrays.asList(FIRST_SID, SECOND_SID);
        String lastPossibleFirstSid =
                sequence(
                        failoverOrder,
                        "false",
                        FIRST_SID + ":18446744073709551615",
                        "18446744073709551615",
                        "18446744073709551615");
        String firstSecondSid = sequence(failoverOrder, "false", SECOND_SID + ":0", "0", "0");

        assertThat(firstSecondSid).isGreaterThan(lastPossibleFirstSid);
        assertThat(sequence(failoverOrder, "false", FIRST_SID + ":1", "2", "3"))
                .isEqualTo(sequence(singleSid(), "false", FIRST_SID + ":1", "2", "3"));
    }

    @Test
    void editingOrReorderingExistingEpochsChangesPreviouslyAssignedSequences() {
        String before =
                sequence(Arrays.asList(FIRST_SID, SECOND_SID), "false", FIRST_SID + ":1", "2", "3");
        String reordered =
                sequence(Arrays.asList(SECOND_SID, FIRST_SID), "false", FIRST_SID + ":1", "2", "3");

        assertThat(reordered).isNotEqualTo(before).isGreaterThan(before);
    }

    @Test
    void acceptsCanonicalUuidCaseAndEveryUnsignedCoordinateBoundary() {
        String upperSid = FIRST_SID.toUpperCase(java.util.Locale.ROOT);
        assertThat(
                        sequence(
                                Collections.singletonList(upperSid),
                                "false",
                                upperSid + ":18446744073709551615",
                                "18446744073709551615",
                                "18446744073709551615"))
                .isEqualTo("0000000000000001/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF");
    }

    @ParameterizedTest(name = "invalid GTID: {0}")
    @MethodSource("invalidGtids")
    void rejectsMissingTaggedMultiSourceAndMalformedGtids(String description, String gtid) {
        assertThatThrownBy(() -> sequence(singleSid(), "false", gtid, "1", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Debezium MySQL");
    }

    private static Stream<Arguments> invalidGtids() {
        return Stream.of(
                Arguments.of("missing", null),
                Arguments.of("tagged", FIRST_SID + ":blue:1"),
                Arguments.of("multiple SIDs", FIRST_SID + ":1," + SECOND_SID + ":2"),
                Arguments.of("malformed UUID", "not-a-uuid:1"),
                Arguments.of("missing transaction", FIRST_SID + ":"),
                Arguments.of("negative transaction", FIRST_SID + ":-1"),
                Arguments.of("transaction overflow", FIRST_SID + ":18446744073709551616"));
    }

    @ParameterizedTest(name = "invalid {0}: {1}")
    @MethodSource("invalidCoordinates")
    void rejectsMissingOrMalformedBinlogCoordinates(String field, String value) {
        String position = "pos".equals(field) ? value : "1";
        String row = "row".equals(field) ? value : "0";
        assertThatThrownBy(() -> sequence(singleSid(), "false", FIRST_SID + ":1", position, row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'" + field + "'")
                .hasMessageContaining("unsigned 64-bit decimal");
    }

    private static Stream<Arguments> invalidCoordinates() {
        return Stream.of(
                Arguments.of("pos", null),
                Arguments.of("pos", "-1"),
                Arguments.of("pos", "not-a-position"),
                Arguments.of("pos", "18446744073709551616"),
                Arguments.of("row", null),
                Arguments.of("row", "-1"),
                Arguments.of("row", "not-a-row"),
                Arguments.of("row", "18446744073709551616"));
    }

    @Test
    void rejectsUnsupportedSnapshotAndConnectorValues() {
        assertThatThrownBy(() -> sequence(singleSid(), "incremental", FIRST_SID + ":1", "1", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incremental snapshots");
        assertThatThrownBy(() -> sequence(singleSid(), "unexpected", FIRST_SID + ":1", "1", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'snapshot'");
        assertThatThrownBy(
                        () ->
                                new DebeziumMySqlCdcSequenceNumberEncoder(singleSid())
                                        .sequenceNumber(
                                                "postgresql", "false", FIRST_SID + ":1", "1", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connector 'mysql'");
    }

    @Test
    void rejectsUnknownInvalidAndDuplicateSourceUuids() {
        assertThatThrownBy(() -> sequence(singleSid(), "false", SECOND_SID + ":1", "1", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown source UUID");
        assertThatThrownBy(() -> new DebeziumMySqlCdcSequenceNumberEncoder(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one source UUID");
        assertThatThrownBy(
                        () ->
                                new DebeziumMySqlCdcSequenceNumberEncoder(
                                        Collections.singletonList("not-a-uuid")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical UUID");
        assertThatThrownBy(
                        () ->
                                new DebeziumMySqlCdcSequenceNumberEncoder(
                                        Arrays.asList(
                                                FIRST_SID,
                                                FIRST_SID.toUpperCase(java.util.Locale.ROOT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate UUID");
    }

    @Test
    void rejectsAnEpochPastTheUnsignedMaximum() {
        assertThatThrownBy(() -> DebeziumMySqlCdcSequenceNumberEncoder.nextEpoch(-1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch overflow");
    }

    private static List<String> singleSid() {
        return Collections.singletonList(FIRST_SID);
    }

    private static String sequence(
            List<String> sourceUuids, String snapshot, String gtid, String position, String row) {
        return new DebeziumMySqlCdcSequenceNumberEncoder(sourceUuids)
                .sequenceNumber("mysql", snapshot, gtid, position, row);
    }
}
