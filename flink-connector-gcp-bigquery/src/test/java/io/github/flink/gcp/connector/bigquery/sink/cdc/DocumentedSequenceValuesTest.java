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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies every hexadecimal value the CDC documentation shows against the encoders. */
class DocumentedSequenceValuesTest {

    @Test
    void postgresql() {
        assertThat(
                        DebeziumPostgreSqlCdcSequenceNumberEncoder.sequenceNumber(
                                "postgresql", "[\"16\",\"17\"]", null))
                .isEqualTo("0000000000000010/0000000000000011");
        assertThat(
                        DebeziumPostgreSqlCdcSequenceNumberEncoder.sequenceNumber(
                                "postgresql", "[null,\"16\"]", null))
                .isEqualTo("0000000000000000/0000000000000010");
    }

    @Test
    void mysql() {
        String sid = "24bc7850-2c16-11e6-a073-0242ac110002";
        DebeziumMySqlCdcSequenceNumberEncoder encoder =
                new DebeziumMySqlCdcSequenceNumberEncoder(Collections.singletonList(sid));
        assertThat(encoder.sequenceNumber("mysql", "true", null, null, null))
                .isEqualTo("0000000000000000/0000000000000000/0000000000000000/0000000000000000");
        assertThat(encoder.sequenceNumber("mysql", "false", sid + ":16", "1081", "2"))
                .isEqualTo("0000000000000001/0000000000000010/0000000000000439/0000000000000002");
    }

    @Test
    void ticdc() {
        TiCdcSequenceNumberEncoder encoder = new TiCdcSequenceNumberEncoder("test_cluster");
        assertThat(encoder.sequenceNumber("TiCDC", "false", "449574614268182531", "test_cluster"))
                .isEqualTo("063D35BACF7D0003");
        assertThat(encoder.sequenceNumber("TiCDC", "false", "449574614268182532", "test_cluster"))
                .isEqualTo("063D35BACF7D0004");
        assertThat(encoder.sequenceNumber("TiCDC", "false", "449574614281027585", "test_cluster"))
                .isEqualTo("063D35BAD0410001");
    }

    @Test
    void spanner() {
        assertThat(
                        SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                "spanner", "1670955531785000000", "1", "0"))
                .isEqualTo("17306D33FB84D440/0000000000000001/0000000000000000");
        assertThat(
                        SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                "spanner", "1670955531785000000", "2", "0"))
                .isEqualTo("17306D33FB84D440/0000000000000002/0000000000000000");
        assertThat(
                        SpannerCdcSequenceNumberEncoder.debeziumSequenceNumber(
                                "spanner", "1670955531785000000", "1", "1"))
                .isEqualTo("17306D33FB84D440/0000000000000001/0000000000000001");
        assertThat(java.time.Instant.parse("2022-12-13T18:18:51.785Z").toEpochMilli())
                .isEqualTo(1670955531785L);
    }

    @Test
    void bigQueryComparesShorterSectionsAsSmallerNumbers() {
        assertThat(Long.parseUnsignedLong("B", 16)).isLessThan(Long.parseUnsignedLong("ABC", 16));
    }
}
