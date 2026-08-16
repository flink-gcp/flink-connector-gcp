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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumPostgreSqlCdcSequenceNumberProviderTest {

    private final DebeziumPostgreSqlCdcSequenceNumberProvider provider =
            new DebeziumPostgreSqlCdcSequenceNumberProvider();

    @Test
    void encodesTheSameStrictPostgreSqlSourcePropertiesUsedByTheTableProfile() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "postgresql");
        properties.put("sequence", "[\"16\",\"17\"]");
        properties.put("lsn", "17");

        assertThat(provider.getSequenceNumber(properties))
                .isEqualTo("0000000000000010/0000000000000011");
    }

    @Test
    void rejectsASequenceShapeThatTheTableProfileRejects() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "postgresql");
        properties.put("sequence", "[null,2]");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two-element JSON array");
    }

    @Test
    void forwardsAContradictoryLsnToTheStrictEncoder() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "postgresql");
        properties.put("sequence", "[\"16\",\"17\"]");
        properties.put("lsn", "18");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'lsn' does not match");
    }
}
