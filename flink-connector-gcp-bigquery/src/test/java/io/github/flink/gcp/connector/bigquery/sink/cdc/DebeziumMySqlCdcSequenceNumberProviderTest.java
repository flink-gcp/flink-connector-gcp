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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumMySqlCdcSequenceNumberProviderTest {

    private static final String SID = "24bc7850-2c16-11e6-a073-0242ac110002";

    private final DebeziumMySqlCdcSequenceNumberProvider provider =
            new DebeziumMySqlCdcSequenceNumberProvider(Collections.singletonList(SID));

    @Test
    void encodesTheSameStrictMySqlSourcePropertiesUsedByTheTableProfile() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "mysql");
        properties.put("snapshot", "false");
        properties.put("gtid", SID + ":16");
        properties.put("pos", "1081");
        properties.put("row", "2");

        assertThat(provider.getSequenceNumber(properties))
                .isEqualTo("0000000000000001/0000000000000010/0000000000000439/0000000000000002");
    }

    @Test
    void rejectsAGtidShapeThatTheTableProfileRejects() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "mysql");
        properties.put("snapshot", "false");
        properties.put("gtid", SID + ":tag:16");
        properties.put("pos", "1081");
        properties.put("row", "0");

        assertThatThrownBy(() -> provider.getSequenceNumber(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("untagged UUID:transaction_id");
    }

    @Test
    void rejectsNullSourceProperties() {
        assertThatThrownBy(() -> provider.getSequenceNumber(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sourceProperties must not be null");
    }
}
