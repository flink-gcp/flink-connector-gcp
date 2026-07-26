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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The acceptance criterion of the Pub/Sub Table API work: one job writes attributes and an ordering
 * key through SQL, another reads the payload and all five metadata columns back.
 *
 * <p>Both tables sit on the same topic and subscription, so what the sink produced is exactly what
 * the source consumes — nothing here is asserted against a hand-built message.
 */
class PubSubTableRoundTripITCase extends PubSubTableTestBase {

    @Test
    void whatSqlWritesIsWhatSqlReadsBack() throws Exception {
        String name = "table-round-trip";
        createTopicAndOrderedSubscription(name, 30);
        Instant before = Instant.now().minusSeconds(5);

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE outbound (\n"
                        + "  id STRING,\n"
                        + "  amount INT,\n"
                        + "  attrs MAP<STRING, STRING> METADATA FROM 'attributes',\n"
                        + "  okey STRING METADATA FROM 'ordering-key'\n"
                        + ") "
                        + withOptions(
                                "topic",
                                name,
                                "format",
                                "json",
                                "sink.message-ordering.enabled",
                                "true"));
        tEnv.executeSql(
                "CREATE TABLE inbound (\n"
                        + "  id STRING,\n"
                        + "  amount INT,\n"
                        + "  message_id STRING METADATA FROM 'message-id' VIRTUAL,\n"
                        + "  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,\n"
                        + "  attrs MAP<STRING, STRING> METADATA FROM 'attributes' VIRTUAL,\n"
                        + "  okey STRING METADATA FROM 'ordering-key' VIRTUAL,\n"
                        + "  sub STRING METADATA FROM 'subscription' VIRTUAL\n"
                        + ") "
                        + withOptions("subscription", name, "format", "json"));

        tEnv.executeSql(
                        "INSERT INTO outbound VALUES"
                                + " ('a', 1, MAP['source', 'sql'], 'key-1'),"
                                + " ('b', 2, MAP['source', 'sql'], 'key-2')")
                .await();

        List<Row> rows =
                collect(
                        tEnv.executeSql(
                                "SELECT id, amount, message_id, publish_time, attrs, okey, sub"
                                        + " FROM inbound"),
                        2,
                        r -> r.getField("id"));

        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getField("id")).containsAll(Arrays.asList("a", "b"));
        assertThat(rows).extracting(r -> r.getField("amount")).containsAll(Arrays.asList(1, 2));
        assertThat(rows)
                .extracting(r -> r.getField("okey"))
                .containsAll(Arrays.asList("key-1", "key-2"));
        // Distinct ids, so a metadata row shared across messages would show up here.
        assertThat(rows).extracting(r -> r.getField("message_id")).doesNotHaveDuplicates();

        for (Row row : rows) {
            assertThat((String) row.getField("message_id")).isNotBlank();
            // Loosely bounded on purpose: the stamp comes from the emulator container's clock and
            // `before` from the host's, so a tight window would flake on VM drift. That the field
            // read really is the publish time — and not the wall clock — is pinned exactly in
            // RowDataDeserializationSchemaTest against a hand-built stamp.
            assertThat((Instant) row.getField("publish_time")).isAfter(before);
            @SuppressWarnings("unchecked")
            Map<String, String> attributes = (Map<String, String>) row.getField("attrs");
            assertThat(attributes).containsExactly(entry("source", "sql"));
            assertThat(row.getField("sub"))
                    .isEqualTo("projects/" + PROJECT + "/subscriptions/" + name);
        }
    }
}
