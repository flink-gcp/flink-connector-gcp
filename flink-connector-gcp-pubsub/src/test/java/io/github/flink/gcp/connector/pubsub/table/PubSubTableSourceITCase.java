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

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL reads against the Pub/Sub emulator through the {@code pubsub} table connector. */
class PubSubTableSourceITCase extends PubSubTableTestBase {

    @Test
    void readsMessagesPublishedOutsideFlink() throws Exception {
        String name = "table-source-basic";
        createTopicAndSubscription(name, 30);
        publish(name, "{\"id\":\"a\",\"amount\":1}", "{\"id\":\"b\",\"amount\":2}");

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE events (id STRING, amount INT) "
                        + withOptions("subscription", name, "format", "json"));

        List<Row> rows =
                collect(tEnv.executeSql("SELECT id, amount FROM events"), 2, r -> r.getField("id"));

        assertThat(rows).extracting(r -> r.getField("id")).containsAll(Arrays.asList("a", "b"));
        assertThat(rows).extracting(r -> r.getField("amount")).containsAll(Arrays.asList(1, 2));
    }

    @Test
    void tellsSeveralSubscriptionsApartThroughTheSubscriptionColumn() throws Exception {
        String first = "table-source-multi-a";
        String second = "table-source-multi-b";
        createTopicAndSubscription(first, 30);
        createTopicAndSubscription(second, 30);
        publish(first, "{\"id\":\"from-a\"}");
        publish(second, "{\"id\":\"from-b\"}");

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE merged (\n"
                        + "  id STRING,\n"
                        + "  sub STRING METADATA FROM 'subscription' VIRTUAL\n"
                        + ") "
                        + withOptions("subscription", first + ";" + second, "format", "json"));

        List<Row> rows =
                collect(tEnv.executeSql("SELECT id, sub FROM merged"), 2, r -> r.getField("id"));

        assertThat(rows)
                .extracting(r -> r.getField("id") + " @ " + r.getField("sub"))
                .containsAll(
                        Arrays.asList(
                                "from-a @ projects/" + PROJECT + "/subscriptions/" + first,
                                "from-b @ projects/" + PROJECT + "/subscriptions/" + second));
    }

    @Test
    void dropsAMessageTheFormatCannotDecodeUnderTheDropPolicy() throws Exception {
        String name = "table-source-drop";
        createTopicAndOrderedSubscription(name, 30);
        // Ordered, so the undecodable message is guaranteed to reach the reader *before* the row
        // after it. Without that, the SDK's receiver callbacks arrive out of order — which this
        // module has already recorded against the emulator — and the test would pass under the
        // 'fail' policy too, whenever the bad message happened to be buffered last.
        publishOrdered(name, "k", "{\"id\":\"good-1\"}", "not json at all", "{\"id\":\"good-2\"}");

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE lenient (id STRING) "
                        + withOptions(
                                "subscription",
                                name,
                                "format",
                                "json",
                                "scan.ordering-mode",
                                "per-key",
                                "scan.deserialization-failure-policy",
                                "drop"));

        List<Row> rows =
                collect(tEnv.executeSql("SELECT id FROM lenient"), 2, r -> r.getField("id"));

        // good-2 arriving proves the bad message did not fail the job: under 'fail' the exception
        // is thrown from emitRecord before good-2 is ever emitted.
        assertThat(rows)
                .extracting(r -> r.getField("id"))
                .containsAll(Arrays.asList("good-1", "good-2"));
        // And nothing else came through, so the dropped message was acknowledged rather than
        // redelivered as a third row.
        assertThat(rows).hasSize(2);
    }
}
