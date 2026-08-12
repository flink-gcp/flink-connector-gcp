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
import org.apache.flink.table.api.ValidationException;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** SQL writes against the Pub/Sub emulator through the {@code pubsub} table connector. */
class PubSubTableSinkITCase extends PubSubTableTestBase {

    private static final Duration PULL_DEADLINE = Duration.ofSeconds(30);

    @Test
    void writesThePayloadTheAttributesAndTheOrderingKey() throws Exception {
        String name = "table-sink-metadata";
        SubscriptionDestination subscription = createTopicAndOrderedSubscription(name, 30);
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE events (\n"
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
                                "true",
                                "sink.parallelism",
                                "2"));

        tEnv.executeSql(
                        "INSERT INTO events VALUES"
                                + " ('a', 1, MAP['source', 'sql'], 'key-1'),"
                                + " ('b', 2, MAP['source', 'sql', 'kind', 'test'], 'key-2')")
                .await();

        List<PubsubMessage> messages = pullMessagesUntil(subscription, 2, PULL_DEADLINE);

        assertThat(messages).hasSize(2);
        assertThat(messages)
                .extracting(m -> m.getData().toStringUtf8())
                .containsExactlyInAnyOrder(
                        "{\"id\":\"a\",\"amount\":1}", "{\"id\":\"b\",\"amount\":2}");
        assertThat(messages)
                .extracting(PubsubMessage::getOrderingKey)
                .containsExactlyInAnyOrder("key-1", "key-2");

        PubsubMessage first =
                messages.stream()
                        .filter(m -> "key-1".equals(m.getOrderingKey()))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertThat(first.getAttributesMap()).containsExactly(entry("source", "sql"));
    }

    @Test
    void writesMessagesWithoutMetadataColumnsAtAll() throws Exception {
        String name = "table-sink-plain";
        SubscriptionDestination subscription = createTopicAndSubscription(name, 30);
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE plain (id STRING, amount INT) "
                        + withOptions("topic", name, "format", "json"));

        tEnv.executeSql("INSERT INTO plain VALUES ('a', 1)").await();

        List<PubsubMessage> messages = pullMessagesUntil(subscription, 1, PULL_DEADLINE);

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getData().toStringUtf8())
                .isEqualTo("{\"id\":\"a\",\"amount\":1}");
        assertThat(messages.get(0).getAttributesMap()).isEmpty();
        assertThat(messages.get(0).getOrderingKey()).isEmpty();
    }

    @Test
    void createsTheTopicWhenItDoesNotExist() throws Exception {
        // No createTopic call: the sink's create-if-needed default has to make the topic itself.
        String name = "table-sink-autocreate";
        assertThat(topicExists(name)).isFalse();
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE created (id STRING) " + withOptions("topic", name, "format", "json"));

        tEnv.executeSql("INSERT INTO created VALUES ('a')").await();

        assertThat(topicExists(name)).isTrue();
    }

    @Test
    void createsTheTopicWithTheConfiguredSettings() throws Exception {
        // SQL to service, end to end: the sink.auto-create.* options must survive the mapper, the
        // dynamic sink, the builder and the writer's repair, and read back off the created topic.
        String name = "table-sink-autocreate-settings";
        assertThat(topicExists(name)).isFalse();
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE configured (id STRING) "
                        + withOptions(
                                "topic",
                                name,
                                "format",
                                "json",
                                "sink.auto-create.message-retention",
                                "2 h",
                                "sink.auto-create.kms-key-name",
                                "projects/p/locations/l/keyRings/r/cryptoKeys/k",
                                "sink.auto-create.storage-policy.allowed-regions",
                                "us-central1",
                                "sink.auto-create.storage-policy.enforce-in-transit",
                                "true"));

        tEnv.executeSql("INSERT INTO configured VALUES ('a')").await();

        com.google.pubsub.v1.Topic created = describeTopic(name);
        assertThat(created.getMessageRetentionDuration().getSeconds())
                .isEqualTo(Duration.ofHours(2).getSeconds());
        assertThat(created.getKmsKeyName())
                .isEqualTo("projects/p/locations/l/keyRings/r/cryptoKeys/k");
        assertThat(created.getMessageStoragePolicy().getAllowedPersistenceRegionsList())
                .containsExactly("us-central1");
        assertThat(created.getMessageStoragePolicy().getEnforceInTransit()).isTrue();
    }

    @Test
    void refusesToCreateTheTopicUnderCreateNever() {
        String name = "table-sink-create-never";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE never (id STRING) "
                        + withOptions(
                                "topic",
                                name,
                                "format",
                                "json",
                                "sink.create-disposition",
                                "create-never"));

        // The writer's message names the Java constant, which is what a stack trace is read with.
        // await() returns once rather than looping because the TableEnvironment enables no
        // checkpointing, so Flink's restart strategy resolves to none.
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO never VALUES ('a')").await())
                .hasStackTraceContaining("CREATE_NEVER");
        assertThat(topicExists(name)).isFalse();
    }

    @Test
    void refusesAnOrderingKeyColumnWithoutOrderingEnabled() {
        String name = "table-sink-unordered";
        createTopic(name);
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE unordered (\n"
                        + "  id STRING,\n"
                        + "  okey STRING METADATA FROM 'ordering-key'\n"
                        + ") "
                        + withOptions("topic", name, "format", "json"));

        // The DDL is accepted; the failure lands at plan time, before any record is published.
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO unordered VALUES ('a', 'key-1')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.message-ordering.enabled");
    }

    @Test
    void refusesAnUpdatingQuery() {
        String name = "table-sink-updating";
        createTopic(name);
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE counts (id STRING, n BIGINT) "
                        + withOptions("topic", name, "format", "json"));

        // Pub/Sub cannot express a retraction, so an aggregation must be rejected at plan time
        // rather than publishing its -U rows as ordinary messages. The assertion names the table
        // rather than the planner's wording: the wording is Flink's and the weekly matrix builds
        // against an unreleased Flink, where a rephrasing upstream would turn this red for no
        // reason. What the connector controls -- ChangelogMode.insertOnly() -- is pinned in
        // PubSubDynamicSinkTest.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT INTO counts SELECT id, COUNT(*) FROM"
                                                + " (VALUES ('a'), ('a')) AS t(id) GROUP BY id"))
                .hasStackTraceContaining("counts");
    }
}
