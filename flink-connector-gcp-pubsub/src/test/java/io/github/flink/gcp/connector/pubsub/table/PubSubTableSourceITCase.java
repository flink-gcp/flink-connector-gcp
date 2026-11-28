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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.Drains;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
        // Ordered under PER_KEY only so that one subtask reads the subscription and emits every row
        // down one channel. The emulator does not dispatch an ordered subscription's messages in
        // order, so the test orders them itself: good-2 is published only once the bad message has
        // been dropped.
        createTopicAndOrderedSubscription(name, 30);
        publishOrdered(name, "k", "{\"id\":\"good-1\"}", "not json at all");

        MessagesDroppedReporter.reset();
        TableEnvironment tEnv =
                checkpointingTableEnvironment(MessagesDroppedReporter.configuration());
        tEnv.executeSql(
                "CREATE TABLE lenient (id STRING, mid STRING METADATA FROM 'message-id' VIRTUAL) "
                        + withOptions(
                                "subscription",
                                name,
                                "format",
                                "json",
                                "scan.ordering-mode",
                                "per-key",
                                "scan.deserialization-failure-policy",
                                "drop"));
        TableResult result = tEnv.executeSql("SELECT id, mid FROM lenient");
        CompletableFuture<JobExecutionResult> ended =
                result.getJobClient().orElseThrow().getJobExecutionResult();

        // Opened first, so that closing it cancels the job however the test ends.
        try (CloseableIterator<Row> iterator = result.collect()) {
            Awaits.await(
                    "the undecodable message to be dropped, or the job to end",
                    COLLECT_TIMEOUT,
                    () -> MessagesDroppedReporter.dropped() >= 1 || ended.isDone());
            // Under 'fail' the bad message fails the job instead, and nothing counts it.
            assertThat(ended)
                    .as(
                            "the job, which ended with %s",
                            ended.handle(
                                            (ignored, failure) ->
                                                    failure == null
                                                            ? "success"
                                                            : ExceptionUtils.stringifyException(
                                                                    failure))
                                    .getNow("nothing"))
                    .isNotDone();
            publishOrdered(name, "k", "{\"id\":\"good-2\"}");

            // Distinct by message id, so a redelivery collapses and a row of the bad message's own
            // does not. Such a row would have been emitted before the drop was counted, so ahead
            // of good-2 on the same channel, and taken one of the two places good-2 needs.
            List<Row> rows =
                    Drains.drainDistinct(iterator, 2, COLLECT_TIMEOUT, r -> r.getField("mid"));

            assertThat(rows)
                    .extracting(r -> r.getField("id"))
                    .containsExactlyInAnyOrder("good-1", "good-2");
        }
    }
}
