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

package io.github.flink.gcp.connector.pubsub.sql;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Runs a SQL job through the shaded classes, against the Pub/Sub emulator.
 *
 * <p>The only test here that exercises relocation at <em>runtime</em>. {@link
 * PubSubSqlConnectorPackagingITCase} proves the jar has the right shape; this proves the shape
 * works — creating a Pub/Sub channel is what puts relocated gRPC and the deliberately unrelocated
 * {@code grpc-netty-shaded} transport together, and a mistake there is invisible to any jar-content
 * assertion.
 *
 * <p>The connector under test comes from the uber-jar, not from the reactor's classes: the module's
 * surefire configuration drops {@code flink-connector-gcp-pubsub} from the test classpath and adds
 * the shaded jar. {@link #theConnectorUnderTestComesFromTheShadedJar()} asserts that rather than
 * trusting it, because a regression there would leave this whole class passing against unshaded
 * code and proving nothing.
 *
 * <p>The harness drives the emulator with the <em>stock</em> Pub/Sub admin client while the
 * connector uses its relocated copy. That the two coexist on one classpath is not incidental — it
 * is the property an uber-jar exists to provide.
 *
 * <p>The container image and the stock clients come from the shared test-utils module ({@link
 * PubSubEmulatorContainers}, {@link PubSubTestClients}), which deals only in stock {@code
 * com.google.*} types — the connector module's harnesses cannot be reused here, because their
 * helpers touch production classes whose relocated and unrelocated forms would not type-check
 * against each other across this boundary (issue #27).
 */
@Testcontainers
@Timeout(180)
class PubSubSqlConnectorSmokeITCase {

    private static final String PROJECT = "it-project";

    private static final String FACTORY_CLASS =
            "io.github.flink.gcp.connector.pubsub.table.PubSubDynamicTableFactory";

    /** Comfortably inside the class timeout, so a shortfall fails the assertion instead. */
    private static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(60);

    @Container
    private static final PubSubEmulatorContainer EMULATOR = PubSubEmulatorContainers.newContainer();

    private static PubSubTestClients clients;

    @BeforeAll
    static void createClients() throws IOException {
        clients = PubSubTestClients.forEmulator(EMULATOR.getEmulatorEndpoint());
    }

    @AfterAll
    static void closeClients() {
        if (clients != null) {
            clients.close();
        }
    }

    @Test
    void theConnectorUnderTestComesFromTheShadedJar() throws Exception {
        Class<?> factory = Class.forName(FACTORY_CLASS);
        Path loadedFrom =
                Path.of(factory.getProtectionDomain().getCodeSource().getLocation().toURI());

        assertThat(loadedFrom)
                .as(
                        "the surefire classpath surgery in this module's pom must put the uber-jar"
                                + " in front of the reactor's unshaded classes, or every other"
                                + " assertion here is about the wrong code")
                .isEqualTo(ShadedJar.path().toAbsolutePath());
    }

    @Test
    void whatSqlWritesThroughTheShadedClassesIsWhatSqlReadsBack() throws Exception {
        String name = "sql-smoke";
        clients.topicAdmin().createTopic(TopicName.of(PROJECT, name));
        clients.subscriptionAdmin()
                .createSubscription(
                        SubscriptionName.of(PROJECT, name).toString(),
                        TopicName.of(PROJECT, name).toString(),
                        PushConfig.getDefaultInstance(),
                        60);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // The source acknowledges on checkpoint completion and fails the job if no checkpoint ever
        // arrives, so checkpointing is not optional. Restarts are off so a permanent failure fails
        // the test rather than looping until the class timeout.
        tEnv.getConfig()
                .set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(500))
                .set(RestartStrategyOptions.RESTART_STRATEGY, "none");

        tEnv.executeSql(
                "CREATE TABLE outbound (\n"
                        + "  id STRING,\n"
                        + "  amount INT,\n"
                        + "  attrs MAP<STRING, STRING> METADATA FROM 'attributes'\n"
                        + ") "
                        + withOptions("topic", name, "format", "json"));
        tEnv.executeSql(
                "CREATE TABLE inbound (\n"
                        + "  id STRING,\n"
                        + "  amount INT,\n"
                        + "  message_id STRING METADATA FROM 'message-id' VIRTUAL,\n"
                        + "  attrs MAP<STRING, STRING> METADATA FROM 'attributes' VIRTUAL\n"
                        + ") "
                        + withOptions("subscription", name, "format", "json"));

        // Bounded for the same reason as collect(): the no-argument await() has no timeout, so an
        // INSERT job that never finishes would hang to the class timeout.
        tEnv.executeSql(
                        "INSERT INTO outbound VALUES"
                                + " ('a', 1, MAP['source', 'sql']),"
                                + " ('b', 2, MAP['source', 'sql'])")
                .await(COLLECT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        List<Row> rows = collect(tEnv.executeSql("SELECT * FROM inbound"), 2);

        assertThat(rows)
                .extracting(row -> row.getField("id"), row -> row.getField("amount"))
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("a", 1),
                        org.assertj.core.groups.Tuple.tuple("b", 2));
        assertThat(rows)
                .allSatisfy(
                        row -> {
                            assertThat(row.getField("message_id")).asString().isNotEmpty();
                            assertThat(row.getField("attrs")).isEqualTo(Map.of("source", "sql"));
                        });
    }

    /**
     * Drains rows until {@code count} distinct ids have arrived, or fails after {@link
     * #COLLECT_TIMEOUT}.
     *
     * <p>Distinct because the transport is at-least-once, so counting total rows would let one
     * redelivery crowd out an original.
     *
     * <p>The timeout is <em>preemptive</em>, and that is the whole point. {@code hasNext()} on an
     * unbounded query blocks until a row arrives, so a deadline tested in the loop condition is
     * only reached between rows — it bounds a partial shortfall and does nothing at all in the case
     * CI actually produces, zero rows. That case would otherwise sit in {@code hasNext()} until the
     * class timeout and report an interrupt from inside Flink's fetcher rather than "expected 2
     * rows". The connector module's harness has the same loop and the same hole ({@code
     * PubSubTableTestBase.collect}); this is the fixed shape.
     */
    private static List<Row> collect(TableResult result, int count) {
        return assertTimeoutPreemptively(
                COLLECT_TIMEOUT,
                () -> {
                    Map<Object, Row> rows = new LinkedHashMap<>();
                    try (CloseableIterator<Row> iterator = result.collect()) {
                        while (rows.size() < count && iterator.hasNext()) {
                            Row row = iterator.next();
                            rows.putIfAbsent(row.getField("id"), row);
                        }
                    }
                    return new ArrayList<>(rows.values());
                },
                () ->
                        "timed out waiting for "
                                + count
                                + " distinct rows from the shaded connector");
    }

    /** Renders a {@code WITH} clause carrying the connector, project and emulator endpoint. */
    private static String withOptions(String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", "pubsub");
        options.put("project", PROJECT);
        options.put("emulator-endpoint", EMULATOR.getEmulatorEndpoint());
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return options.entrySet().stream()
                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                .collect(java.util.stream.Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }
}
