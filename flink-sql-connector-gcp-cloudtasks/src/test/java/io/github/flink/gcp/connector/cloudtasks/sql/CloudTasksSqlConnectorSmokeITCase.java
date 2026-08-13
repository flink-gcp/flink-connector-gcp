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

package io.github.flink.gcp.connector.cloudtasks.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.ListTasksRequest;
import com.google.cloud.tasks.v2.LocationName;
import com.google.cloud.tasks.v2.Queue;
import com.google.cloud.tasks.v2.QueueName;
import com.google.cloud.tasks.v2.RateLimits;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a SQL insert through the shaded classes against the Cloud Tasks emulator.
 *
 * <p>The module's integration-test classpath excludes the plain connector and adds the uber-jar.
 * {@link #theConnectorUnderTestComesFromTheShadedJar()} proves that boundary. The connector opens
 * its relocated Cloud Tasks client while this harness creates the queue and inspects the task with
 * the stock client that arrives transitively from the connector. Their coexistence, the built-in
 * {@code form-urlencoded} factory, and the production {@code cloud-tasks} factory are all exercised
 * by the same insert.
 */
@Testcontainers
@Timeout(180)
class CloudTasksSqlConnectorSmokeITCase extends AbstractSqlConnectorSmokeITCase {

    private static final String PROJECT = "it-project";
    private static final String LOCATION = "us-central1";
    private static final String QUEUE = "sql-smoke";
    private static final int EMULATOR_PORT = 8123;

    @Container
    private static final GenericContainer<?> EMULATOR =
            new GenericContainer<>("ghcr.io/aertje/cloud-tasks-emulator:1.2.0")
                    .withCommand("-host", "0.0.0.0", "-port", String.valueOf(EMULATOR_PORT))
                    .withExposedPorts(EMULATOR_PORT)
                    .waitingFor(Wait.forListeningPorts(EMULATOR_PORT));

    private static ManagedChannel channel;
    private static CloudTasksClient client;

    @BeforeAll
    static void createClientAndPausedQueue() throws IOException {
        channel = ManagedChannelBuilder.forTarget(emulatorEndpoint()).usePlaintext().build();
        client =
                CloudTasksClient.create(
                        CloudTasksSettings.newBuilder()
                                .setTransportChannelProvider(
                                        FixedTransportChannelProvider.create(
                                                GrpcTransportChannel.create(channel)))
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
        client.createQueue(
                LocationName.of(PROJECT, LOCATION),
                Queue.newBuilder()
                        .setName(QueueName.of(PROJECT, LOCATION, QUEUE).toString())
                        .setRateLimits(RateLimits.newBuilder().setMaxConcurrentDispatches(1))
                        .build());
        client.pauseQueue(QueueName.of(PROJECT, LOCATION, QUEUE));
    }

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Override
    protected ShadedJar shadedJar() {
        return UberJar.SHADED;
    }

    @Override
    protected String factoryClass() {
        return UberJar.FACTORY_CLASS;
    }

    @Test
    void sqlCreatesTheFormEncodedTaskThroughTheShadedFactories() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                "CREATE TABLE tasks (\n"
                        + "  customer_name STRING,\n"
                        + "  tags ARRAY<STRING>,\n"
                        + "  empty_value STRING,\n"
                        + "  omitted_value STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'cloud-tasks',\n"
                        + "  'project' = '"
                        + PROJECT
                        + "',\n"
                        + "  'location' = '"
                        + LOCATION
                        + "',\n"
                        + "  'queue' = '"
                        + QUEUE
                        + "',\n"
                        + "  'format' = 'form-urlencoded',\n"
                        + "  'http.url' = 'https://example.invalid/tasks',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO tasks VALUES ('東京 +&=', ARRAY['one two', 'a+b'], '',"
                                + " CAST(NULL AS STRING))")
                .await(60, TimeUnit.SECONDS);

        List<Task> tasks = new ArrayList<>();
        client.listTasks(
                        ListTasksRequest.newBuilder()
                                .setParent(QueueName.of(PROJECT, LOCATION, QUEUE).toString())
                                .setResponseView(Task.View.FULL)
                                .build())
                .iterateAll()
                .forEach(tasks::add);
        String expected =
                "customer_name=%E6%9D%B1%E4%BA%AC+%2B%26%3D"
                        + "&tags=one+two&tags=a%2Bb&empty_value=";

        assertThat(tasks).singleElement();
        Task task = tasks.get(0);
        assertThat(task.getHttpRequest().getUrl()).isEqualTo("https://example.invalid/tasks");
        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.POST);
        assertThat(task.getHttpRequest().getHeaders())
                .containsEntry("Content-Type", "application/x-www-form-urlencoded");
        assertThat(task.getHttpRequest().getBody().toByteArray())
                .containsExactly(expected.getBytes(StandardCharsets.UTF_8));
    }

    private static String emulatorEndpoint() {
        return EMULATOR.getHost() + ":" + EMULATOR.getMappedPort(EMULATOR_PORT);
    }
}
