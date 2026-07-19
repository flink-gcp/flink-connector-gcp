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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Shared harness for integration tests against the BigQuery emulator (goccy/bigquery-emulator): the
 * container, a REST client pointed at it, the Storage Write API gRPC endpoint, and a no-op {@link
 * SinkWriter.Context}. Use together with {@link EmulatorAppenderFactory}.
 */
@Testcontainers
@Timeout(180)
abstract class AbstractBigQueryEmulatorITCase {

    static final String PROJECT = "it-project";
    static final String DATASET = "it_dataset";
    private static final int REST_PORT = 9050;
    private static final int GRPC_PORT = 9060;

    static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    @Container
    private static final GenericContainer<?> EMULATOR =
            new GenericContainer<>("ghcr.io/goccy/bigquery-emulator:0.8.1")
                    .withCommand("--project=" + PROJECT, "--dataset=" + DATASET)
                    .withExposedPorts(REST_PORT, GRPC_PORT)
                    .waitingFor(Wait.forListeningPorts(REST_PORT, GRPC_PORT));

    static BigQuery restClient;

    @BeforeAll
    static void createRestClient() {
        restClient =
                BigQueryOptions.newBuilder()
                        .setHost(
                                "http://"
                                        + EMULATOR.getHost()
                                        + ":"
                                        + EMULATOR.getMappedPort(REST_PORT))
                        .setProjectId(PROJECT)
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService();
    }

    static String grpcEndpoint() {
        return EMULATOR.getHost() + ":" + EMULATOR.getMappedPort(GRPC_PORT);
    }
}
