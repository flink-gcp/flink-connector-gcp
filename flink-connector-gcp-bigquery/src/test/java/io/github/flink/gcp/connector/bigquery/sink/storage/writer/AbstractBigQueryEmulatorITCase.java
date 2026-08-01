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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

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

    static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

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

    /** Creates a table in the emulator dataset with the given Storage-form schema. */
    static void createTable(String table, TableSchema schema) {
        restClient.create(
                TableInfo.newBuilder(
                                TableId.of(PROJECT, DATASET, table),
                                StandardTableDefinition.newBuilder()
                                        .setSchema(StorageSchemaConverter.toBigQuerySchema(schema))
                                        .build())
                        .build());
    }

    /** Returns the values of the {@code name} column of the given table, sorted. */
    static List<String> queryNames(String table) throws InterruptedException {
        List<String> names = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT name FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + "."
                                                + table
                                                + "` ORDER BY name")
                                .build())
                .iterateAll()
                .forEach((FieldValueList row) -> names.add(row.get(0).getStringValue()));
        return names;
    }
}
