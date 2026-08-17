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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.bigquery.BigQueryEmulatorContainers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared harness for integration tests against the BigQuery emulator (goccy/bigquery-emulator): the
 * container, a REST client pointed at it, the Storage Write API gRPC endpoint, and a no-op {@link
 * SinkWriter.Context}.
 */
@Testcontainers
@Timeout(180)
public abstract class AbstractBigQueryEmulatorITCase {

    public static final String PROJECT = "it-project";
    public static final String DATASET = "it_dataset";

    public static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    @Container
    private static final GenericContainer<?> EMULATOR =
            BigQueryEmulatorContainers.newContainer(PROJECT, DATASET);

    public static BigQuery restClient;

    @BeforeAll
    static void createRestClient() {
        restClient = BigQueryEmulatorContainers.restClient(EMULATOR, PROJECT);
    }

    public static String grpcEndpoint() {
        return BigQueryEmulatorContainers.grpcEndpoint(EMULATOR);
    }

    public static String restEndpoint() {
        return BigQueryEmulatorContainers.restEndpoint(EMULATOR);
    }

    /**
     * The production appender factory pointed at the emulator — the same code path a sink built
     * with {@code emulatorEndpoint(...)} takes, so these tests measure production behaviour rather
     * than a test-only stand-in.
     */
    public static RowAppenderFactory emulatorAppenderFactory() {
        return new StreamWriterRowAppenderFactory(
                DefaultStreamOptions.builder().build(),
                EmulatorEndpoint.parse(grpcEndpoint(), "emulatorEndpoint"));
    }

    /** Creates a table in the emulator dataset with the given Storage-form schema. */
    public static void createTable(String table, TableSchema schema) {
        restClient.create(
                TableInfo.newBuilder(
                                TableId.of(PROJECT, DATASET, table),
                                StandardTableDefinition.newBuilder()
                                        .setSchema(StorageSchemaConverter.toBigQuerySchema(schema))
                                        .build())
                        .build());
    }

    /** Returns the values of the {@code name} column of the given table, sorted. */
    public static List<String> queryNames(String table) throws InterruptedException {
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
