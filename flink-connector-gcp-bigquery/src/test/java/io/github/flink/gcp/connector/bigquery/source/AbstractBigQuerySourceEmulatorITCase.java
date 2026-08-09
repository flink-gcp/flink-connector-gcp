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

package io.github.flink.gcp.connector.bigquery.source;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.testutils.bigquery.BigQueryEmulatorContainers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Shared harness for the source's integration tests against the BigQuery emulator
 * (goccy/bigquery-emulator): the container, a REST client that seeds tables, and the gRPC endpoint
 * the source reads through.
 *
 * <p>The project id has no hyphen, unlike the sink harness's, and that is load-bearing rather than
 * taste. The emulator answers a read session with an Avro schema whose namespace is {@code
 * <project>.<dataset>} — real BigQuery sends a record named {@code __root__} with no namespace —
 * and a hyphen is not a legal character in an Avro namespace, so every read against a hyphenated
 * project fails in Avro's schema parser before a row is decoded (measured against
 * goccy/bigquery-emulator 0.8.1 and Avro 1.12.1, 2026-08-09).
 */
@Testcontainers
@Timeout(180)
public abstract class AbstractBigQuerySourceEmulatorITCase {

    public static final String PROJECT = "itproject";
    public static final String DATASET = "it_dataset";

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

    /** Returns the destination of a table in the emulator's dataset. */
    public static TableDestination destination(String table) {
        return TableDestination.of(PROJECT, DATASET, table);
    }

    /** Creates a table in the emulator dataset. */
    public static void createTable(String table, Field... fields) {
        restClient.create(
                TableInfo.of(
                        TableId.of(PROJECT, DATASET, table),
                        StandardTableDefinition.of(Schema.of(fields))));
    }

    /**
     * Inserts rows with a query, which is how the emulator accepts data without a write path.
     *
     * @param table the table to insert into
     * @param columns the column list, for example {@code "id, name"}
     * @param values the values list, for example {@code "(1, 'a'), (2, 'b')"}
     */
    public static void insert(String table, String columns, String values)
            throws InterruptedException {
        restClient.query(
                QueryJobConfiguration.newBuilder(
                                "INSERT INTO `"
                                        + DATASET
                                        + "."
                                        + table
                                        + "` ("
                                        + columns
                                        + ") VALUES "
                                        + values)
                        .build());
    }
}
