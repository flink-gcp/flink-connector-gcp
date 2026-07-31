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

package io.github.flink.gcp.connector.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * REST plumbing shared by the ITCases gated on real-GCP credentials: the {@code BQ_IT_*} variables,
 * a REST client over application-default credentials, table creation/deletion and query read-back.
 *
 * <p>Deliberately a static utility and <b>not</b> a base class carrying the gating annotation:
 * {@code scripts/e2e-gated-its.sh} greps test sources for the literal environment-variable gate and
 * demands a surefire report per match, so an abstract class carrying it would fail {@code
 * --assert-ran}. Each gated ITCase declares its own {@code @EnabledIfEnvironmentVariable}
 * annotations.
 */
public final class RealBigQuery {

    private RealBigQuery() {}

    /** The GCP project the gated ITCases run against ({@code BQ_IT_PROJECT}). */
    public static String project() {
        return System.getenv("BQ_IT_PROJECT");
    }

    /** The BigQuery dataset the gated ITCases create their tables in ({@code BQ_IT_DATASET}). */
    public static String dataset() {
        return System.getenv("BQ_IT_DATASET");
    }

    /** A REST client over application-default credentials. */
    public static BigQuery client() {
        return BigQueryOptions.newBuilder().setProjectId(project()).build().getService();
    }

    /** Creates {@code table} in the gated dataset with the given Storage Write API schema. */
    public static void createTable(String table, TableSchema schema) {
        client().create(
                        TableInfo.newBuilder(
                                        TableId.of(project(), dataset(), table),
                                        StandardTableDefinition.newBuilder()
                                                .setSchema(
                                                        StorageSchemaConverter.toBigQuerySchema(
                                                                schema))
                                                .build())
                                .build());
    }

    /**
     * Deletes the given tables, best-effort; the dataset's default table expiration (24 h, set in
     * {@code opentofu/flink-gcp/it-resources.tf}) is the backstop for a crashed run.
     */
    public static void deleteTables(String... tables) {
        BigQuery client = client();
        for (String table : tables) {
            client.delete(TableId.of(project(), dataset(), table));
        }
    }

    /** Runs the query and returns its rows. */
    public static List<FieldValueList> queryRows(String sql) throws InterruptedException {
        List<FieldValueList> rows = new ArrayList<>();
        client().query(QueryJobConfiguration.newBuilder(sql).build())
                .iterateAll()
                .forEach(rows::add);
        return rows;
    }

    /** Returns the fully qualified, backtick-quoted table path for use in a query. */
    public static String tablePath(String table) {
        return "`" + project() + "." + dataset() + "." + table + "`";
    }
}
