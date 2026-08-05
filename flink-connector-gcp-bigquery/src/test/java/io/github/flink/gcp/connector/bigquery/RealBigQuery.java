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
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * REST plumbing shared by the ITCases gated on real-GCP credentials: the {@code BQ_IT_*} variables,
 * a REST client over application-default credentials, table creation/deletion and query read-back.
 * The gated classes reach BigQuery through this and nothing else (#292); {@link RealGcs} is the
 * sibling for the FILE_LOADS staging bucket, which only that write method needs.
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

    /** The gated dataset's destination for {@code table}, as the sink builders take it. */
    public static TableDestination destination(String table) {
        return TableDestination.of(project(), dataset(), table);
    }

    /** Creates {@code table} in the gated dataset with the given Storage Write API schema. */
    public static void createTable(String table, TableSchema schema) {
        createTable(table, StorageSchemaConverter.toBigQuerySchema(schema));
    }

    /**
     * Creates {@code table} in the gated dataset with the given REST schema — the overload for
     * columns the Storage Write API schema cannot express, such as {@code INTERVAL}.
     */
    public static void createTable(String table, Schema schema) {
        client().create(
                        TableInfo.of(
                                TableId.of(project(), dataset(), table),
                                StandardTableDefinition.of(schema)));
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

    /** Returns the live columns of {@code table}, in order. */
    public static FieldList tableFields(String table) {
        Schema schema =
                client().getTable(TableId.of(project(), dataset(), table))
                        .<StandardTableDefinition>getDefinition()
                        .getSchema();
        assertThat(schema).isNotNull();
        return schema.getFields();
    }

    /** Runs the query and returns its rows. */
    public static List<FieldValueList> queryRows(String sql) throws InterruptedException {
        List<FieldValueList> rows = new ArrayList<>();
        client().query(QueryJobConfiguration.newBuilder(sql).build())
                .iterateAll()
                .forEach(rows::add);
        return rows;
    }

    /**
     * Runs the query and returns its first column, one entry per row, {@code null} for a NULL cell.
     *
     * <p>The null mapping is not decoration: {@link
     * com.google.cloud.bigquery.FieldValue#getLongValue()} is {@code
     * Long.parseLong(getStringValue())} and {@code getStringValue()} null-checks, so a NULL cell
     * throws — and defaulting it to {@code 0} would be a wrong answer rather than a failure.
     */
    public static List<Long> queryLongs(String sql) throws InterruptedException {
        List<Long> values = new ArrayList<>();
        for (FieldValueList row : queryRows(sql)) {
            values.add(row.get(0).isNull() ? null : row.get(0).getLongValue());
        }
        return values;
    }

    /** Returns the fully qualified, backtick-quoted table path for use in a query. */
    public static String tablePath(String table) {
        return "`" + project() + "." + dataset() + "." + table + "`";
    }
}
