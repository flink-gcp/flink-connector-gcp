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

package io.github.flink.gcp.connector.bigquery.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import io.github.flink.gcp.connector.bigquery.source.AbstractBigQuerySourceEmulatorITCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code bigquery} catalog through the planner against the emulator: {@code CREATE CATALOG},
 * listing, {@code DESCRIBE}, a read that matches a hand-written table's, a write, and the read-back
 * types of a table the sink created.
 *
 * <p>On the source harness because a read needs its hyphen-free project; the sink writes to it as
 * well.
 */
class BigQueryCatalogITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String PEOPLE = "catalog_people";
    private static final String OTHER_DATASET = "catalog_other";
    private static final String RESULT_DATASET = "catalog_results";

    @BeforeAll
    static void seed() throws Exception {
        restClient.create(DatasetInfo.of(DatasetId.of(PROJECT, OTHER_DATASET)));
        restClient.create(DatasetInfo.of(DatasetId.of(PROJECT, RESULT_DATASET)));
        createTable(
                PEOPLE,
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED)
                        .build(),
                Field.of("name", StandardSQLTypeName.STRING));
        insert(PEOPLE, "id, name", "(1, 'Ada'), (2, 'Grace')");
    }

    private static TableEnvironment batch() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        createCatalog(table);
        return table;
    }

    private static void createCatalog(TableEnvironment table) {
        table.executeSql(
                "CREATE CATALOG bq WITH ("
                        + "'type' = 'bigquery', "
                        + "'project' = '"
                        + PROJECT
                        + "', "
                        + "'default-database' = '"
                        + DATASET
                        + "', "
                        + "'emulator-endpoint' = '"
                        + grpcEndpoint()
                        + "', "
                        + "'emulator-rest-endpoint' = '"
                        + restEndpoint()
                        + "')");
    }

    @Test
    void listsDatasetsAndTables() throws Exception {
        TableEnvironment table = batch();
        table.executeSql("USE CATALOG bq");

        assertThat(firstColumn(table, "SHOW DATABASES")).contains(DATASET, OTHER_DATASET);
        assertThat(firstColumn(table, "SHOW TABLES")).contains(PEOPLE);
    }

    @Test
    void describesATableWithTheTypesItsColumnsMapTo() {
        TableEnvironment table = batch();

        ResolvedSchema schema = table.from("bq." + DATASET + "." + PEOPLE).getResolvedSchema();

        assertThat(schema.getColumns())
                .extracting(Column::getName, Column::getDataType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("id", DataTypes.BIGINT().notNull()),
                        org.assertj.core.groups.Tuple.tuple("name", DataTypes.STRING()));
    }

    @Test
    void readsWhatAHandWrittenTableReads() throws Exception {
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE hand_written (id BIGINT NOT NULL, name STRING) WITH ("
                        + "'connector' = 'bigquery', 'project' = '"
                        + PROJECT
                        + "', 'dataset' = '"
                        + DATASET
                        + "', 'table' = '"
                        + PEOPLE
                        + "', 'emulator-endpoint' = '"
                        + grpcEndpoint()
                        + "')");

        List<Row> throughCatalog =
                rows(table, "SELECT id, name FROM bq." + DATASET + "." + PEOPLE + " ORDER BY id");
        List<Row> handWritten = rows(table, "SELECT id, name FROM hand_written ORDER BY id");

        assertThat(throughCatalog).isEqualTo(handWritten).hasSize(2);
    }

    @Test
    void writesThroughACatalogTable() throws Exception {
        String target = "catalog_written";
        createTable(
                target,
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING));
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        createCatalog(table);

        table.executeSql(
                        "INSERT INTO bq."
                                + DATASET
                                + "."
                                + target
                                + " VALUES (CAST(7 AS BIGINT), 'Linus')")
                .await(60, java.util.concurrent.TimeUnit.SECONDS);

        List<String> names = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT name FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + "."
                                                + target
                                                + "`")
                                .build())
                .iterateAll()
                .forEach((FieldValueList row) -> names.add(row.get(0).getStringValue()));
        assertThat(names).containsExactly("Linus");
    }

    /**
     * Every declaration the sink derives a BigQuery column from reads back as the documented Flink
     * type — the lossy ones included, which are the point of the "Read back through the catalog as"
     * column on the connector page.
     */
    @Test
    void aTableTheSinkCreatedReadsBackAsTheDocumentedTypes() throws Exception {
        String target = "catalog_round_trip";
        Map<String, String> declared = new LinkedHashMap<>();
        Map<String, DataType> readBack = new LinkedHashMap<>();
        column(declared, readBack, "c_tinyint", "TINYINT", DataTypes.BIGINT());
        column(declared, readBack, "c_smallint", "SMALLINT", DataTypes.BIGINT());
        column(declared, readBack, "c_int", "INT", DataTypes.BIGINT());
        column(declared, readBack, "c_bigint", "BIGINT", DataTypes.BIGINT());
        column(declared, readBack, "c_float", "FLOAT", DataTypes.DOUBLE());
        column(declared, readBack, "c_double", "DOUBLE", DataTypes.DOUBLE());
        column(declared, readBack, "c_numeric", "DECIMAL(10, 2)", DataTypes.DECIMAL(10, 2));
        column(declared, readBack, "c_bignumeric", "DECIMAL(38, 20)", DataTypes.DECIMAL(38, 20));
        column(declared, readBack, "c_boolean", "BOOLEAN", DataTypes.BOOLEAN());
        column(declared, readBack, "c_char", "CHAR(3)", DataTypes.STRING());
        column(declared, readBack, "c_varchar", "VARCHAR(10)", DataTypes.STRING());
        column(declared, readBack, "c_string", "STRING", DataTypes.STRING());
        column(declared, readBack, "c_bytes", "BYTES", DataTypes.BYTES());
        column(declared, readBack, "c_binary", "BINARY(4)", DataTypes.BYTES());
        column(declared, readBack, "c_varbinary", "VARBINARY(8)", DataTypes.BYTES());
        column(declared, readBack, "c_date", "DATE", DataTypes.DATE());
        column(declared, readBack, "c_time", "TIME", DataTypes.TIME(3));
        column(declared, readBack, "c_timestamp", "TIMESTAMP(6)", DataTypes.TIMESTAMP(6));
        column(
                declared,
                readBack,
                "c_timestamp_ltz",
                "TIMESTAMP_LTZ(6)",
                DataTypes.TIMESTAMP_LTZ(6));
        column(
                declared,
                readBack,
                "c_row",
                "ROW<a BIGINT, b STRING>",
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.BIGINT()),
                        DataTypes.FIELD("b", DataTypes.STRING())));
        column(
                declared,
                readBack,
                "c_array",
                "ARRAY<STRING NOT NULL>",
                DataTypes.ARRAY(DataTypes.STRING().notNull()));
        column(
                declared,
                readBack,
                "c_map",
                "MAP<STRING, BIGINT>",
                DataTypes.ARRAY(
                        DataTypes.ROW(
                                        DataTypes.FIELD("key", DataTypes.STRING()),
                                        DataTypes.FIELD("value", DataTypes.BIGINT()))
                                .notNull()));
        column(
                declared,
                readBack,
                "c_multiset",
                "MULTISET<STRING>",
                DataTypes.ARRAY(
                        DataTypes.ROW(
                                        DataTypes.FIELD("key", DataTypes.STRING()),
                                        DataTypes.FIELD("value", DataTypes.BIGINT()))
                                .notNull()));
        column(declared, readBack, "c_not_null", "STRING NOT NULL", DataTypes.STRING());
        column(declared, readBack, "c_geography", "STRING", DataTypes.STRING());
        column(declared, readBack, "c_json_row", "ROW<a BIGINT>", DataTypes.STRING());
        column(declared, readBack, "c_json", "STRING", DataTypes.STRING());

        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String columns =
                declared.entrySet().stream()
                        .map(e -> e.getKey() + " " + e.getValue())
                        .collect(Collectors.joining(", "));
        table.executeSql(
                "CREATE TABLE sink_declared ("
                        + columns
                        + ") WITH ('connector' = 'bigquery', 'project' = '"
                        + PROJECT
                        + "', 'dataset' = '"
                        + DATASET
                        + "', 'table' = '"
                        + target
                        + "', 'sink.json-field-paths' = 'c_json;c_json_row',"
                        + " 'sink.geography-field-paths' = 'c_geography', 'emulator-endpoint' = '"
                        + grpcEndpoint()
                        + "', 'emulator-rest-endpoint' = '"
                        + restEndpoint()
                        + "')");
        // One row makes the sink create the table; its values do not matter here.
        table.executeSql(
                        "INSERT INTO sink_declared SELECT "
                                + declared.entrySet().stream()
                                        .map(
                                                e ->
                                                        e.getValue().endsWith("NOT NULL")
                                                                ? "'x'"
                                                                : "CAST(NULL AS "
                                                                        + e.getValue()
                                                                        + ")")
                                        .collect(Collectors.joining(", ")))
                .await(60, java.util.concurrent.TimeUnit.SECONDS);

        createCatalog(table);
        ResolvedSchema schema = table.from("bq." + DATASET + "." + target).getResolvedSchema();

        Map<String, DataType> actual = new LinkedHashMap<>();
        for (Column column : schema.getColumns()) {
            actual.put(column.getName(), column.getDataType());
        }
        assertThat(actual).containsExactlyEntriesOf(readBack);
        // Two rows whose Flink answer cannot tell BigQuery types apart: the sink made these, and
        // the catalog read them back as the documented Flink types.
        Schema created =
                restClient
                        .getTable(TableId.of(PROJECT, DATASET, target))
                        .getDefinition()
                        .getSchema();
        assertThat(created.getFields().get("c_bignumeric").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.BIGNUMERIC);
        assertThat(created.getFields().get("c_json").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.JSON);
        assertThat(created.getFields().get("c_json_row").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.JSON);
        assertThat(created.getFields().get("c_geography").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.GEOGRAPHY);
    }

    @Test
    void aBigQueryPrimaryKeyResolvesAsAFlinkPrimaryKeyOverNotNullColumns() {
        String keyed = "catalog_keyed";
        restClient.create(
                TableInfo.newBuilder(
                                TableId.of(PROJECT, DATASET, keyed),
                                StandardTableDefinition.of(
                                        Schema.of(
                                                Field.of("id", StandardSQLTypeName.STRING),
                                                Field.of("amount", StandardSQLTypeName.INT64))))
                        .setTableConstraints(
                                TableConstraints.newBuilder()
                                        .setPrimaryKey(
                                                PrimaryKey.newBuilder()
                                                        .setColumns(List.of("id"))
                                                        .build())
                                        .build())
                        .build());

        ResolvedSchema schema = batch().from("bq." + DATASET + "." + keyed).getResolvedSchema();

        assertThat(schema.getPrimaryKey())
                .hasValueSatisfying(key -> assertThat(key.getColumns()).containsExactly("id"));
        assertThat(schema.getColumn("id"))
                .hasValueSatisfying(
                        id -> assertThat(id.getDataType()).isEqualTo(DataTypes.STRING().notNull()));
    }

    @Test
    void aViewIsListedAndReadThroughMaterialization() throws Exception {
        String view = "catalog_people_view";
        restClient.create(
                TableInfo.of(
                        TableId.of(PROJECT, DATASET, view),
                        ViewDefinition.of(
                                "SELECT name FROM `"
                                        + PROJECT
                                        + "."
                                        + DATASET
                                        + "."
                                        + PEOPLE
                                        + "`")));
        TableEnvironment table = batch();
        table.executeSql("USE CATALOG bq");

        assertThat(firstColumn(table, "SHOW TABLES")).contains(view);
        assertThat(firstColumn(table, "SHOW VIEWS")).doesNotContain(view);
        // The emulator's Storage Read serves a view directly, so the rows alone would pass
        // without materialization; the query job's result table in the named dataset is what
        // shows the read went through it.
        assertThat(tablesIn(RESULT_DATASET)).isEmpty();
        assertThat(
                        firstColumn(
                                table,
                                "SELECT name FROM "
                                        + view
                                        + " /*+ OPTIONS('scan.query-result-dataset' = '"
                                        + RESULT_DATASET
                                        + "') */ ORDER BY name"))
                .containsExactly("Ada", "Grace");
        assertThat(tablesIn(RESULT_DATASET))
                .singleElement()
                .satisfies(name -> assertThat(name).startsWith("flink_bigquery_source_"));
    }

    private static void column(
            Map<String, String> declared,
            Map<String, DataType> readBack,
            String name,
            String declaration,
            DataType expected) {
        declared.put(name, declaration);
        readBack.put(name, expected);
    }

    private static List<String> tablesIn(String dataset) {
        List<String> tables = new ArrayList<>();
        restClient
                .listTables(DatasetId.of(PROJECT, dataset))
                .iterateAll()
                .forEach(table -> tables.add(table.getTableId().getTable()));
        return tables;
    }

    private static List<String> firstColumn(TableEnvironment table, String sql) throws Exception {
        return rows(table, sql).stream()
                .map(row -> String.valueOf(row.getField(0)))
                .collect(Collectors.toList());
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }
}
