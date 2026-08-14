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

package io.github.flink.gcp.connector.cloudtasks.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Planner-level checks for writable metadata and the documented SQL shape. */
class CloudTasksTablePlanTest {

    private static final String QUEUE_OPTIONS =
            "  'connector' = 'cloud-tasks',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'location' = 'asia-northeast1',\n"
                    + "  'queue' = 'orders',\n"
                    + "  'format' = 'json'";

    private static final String FORM_OPTIONS =
            "  'connector' = 'cloud-tasks',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'location' = 'asia-northeast1',\n"
                    + "  'queue' = 'forms',\n"
                    + "  'http.url' = 'https://api.example.com/orders',\n"
                    + "  'format' = 'form-urlencoded'";

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void aNotNullUrlMetadataColumnCanReplaceTheFixedUrl() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_url STRING NOT NULL METADATA FROM 'url',\n"
                        + "  request_method STRING METADATA FROM 'http-method'\n"
                        + ") WITH (\n"
                        + QUEUE_OPTIONS
                        + "\n)");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO tasks VALUES ('payload',"
                                                + " 'https://example.com/tasks/1', 'GET')"))
                .doesNotThrowAnyException();
    }

    @Test
    void aNullableDynamicUrlIsRefusedWhenTheInsertIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_url STRING METADATA FROM 'url'\n"
                        + ") WITH (\n"
                        + QUEUE_OPTIONS
                        + "\n)");

        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO tasks VALUES ('payload',"
                                                + " 'https://example.com/tasks/1')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("STRING NOT NULL");
    }

    @Test
    void appEngineRoutingMetadataAndANotNullRelativeUriCanBePlanned() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE app_tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_path STRING NOT NULL METADATA FROM 'relative-uri',\n"
                        + "  request_method STRING METADATA FROM 'http-method',\n"
                        + "  service_name STRING METADATA FROM 'app-engine-service',\n"
                        + "  version_name STRING METADATA FROM 'app-engine-version',\n"
                        + "  instance_name STRING METADATA FROM 'app-engine-instance'\n"
                        + ") WITH (\n"
                        + QUEUE_OPTIONS
                        + ",\n  'target.type' = 'app-engine'\n)");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO app_tasks VALUES ('payload', '/tasks/1',"
                                                + " 'PUT', 'worker', 'v2', 'instance-3')"))
                .doesNotThrowAnyException();
    }

    @Test
    void aNullableDynamicAppEngineRelativeUriIsRefused() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE app_tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_path STRING METADATA FROM 'relative-uri'\n"
                        + ") WITH (\n"
                        + QUEUE_OPTIONS
                        + ",\n  'target.type' = 'app-engine'\n)");

        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO app_tasks VALUES ('payload', '/tasks/1')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("app-engine.relative-uri")
                .hasStackTraceContaining("STRING NOT NULL");
    }

    @Test
    void metadataFromTheOtherTargetFamilyIsRefused() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE app_tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_url STRING METADATA FROM 'url'\n"
                        + ") WITH (\n"
                        + QUEUE_OPTIONS
                        + ",\n  'target.type' = 'app-engine',\n"
                        + "  'app-engine.relative-uri' = '/tasks'\n)");

        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO app_tasks VALUES ('payload',"
                                                + " 'https://example.com/tasks')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("url")
                .hasStackTraceContaining("metadata");
    }

    @Test
    void aBodyFormatAndAuthenticatedFixedRequestParseTogether() {
        TableEnvironment tEnv = tableEnvironment();

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE tasks (\n"
                                                + "  order_id STRING,\n"
                                                + "  amount DECIMAL(12, 2),\n"
                                                + "  request_headers MAP<STRING, STRING> METADATA"
                                                + " FROM 'headers',\n"
                                                + "  schedule_at TIMESTAMP_LTZ(6) METADATA FROM"
                                                + " 'schedule-time',\n"
                                                + "  dedupe_key STRING METADATA FROM 'task-id'\n"
                                                + ") WITH (\n"
                                                + QUEUE_OPTIONS
                                                + ",\n"
                                                + "  'http.url' = 'https://service.example/orders',\n"
                                                + "  'http.method' = 'POST',\n"
                                                + "  'http.headers.Content-Type' ="
                                                + " 'application/json',\n"
                                                + "  'http.oidc.service-account-email' ="
                                                + " 'dispatcher@my-project.iam.gserviceaccount.com',\n"
                                                + "  'http.oidc.audience' ="
                                                + " 'https://service.example'\n"
                                                + ")"))
                .doesNotThrowAnyException();

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO tasks VALUES ('o-1', 12.34, MAP['X-Trace',"
                                                + " 'abc'], CAST('2026-08-13 01:02:03.123456' AS"
                                                + " TIMESTAMP_LTZ(6)), 'o-1')"))
                .doesNotThrowAnyException();
    }

    @Test
    void documentedNestedJsonRequestParses() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE json_tasks (\n"
                        + "  order_id STRING,\n"
                        + "  customer ROW<name STRING, city STRING>,\n"
                        + "  items ARRAY<ROW<sku STRING, quantity INT>>,\n"
                        + "  attributes MAP<STRING, STRING>,\n"
                        + "  note STRING,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + QUEUE_OPTIONS
                        + ",\n"
                        + "  'http.url' = 'https://api.example.com/orders',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'application/json',\n"
                        + "  'json.encode.ignore-null-fields' = 'false'\n"
                        + ")");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO json_tasks VALUES ("
                                                + "'o-42', "
                                                + "CAST(ROW('Alice \"A\"', '東京') AS "
                                                + "ROW<name STRING, city STRING>), "
                                                + "ARRAY["
                                                + "CAST(ROW('book', 2) AS "
                                                + "ROW<sku STRING, quantity INT>), "
                                                + "CAST(ROW('pen', 1) AS "
                                                + "ROW<sku STRING, quantity INT>)], "
                                                + "MAP['priority', 'high'], "
                                                + "CAST(NULL AS STRING), "
                                                + "MAP['X-Trace-Id', 'trace-42'])"))
                .doesNotThrowAnyException();
    }

    @Test
    void documentedCsvRequestParses() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE csv_tasks (\n"
                        + "  order_id STRING,\n"
                        + "  note STRING,\n"
                        + "  missing_value STRING,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + "  'connector' = 'cloud-tasks',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'location' = 'asia-northeast1',\n"
                        + "  'queue' = 'orders',\n"
                        + "  'http.url' = 'https://api.example.com/import',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'text/csv; charset=UTF-8',\n"
                        + "  'format' = 'csv',\n"
                        + "  'csv.field-delimiter' = '|',\n"
                        + "  'csv.quote-character' = '\"',\n"
                        + "  'csv.null-literal' = 'NULL'\n"
                        + ")");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO csv_tasks SELECT '42', "
                                                + "U&'line 1 | \"quoted\"\\000Aline 2', "
                                                + "CAST(NULL AS STRING), "
                                                + "MAP['X-Trace-Id', 'trace-42']"))
                .doesNotThrowAnyException();
    }

    @Test
    void documentedRawRequestParses() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE raw_tasks (\n"
                        + "  body STRING,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + "  'connector' = 'cloud-tasks',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'location' = 'asia-northeast1',\n"
                        + "  'queue' = 'orders',\n"
                        + "  'http.url' = 'https://api.example.com/text',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'text/plain; charset=UTF-16BE',\n"
                        + "  'format' = 'raw',\n"
                        + "  'raw.charset' = 'UTF-16BE'\n"
                        + ")");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO raw_tasks VALUES ("
                                                + "'東京', MAP['X-Trace-Id', 'trace-42'])"))
                .doesNotThrowAnyException();
    }

    @Test
    void documentedBinaryAvroRequestParses() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE avro_tasks (\n"
                        + "  order_id STRING NOT NULL,\n"
                        + "  quantity INT NOT NULL,\n"
                        + "  gift BOOLEAN NOT NULL,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + "  'connector' = 'cloud-tasks',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'location' = 'asia-northeast1',\n"
                        + "  'queue' = 'orders',\n"
                        + "  'http.url' = 'https://api.example.com/avro-orders',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'application/octet-stream',\n"
                        + "  'format' = 'avro',\n"
                        + "  'avro.encoding' = 'binary'\n"
                        + ")");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO avro_tasks VALUES ("
                                                + "'o-7', 3, TRUE, "
                                                + "MAP['X-Trace-Id', 'trace-7'])"))
                .doesNotThrowAnyException();
    }

    @Test
    void documentedFormTransformationsParse() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE form_tasks (\n"
                        + "  order_id STRING,\n"
                        + "  note STRING,\n"
                        + "  tags ARRAY<STRING>,\n"
                        + "  categories STRING\n"
                        + ") WITH (\n"
                        + FORM_OPTIONS
                        + "\n)");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO form_tasks VALUES ("
                                                + "'42', '東京 + pickup', ARRAY['urgent', 'gift'], "
                                                + "ARRAY_JOIN(ARRAY['books', 'sale'], ','))"))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO form_tasks VALUES ("
                                                + "'43', '', CAST(NULL AS ARRAY<STRING>), "
                                                + "CAST(NULL AS STRING))"))
                .doesNotThrowAnyException();

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW incoming_orders AS\n"
                        + "SELECT ARRAY['book', 'pen'] AS items,\n"
                        + "       CAST(ROW('Alice', '100-0001') AS "
                        + "ROW<name STRING, postal_code STRING>) AS customer,\n"
                        + "       MAP['priority', 'high'] AS attributes");
        tEnv.executeSql(
                "CREATE TABLE nested_form_tasks (\n"
                        + "  `items[]` ARRAY<STRING>,\n"
                        + "  `customer.name` STRING,\n"
                        + "  `customer[postalCode]` STRING,\n"
                        + "  `attributes[priority]` STRING\n"
                        + ") WITH (\n"
                        + FORM_OPTIONS
                        + "\n)");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO nested_form_tasks "
                                                + "SELECT items, customer.name, "
                                                + "customer.postal_code, attributes['priority'] "
                                                + "FROM incoming_orders"))
                .doesNotThrowAnyException();

        tEnv.executeSql(
                "CREATE TABLE json_parameter_tasks (payload STRING) WITH (\n"
                        + FORM_OPTIONS
                        + "\n)");

        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO json_parameter_tasks "
                                                + "SELECT JSON_OBJECT("
                                                + "KEY 'name' VALUE customer.name, "
                                                + "KEY 'postalCode' VALUE customer.postal_code, "
                                                + "KEY 'items' VALUE items) "
                                                + "FROM incoming_orders"))
                .doesNotThrowAnyException();

        tEnv.executeSql(
                "CREATE TABLE custom_form_tasks (body STRING) WITH (\n"
                        + "  'connector' = 'cloud-tasks',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'location' = 'asia-northeast1',\n"
                        + "  'queue' = 'forms',\n"
                        + "  'http.url' = 'https://api.example.com/orders',\n"
                        + "  'http.headers.Content-Type' = "
                        + "'application/x-www-form-urlencoded',\n"
                        + "  'format' = 'raw'\n"
                        + ")");
        assertThatCode(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO custom_form_tasks VALUES ("
                                                + "'items%5B0%5D=book&items%5B1%5D=pen"
                                                + "&attributes=priority%3Ahigh%2Ccolor%3Ablue')"))
                .doesNotThrowAnyException();
    }
}
