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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL-to-emulator acceptance tests for the Cloud Tasks table sink. */
class CloudTasksTableSinkITCase extends AbstractCloudTasksEmulatorITCase {

    private static RecordedRequest awaitFirstRequest(String path) throws InterruptedException {
        List<RecordedRequest> requests = awaitRequests(path, 1);
        assertThat(requests).as("requests dispatched to %s", path).isNotEmpty();
        return requests.get(0);
    }

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inBatchMode());
    }

    private static String queueOptions(QueueDestination queue) {
        return queueOptions(queue, "json");
    }

    private static String queueOptions(QueueDestination queue, String format) {
        return "  'connector' = 'cloud-tasks',\n"
                + "  'project' = '"
                + queue.getProject()
                + "',\n"
                + "  'location' = '"
                + queue.getLocation()
                + "',\n"
                + "  'queue' = '"
                + queue.getQueue()
                + "',\n"
                + "  'format' = '"
                + format
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'";
    }

    @Test
    void sqlMetadataCreatesOneNamedBodylessTaskAcrossReplays() throws Exception {
        QueueDestination queue = createPausedQueue("table-metadata");
        String url = targetUrl("/table-metadata");
        long scheduleMillis = Instant.now().plusSeconds(60).toEpochMilli();
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_url STRING NOT NULL METADATA FROM 'url',\n"
                        + "  request_method STRING METADATA FROM 'http-method',\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers',\n"
                        + "  schedule_at TIMESTAMP_LTZ(6) METADATA FROM 'schedule-time',\n"
                        + "  dedupe_key STRING METADATA FROM 'task-id'\n"
                        + ") WITH (\n"
                        + queueOptions(queue)
                        + ",\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.X-Fixed' = 'fixed'\n"
                        + ")");
        String values =
                " VALUES ('ignored', '"
                        + url
                        + "', 'GET', MAP['x-fixed', 'row', 'X-Trace', 'abc'],"
                        + " TO_TIMESTAMP_LTZ("
                        + scheduleMillis
                        + ", 3), 'event-1')";

        tEnv.executeSql("INSERT INTO tasks" + values).await();
        // A separate completed job gives the emulator two distinct create calls. This avoids the
        // emulator's non-atomic uniqueness check while proving replay deduplication.
        tEnv.executeSql("INSERT INTO tasks" + values).await();

        List<Task> tasks = listTasks(queue);
        assertThat(tasks).hasSize(1);
        Task task = tasks.get(0);
        assertThat(task.getName()).matches(".*/tasks/[0-9a-f]{64}");
        assertThat(task.getHttpRequest().getUrl()).isEqualTo(url);
        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.GET);
        assertThat(task.getHttpRequest().getBody()).isEmpty();
        assertThat(task.getHttpRequest().getHeadersMap())
                .containsEntry("x-fixed", "row")
                .containsEntry("X-Trace", "abc")
                .doesNotContainKey("X-Fixed");
        assertThat(task.getScheduleTime().getSeconds()).isEqualTo(scheduleMillis / 1_000L);
    }

    @Test
    void sqlPostDispatchesTheFormatBodyAndMergedHeaders() throws Exception {
        QueueDestination queue = createQueue("table-post");
        String path = "/table-post";
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE tasks (\n"
                        + "  payload STRING,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + queueOptions(queue)
                        + ",\n"
                        + "  'http.url' = '"
                        + targetUrl(path)
                        + "',\n"
                        + "  'http.headers.Content-Type' = 'application/json',\n"
                        + "  'http.headers.X-Fixed' = 'fixed'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO tasks VALUES ('hello', MAP['x-fixed', 'row', 'X-Trace',"
                                + " 'trace-1'])")
                .await();

        List<RecordedRequest> requests = awaitRequests(path, 1);
        assertThat(requests).hasSize(1);
        RecordedRequest request = requests.get(0);
        assertThat(request.method).isEqualTo("POST");
        assertThat(request.body).isEqualTo("{\"payload\":\"hello\"}");
        assertThat(request.header("content-type")).isEqualTo("application/json");
        assertThat(request.header("x-fixed")).isEqualTo("row");
        assertThat(request.header("x-trace")).isEqualTo("trace-1");
    }

    @Test
    void documentedNestedJsonRequestDispatchesExactUtf8Bytes() throws Exception {
        QueueDestination queue = createQueue("table-json-example");
        String path = "/table-json-example";
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
                        + queueOptions(queue)
                        + ",\n"
                        + "  'http.url' = '"
                        + targetUrl(path)
                        + "',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'application/json',\n"
                        + "  'json.encode.ignore-null-fields' = 'false'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO json_tasks VALUES ("
                                + "'o-42', "
                                + "CAST(ROW('Alice \"A\"', '東京') AS "
                                + "ROW<name STRING, city STRING>), "
                                + "ARRAY["
                                + "CAST(ROW('book', 2) AS ROW<sku STRING, quantity INT>), "
                                + "CAST(ROW('pen', 1) AS ROW<sku STRING, quantity INT>)], "
                                + "MAP['priority', 'high'], "
                                + "CAST(NULL AS STRING), "
                                + "MAP['X-Trace-Id', 'trace-42'])")
                .await();

        RecordedRequest request = awaitFirstRequest(path);
        String expected =
                "{\"order_id\":\"o-42\","
                        + "\"customer\":{\"name\":\"Alice \\\"A\\\"\",\"city\":\"東京\"},"
                        + "\"items\":[{\"sku\":\"book\",\"quantity\":2},"
                        + "{\"sku\":\"pen\",\"quantity\":1}],"
                        + "\"attributes\":{\"priority\":\"high\"},\"note\":null}";
        assertThat(request.method).isEqualTo("POST");
        assertThat(request.bodyBytes).containsExactly(expected.getBytes(StandardCharsets.UTF_8));
        assertThat(request.header("content-type")).isEqualTo("application/json");
        assertThat(request.header("x-trace-id")).isEqualTo("trace-42");
    }

    @Test
    void documentedCsvRequestDispatchesExactUtf8Bytes() throws Exception {
        QueueDestination queue = createQueue("table-csv-example");
        String path = "/table-csv-example";
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE csv_tasks (\n"
                        + "  order_id STRING,\n"
                        + "  note STRING,\n"
                        + "  missing_value STRING,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + queueOptions(queue, "csv")
                        + ",\n"
                        + "  'http.url' = '"
                        + targetUrl(path)
                        + "',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'text/csv; charset=UTF-8',\n"
                        + "  'csv.field-delimiter' = '|',\n"
                        + "  'csv.quote-character' = '\"',\n"
                        + "  'csv.null-literal' = 'NULL'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO csv_tasks SELECT '42', "
                                + "U&'line 1 | \"quoted\"\\000Aline 2', "
                                + "CAST(NULL AS STRING), "
                                + "MAP['X-Trace-Id', 'trace-42']")
                .await();

        RecordedRequest request = awaitFirstRequest(path);
        String expected = "\"42\"|\"line 1 | \"\"quoted\"\"\nline 2\"|NULL";
        assertThat(request.bodyBytes).containsExactly(expected.getBytes(StandardCharsets.UTF_8));
        assertThat(request.header("content-type")).isEqualTo("text/csv; charset=UTF-8");
        assertThat(request.header("x-trace-id")).isEqualTo("trace-42");
    }

    @Test
    void documentedRawRequestDispatchesExactUtf16BeBytes() throws Exception {
        QueueDestination queue = createQueue("table-raw-example");
        String path = "/table-raw-example";
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE raw_tasks (\n"
                        + "  body STRING,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + queueOptions(queue, "raw")
                        + ",\n"
                        + "  'http.url' = '"
                        + targetUrl(path)
                        + "',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'text/plain; charset=UTF-16BE',\n"
                        + "  'raw.charset' = 'UTF-16BE'\n"
                        + ")");

        tEnv.executeSql("INSERT INTO raw_tasks VALUES (" + "'東京', MAP['X-Trace-Id', 'trace-42'])")
                .await();

        RecordedRequest request = awaitFirstRequest(path);
        assertThat(request.bodyBytes)
                .containsExactly((byte) 0x67, (byte) 0x71, (byte) 0x4E, (byte) 0xAC);
        assertThat(request.header("content-type")).isEqualTo("text/plain; charset=UTF-16BE");
        assertThat(request.header("x-trace-id")).isEqualTo("trace-42");
    }

    @Test
    void documentedAvroRequestDispatchesExactBinaryDatum() throws Exception {
        QueueDestination queue = createQueue("table-avro-example");
        String path = "/table-avro-example";
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE avro_tasks (\n"
                        + "  order_id STRING NOT NULL,\n"
                        + "  quantity INT NOT NULL,\n"
                        + "  gift BOOLEAN NOT NULL,\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers'\n"
                        + ") WITH (\n"
                        + queueOptions(queue, "avro")
                        + ",\n"
                        + "  'http.url' = '"
                        + targetUrl(path)
                        + "',\n"
                        + "  'http.method' = 'POST',\n"
                        + "  'http.headers.Content-Type' = 'application/octet-stream',\n"
                        + "  'avro.encoding' = 'binary'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO avro_tasks VALUES ("
                                + "'o-7', 3, TRUE, MAP['X-Trace-Id', 'trace-7'])")
                .await();

        RecordedRequest request = awaitFirstRequest(path);
        assertThat(request.bodyBytes)
                .containsExactly(
                        (byte) 0x06,
                        (byte) 0x6F,
                        (byte) 0x2D,
                        (byte) 0x37,
                        (byte) 0x06,
                        (byte) 0x01);
        assertThat(request.header("content-type")).isEqualTo("application/octet-stream");
        assertThat(request.header("x-trace-id")).isEqualTo("trace-7");

        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"record\","
                                        + "\"namespace\":\"org.apache.flink.avro.generated\","
                                        + "\"fields\":["
                                        + "{\"name\":\"order_id\",\"type\":\"string\"},"
                                        + "{\"name\":\"quantity\",\"type\":\"int\"},"
                                        + "{\"name\":\"gift\",\"type\":\"boolean\"}]}");
        GenericRecord decoded =
                new GenericDatumReader<GenericRecord>(schema)
                        .read(null, DecoderFactory.get().binaryDecoder(request.bodyBytes, null));
        assertThat(decoded.get("order_id").toString()).isEqualTo("o-7");
        assertThat(decoded.get("quantity")).isEqualTo(3);
        assertThat(decoded.get("gift")).isEqualTo(true);
    }

    @Test
    void sqlPostDispatchesTheFormFormatsHeaderAndExactUtf8Bytes() throws Exception {
        QueueDestination queue = createQueue("table-form-post");
        String path = "/table-form-post";
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE tasks (\n"
                        + "  customer_name STRING,\n"
                        + "  tags ARRAY<STRING>,\n"
                        + "  empty_value STRING,\n"
                        + "  omitted_value STRING\n"
                        + ") WITH (\n"
                        + queueOptions(queue, "form-urlencoded")
                        + ",\n"
                        + "  'http.url' = '"
                        + targetUrl(path)
                        + "'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO tasks VALUES ('東京 +&=', ARRAY['one two', 'a+b'], '',"
                                + " CAST(NULL AS STRING))")
                .await();

        RecordedRequest request = awaitFirstRequest(path);
        String expected =
                "customer_name=%E6%9D%B1%E4%BA%AC+%2B%26%3D"
                        + "&tags=one+two&tags=a%2Bb&empty_value=";
        assertThat(request.method).isEqualTo("POST");
        assertThat(request.bodyBytes)
                .containsExactly(expected.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(request.header("content-type")).isEqualTo("application/x-www-form-urlencoded");
    }

    @Test
    void sqlAppEngineMetadataCreatesAnInspectableInternalTargetTask() throws Exception {
        QueueDestination queue = createPausedQueue("table-app-engine");
        long scheduleMillis = Instant.now().plusSeconds(60).toEpochMilli();
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE app_tasks (\n"
                        + "  payload STRING,\n"
                        + "  target_path STRING NOT NULL METADATA FROM 'relative-uri',\n"
                        + "  request_method STRING METADATA FROM 'http-method',\n"
                        + "  request_headers MAP<STRING, STRING> METADATA FROM 'headers',\n"
                        + "  service_name STRING METADATA FROM 'app-engine-service',\n"
                        + "  version_name STRING METADATA FROM 'app-engine-version',\n"
                        + "  instance_name STRING METADATA FROM 'app-engine-instance',\n"
                        + "  schedule_at TIMESTAMP_LTZ(6) METADATA FROM 'schedule-time',\n"
                        + "  dedupe_key STRING METADATA FROM 'task-id'\n"
                        + ") WITH (\n"
                        + queueOptions(queue)
                        + ",\n"
                        + "  'target.type' = 'app-engine',\n"
                        + "  'app-engine.method' = 'POST',\n"
                        + "  'app-engine.headers.Content-Type' = 'application/json',\n"
                        + "  'app-engine.headers.X-Fixed' = 'fixed',\n"
                        + "  'app-engine.service' = 'fixed-service',\n"
                        + "  'app-engine.version' = 'fixed-version',\n"
                        + "  'app-engine.instance' = 'fixed-instance'\n"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO app_tasks VALUES ('hello', '/tasks/17?source=table', 'PUT',"
                                + " MAP['x-fixed', 'row', 'X-Trace', 'trace-17'], 'row-service',"
                                + " CAST(NULL AS STRING), '', TO_TIMESTAMP_LTZ("
                                + scheduleMillis
                                + ", 3), 'app-event-17')")
                .await();

        List<Task> tasks = listTasks(queue);
        assertThat(tasks).hasSize(1);
        Task task = tasks.get(0);
        assertThat(task.getName()).matches(".*/tasks/[0-9a-f]{64}");
        assertThat(task.getMessageTypeCase())
                .isEqualTo(Task.MessageTypeCase.APP_ENGINE_HTTP_REQUEST);
        assertThat(task.getAppEngineHttpRequest().getRelativeUri())
                .isEqualTo("/tasks/17?source=table");
        assertThat(task.getAppEngineHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(task.getAppEngineHttpRequest().getBody().toStringUtf8())
                .isEqualTo("{\"payload\":\"hello\"}");
        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsEntry("Content-Type", "application/json")
                .containsEntry("x-fixed", "row")
                .containsEntry("X-Trace", "trace-17")
                .doesNotContainKey("X-Fixed");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getService())
                .isEqualTo("row-service");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getVersion())
                .isEqualTo("fixed-version");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getInstance()).isEmpty();
        assertThat(task.getScheduleTime().getSeconds()).isEqualTo(scheduleMillis / 1_000L);
    }
}
