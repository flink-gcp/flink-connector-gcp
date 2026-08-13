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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL-to-emulator acceptance tests for the Cloud Tasks table sink. */
class CloudTasksTableSinkITCase extends AbstractCloudTasksEmulatorITCase {

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inBatchMode());
    }

    private static String queueOptions(QueueDestination queue) {
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
                + "  'format' = 'json',\n"
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
}
