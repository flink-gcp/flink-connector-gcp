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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.grpc.Deadline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservedSinksTest {
    private static Task task() {
        return Task.newBuilder()
                .setHttpRequest(
                        HttpRequest.newBuilder().setBody(MeasurementPayload.create(1024, 0)))
                .build();
    }

    @Test
    void joinsRegistrationCsvAndTerminalBeforeAndAfterTheActualSend() throws Exception {
        var options =
                MeasurementOptions.parse(
                        MeasurementOptionsTest.windowArguments("--arm", "UNNAMED"));
        Map<String, String> documents = new LinkedHashMap<>();
        var raw = new FakeCreator();
        var rows = new FakeRowsOutput();
        UUID[] rowsIncarnation = new UUID[1];
        var creator =
                ObservedSinks.observe(
                        raw,
                        options,
                        documents::put,
                        incarnation -> {
                            rowsIncarnation[0] = incarnation;
                            return rows;
                        });
        assertThat(documents).hasSize(1);
        assertThat(raw.calls).isZero();
        var task = task();
        var result =
                creator.createTask(
                        CreateTaskRequest.newBuilder()
                                .setParent(options.queue)
                                .setTask(task)
                                .build());
        raw.future.set(task.toBuilder().setName(options.queue + "/tasks/server-id").build());
        result.get();
        assertThat(rows.closed).isFalse();
        creator.close();
        assertThat(raw.closed).isTrue();
        assertThat(rows.closed).isTrue();
        assertThat(raw.calls).isEqualTo(1);
        assertThat(documents).hasSize(2);
        var json = new ObjectMapper();
        var iterator = documents.values().iterator();
        var start = json.readTree(iterator.next());
        var terminal = json.readTree(iterator.next());
        assertThat(rows.rows).hasSize(1);
        var csv = rows.rows.get(0).split(",", -1);
        assertThat(csv[0]).isEqualTo("CT1246");
        assertThat(csv[4]).isEqualTo(start.path("incarnation").asText());
        assertThat(csv[4]).isEqualTo(terminal.path("incarnation").asText());
        assertThat(rowsIncarnation[0]).hasToString(csv[4]);
        assertThat(terminal.path("complete").asBoolean()).isTrue();
        assertThat(terminal.path("observations").asLong()).isEqualTo(1);
        assertThat(terminal.path("rows_exported").asLong()).isEqualTo(1);
        assertThat(terminal.path("parts_closed").asLong()).isEqualTo(1);
        assertThat(terminal.path("rows_flush_failed").asBoolean()).isFalse();
    }

    @Test
    void windowModeExportsRowsToStorageBesideTheReceipts() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        // Construction opens nothing; the first row would address the run's rows prefix.
        assertThat(ObservedSinks.rows(options, UUID.randomUUID())).isInstanceOf(RowsOutput.class);
        assertThat(options.rowsPrefix()).startsWith("gs://").endsWith("/rows/");
    }

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void recordCountModePrintsRowsAndTouchesNoStorage() throws Exception {
        var options =
                MeasurementOptions.parse(MeasurementOptionsTest.arguments("--arm", "UNNAMED"));
        assertThat(ObservedSinks.rows(options, UUID.randomUUID()))
                .isNotInstanceOf(RowsOutput.class);
        var raw = new FakeCreator();
        var creator =
                ObservedSinks.observe(
                        raw,
                        options,
                        (name, data) -> {
                            throw new IOException("storage must not be touched");
                        });
        var output = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            var task = task();
            var result =
                    creator.createTask(
                            CreateTaskRequest.newBuilder()
                                    .setParent(options.queue)
                                    .setTask(task)
                                    .build());
            raw.future.set(task.toBuilder().setName(options.queue + "/tasks/server-id").build());
            result.get();
            creator.close();
        } finally {
            System.setOut(previous);
        }
        assertThat(raw.closed).isTrue();
        var lines = output.toString(StandardCharsets.UTF_8).lines().toList();
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).startsWith("CT1246,");
        assertThat(lines.get(0).split(",", -1)).hasSize(16);
    }

    @Test
    void failedRegistrationClosesTheUnadmittedClient() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        var raw = new FakeCreator();
        assertThatThrownBy(
                        () ->
                                ObservedSinks.observe(
                                        raw,
                                        options,
                                        (name, data) -> {
                                            throw new IOException("registration lost");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("registration lost");
        assertThat(raw.closed).isTrue();
        assertThat(raw.calls).isZero();
    }

    @Test
    void failedRowsOutputConstructionClosesTheUnadmittedClient() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        var raw = new FakeCreator();
        Map<String, String> documents = new LinkedHashMap<>();
        var failure = new IllegalStateException("rows output unavailable");
        assertThatThrownBy(
                        () ->
                                ObservedSinks.observe(
                                        raw,
                                        options,
                                        documents::put,
                                        incarnation -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
        assertThat(documents).hasSize(1);
        assertThat(raw.closed).isTrue();
        assertThat(raw.calls).isZero();
    }

    @Test
    void pluginLinkageFailureClosesClientAndPreservesCause() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        var raw = new FakeCreator();
        var failure = new NoClassDefFoundError("GCS plugin dependency");
        raw.closeFailure = new IllegalStateException("client close failed");
        assertThatThrownBy(
                        () ->
                                ObservedSinks.observe(
                                        raw,
                                        options,
                                        (name, data) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure)
                .hasSuppressedException(raw.closeFailure);
        assertThat(raw.closed).isTrue();
        assertThat(raw.calls).isZero();
    }

    private static final class FakeCreator implements TaskCreator {
        private final SettableApiFuture<Task> future = SettableApiFuture.create();
        private int calls;
        private boolean closed;
        private RuntimeException closeFailure;

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request) {
            calls++;
            return future;
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
            return createTask(request);
        }

        @Override
        public void close() {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
