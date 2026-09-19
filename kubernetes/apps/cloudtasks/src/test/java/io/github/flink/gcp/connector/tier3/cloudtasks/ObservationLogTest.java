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

import com.google.cloud.tasks.v2.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ResourceLock(Resources.SYSTEM_OUT)
class ObservationLogTest {
    @Test
    void recordsRestoredOriginWithoutInventingMonotonicLatency() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        var output = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        String name = options.queue + "/tasks/" + "a".repeat(64);
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            new ObservationLog(options)
                    .accept(
                            new ObservedTaskCreator.Observation(
                                    new MeasurementPayload.Origin(19, UUID.randomUUID(), 200, 400),
                                    2,
                                    500,
                                    600,
                                    name,
                                    null,
                                    io.grpc.Status.ALREADY_EXISTS.asRuntimeException()));
        } finally {
            System.setOut(previous);
        }
        String[] fields = output.toString(StandardCharsets.UTF_8).strip().split(",", -1);
        assertThat(fields).hasSize(16);
        assertThat(fields[7]).isEqualTo("19");
        assertThat(fields[9]).isEqualTo("400");
        assertThat(fields[13]).isEqualTo("-1");
        assertThat(fields[14]).isEqualTo("ALREADY_EXISTS");
        assertThat(fields[15]).isEqualTo(name);
    }

    @Test
    void distinguishesCancellationFromDeadlineAndUnknownFailures() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        var output = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        var log = new ObservationLog(options);
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            for (Throwable failure :
                    List.of(
                            new CancellationException(),
                            io.grpc.Status.CANCELLED.asRuntimeException(),
                            io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException(),
                            new IllegalStateException("synthetic"))) {
                log.accept(
                        new ObservedTaskCreator.Observation(
                                new MeasurementPayload.Origin(
                                        1, MeasurementPayload.process(), 10, 20),
                                1,
                                30,
                                40,
                                "",
                                null,
                                failure));
            }
        } finally {
            System.setOut(previous);
        }
        assertThat(
                        output.toString(StandardCharsets.UTF_8)
                                .lines()
                                .map(line -> line.split(",", -1)[14])
                                .toList())
                .containsExactly("CANCELLED", "CANCELLED", "DEADLINE_EXCEEDED", "UNKNOWN");
    }

    @Test
    void refusesAResponseForAnotherTaskOrADispatchedTask() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        String name = options.queue + "/tasks/" + "a".repeat(64);
        var origin = new MeasurementPayload.Origin(1, MeasurementPayload.process(), 10, 20);
        var log = new ObservationLog(options);
        for (Task task :
                new Task[] {
                    Task.newBuilder().setName(name + "b").build(),
                    Task.newBuilder().setName(name).setDispatchCount(1).build(),
                    Task.newBuilder().setName("another-queue/tasks/id").build()
                }) {
            assertThatThrownBy(
                            () ->
                                    log.accept(
                                            new ObservedTaskCreator.Observation(
                                                    origin, 1, 30, 40, name, task, null)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void countsOnlyControlStillRejectsDispatchButProducesNoCsv() {
        var options =
                MeasurementOptions.parse(
                        MeasurementOptionsTest.windowArguments("--emit-attempts", "false"));
        String name = options.queue + "/tasks/" + "a".repeat(64);
        var origin = new MeasurementPayload.Origin(1, MeasurementPayload.process(), 10, 20);
        var log = new ObservationLog(options);
        var output = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            log.accept(
                    new ObservedTaskCreator.Observation(
                            origin,
                            1,
                            30,
                            40,
                            name,
                            Task.newBuilder().setName(name).build(),
                            null));
            assertThatThrownBy(
                            () ->
                                    log.accept(
                                            new ObservedTaskCreator.Observation(
                                                    origin,
                                                    2,
                                                    30,
                                                    40,
                                                    name,
                                                    Task.newBuilder()
                                                            .setName(name)
                                                            .setDispatchCount(1)
                                                            .build(),
                                                    null)))
                    .hasMessageContaining("has dispatched");
        } finally {
            System.setOut(previous);
        }
        assertThat(output.size()).isZero();
    }
}
