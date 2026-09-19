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
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservationLogTest {
    @Test
    void recordsRestoredOriginWithoutInventingMonotonicLatency() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        var output = new FakeRowsOutput();
        UUID incarnation = UUID.randomUUID();
        String name = options.queue + "/tasks/" + "a".repeat(64);
        new ObservationLog(options, incarnation, output)
                .accept(
                        new ObservedTaskCreator.Observation(
                                new MeasurementPayload.Origin(19, UUID.randomUUID(), 200, 400),
                                2,
                                500,
                                600,
                                name,
                                null,
                                io.grpc.Status.ALREADY_EXISTS.asRuntimeException()));
        assertThat(output.rows).hasSize(1);
        String[] fields = output.rows.get(0).split(",", -1);
        assertThat(fields).hasSize(16);
        assertThat(fields[0]).isEqualTo("CT1246");
        assertThat(fields[4]).isEqualTo(incarnation.toString());
        assertThat(fields[7]).isEqualTo("19");
        assertThat(fields[9]).isEqualTo("400");
        assertThat(fields[13]).isEqualTo("-1");
        assertThat(fields[14]).isEqualTo("ALREADY_EXISTS");
        assertThat(fields[15]).isEqualTo(name);
    }

    @Test
    void distinguishesCancellationFromDeadlineAndUnknownFailures() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        var output = new FakeRowsOutput();
        var log = new ObservationLog(options, UUID.randomUUID(), output);
        for (Throwable failure :
                List.of(
                        new CancellationException(),
                        io.grpc.Status.CANCELLED.asRuntimeException(),
                        io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException(),
                        new IllegalStateException("synthetic"))) {
            log.accept(
                    new ObservedTaskCreator.Observation(
                            new MeasurementPayload.Origin(1, MeasurementPayload.process(), 10, 20),
                            1,
                            30,
                            40,
                            "",
                            null,
                            failure));
        }
        assertThat(output.rows.stream().map(line -> line.split(",", -1)[14]).toList())
                .containsExactly("CANCELLED", "CANCELLED", "DEADLINE_EXCEEDED", "UNKNOWN");
    }

    @Test
    void refusesAResponseForAnotherTaskOrADispatchedTask() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        String name = options.queue + "/tasks/" + "a".repeat(64);
        var origin = new MeasurementPayload.Origin(1, MeasurementPayload.process(), 10, 20);
        var output = new FakeRowsOutput();
        var log = new ObservationLog(options, UUID.randomUUID(), output);
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
        assertThat(output.rows).isEmpty();
    }

    @Test
    void countsOnlyControlStillRejectsDispatchButProducesNoCsv() throws Exception {
        var options =
                MeasurementOptions.parse(
                        MeasurementOptionsTest.windowArguments("--emit-attempts", "false"));
        String name = options.queue + "/tasks/" + "a".repeat(64);
        var origin = new MeasurementPayload.Origin(1, MeasurementPayload.process(), 10, 20);
        var output = new FakeRowsOutput();
        var log = new ObservationLog(options, UUID.randomUUID(), output);
        log.accept(
                new ObservedTaskCreator.Observation(
                        origin, 1, 30, 40, name, Task.newBuilder().setName(name).build(), null));
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
        assertThat(output.rows).isEmpty();
        assertThat(log.exportsRows()).isFalse();
        log.close();
        assertThat(output.closed).isTrue();
        assertThat(log.rowsExported()).isZero();
        assertThat(log.partsClosed()).isZero();
    }

    @Test
    void outputFailureFailsTheObservationWithItsCause() throws Exception {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        var output = new FakeRowsOutput();
        output.rowFailure = new IOException("part lost");
        var log = new ObservationLog(options, UUID.randomUUID(), output);
        assertThatThrownBy(
                        () ->
                                log.accept(
                                        new ObservedTaskCreator.Observation(
                                                new MeasurementPayload.Origin(
                                                        1, MeasurementPayload.process(), 10, 20),
                                                1,
                                                30,
                                                40,
                                                "",
                                                null,
                                                new CancellationException())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Measurement evidence output failed")
                .hasCause(output.rowFailure);
        assertThat(log.exportsRows()).isTrue();
        output.closeFailure = new IOException("flush lost");
        assertThatThrownBy(log::close).isSameAs(output.closeFailure);
    }

    @Test
    void closeAndCountsDelegateToTheOutput() throws Exception {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.arguments());
        var output = new FakeRowsOutput();
        var log = new ObservationLog(options, UUID.randomUUID(), output);
        log.accept(
                new ObservedTaskCreator.Observation(
                        new MeasurementPayload.Origin(1, MeasurementPayload.process(), 10, 20),
                        1,
                        30,
                        40,
                        "",
                        null,
                        new CancellationException()));
        assertThat(log.rowsExported()).isZero();
        log.close();
        assertThat(output.closed).isTrue();
        assertThat(log.rowsExported()).isEqualTo(1);
        assertThat(log.partsClosed()).isEqualTo(1);
    }

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void stdoutOutputPrintsEveryRowAndFailsOnTheStreamErrorFlag() throws Exception {
        var captured = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        var output = ObservationLog.Output.stdout();
        try {
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
            output.row("CT1246,first");
            output.row("CT1246,second");
            assertThat(output.rowsExported()).isEqualTo(2);
            assertThat(output.partsClosed()).isZero();
            System.setOut(
                    new PrintStream(
                            new OutputStream() {
                                @Override
                                public void write(int value) throws IOException {
                                    throw new IOException("stdout closed");
                                }
                            },
                            true,
                            StandardCharsets.UTF_8));
            assertThatThrownBy(() -> output.row("CT1246,third"))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Measurement evidence output failed");
        } finally {
            System.setOut(previous);
        }
        assertThat(captured.toString(StandardCharsets.UTF_8).lines())
                .containsExactly("CT1246,first", "CT1246,second");
        assertThat(output.rowsExported()).isEqualTo(2);
        output.close();
        assertThat(output.rowsExported()).isEqualTo(2);
    }
}
