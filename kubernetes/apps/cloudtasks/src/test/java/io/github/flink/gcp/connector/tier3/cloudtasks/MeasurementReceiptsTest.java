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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MeasurementReceiptsTest {
    @TempDir Path directory;

    @Test
    void discoversAnIncarnationBeforeSendingAndExportsIndependentTerminalCounts() throws Exception {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        UUID incarnation = UUID.randomUUID();
        var terminal =
                MeasurementReceipts.creator(
                        options,
                        incarnation,
                        MeasurementReceipts.storage(directory.toUri().toString()));
        Path start = directory.resolve("creator-" + incarnation + "-start.json");
        Path end = directory.resolve("creator-" + incarnation + "-terminal.json");
        assertThat(start).exists();
        assertThat(end).doesNotExist();
        terminal.accept(new ObservedTaskCreator.Terminal(3, 3, 3, false, false, false));
        var json = new ObjectMapper();
        var birth = json.readTree(Files.readString(start));
        var result = json.readTree(Files.readString(end));
        assertThat(result.path("attempts").asLong()).isEqualTo(3);
        assertThat(result.path("complete").asBoolean()).isTrue();
        assertThat(result.path("incarnation").asText()).isEqualTo(incarnation.toString());
        assertThat(result.path("process")).isEqualTo(birth.path("process"));
        assertThat(result.path("run_id").asText()).isEqualTo(options.runId);
        assertThat(result.path("cell_id").asText()).isEqualTo(options.cellId);
        assertThat(birth.path("attempt_limit").asLong()).isEqualTo(options.attemptLimit);
        assertThatThrownBy(
                        () ->
                                terminal.accept(
                                        new ObservedTaskCreator.Terminal(
                                                4, 4, 4, false, false, false)))
                .isInstanceOf(java.io.UncheckedIOException.class);
        assertThat(json.readTree(Files.readString(end)).path("attempts").asLong()).isEqualTo(3);
    }

    @Test
    void failedRegistrationCannotReturnAUsableRecorder() {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        assertThatThrownBy(
                        () ->
                                MeasurementReceipts.creator(
                                        options,
                                        UUID.randomUUID(),
                                        (name, data) -> {
                                            throw new IOException("registration failed");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("registration failed");
    }

    @Test
    void sourceReceiptsDescribeActualMappingIncludingARestoredStartingSequence() throws Exception {
        var options = MeasurementOptions.parse(MeasurementOptionsTest.windowArguments());
        Map<String, String> documents = new LinkedHashMap<>();
        var input = new MeasurementInput(options, documents::put);
        assertThat(input.map(17L)).isEqualTo(17L);
        assertThat(input.map(18L)).isEqualTo(18L);
        assertThat(documents).hasSize(1);
        assertThat(input.map(options.records - 1)).isEqualTo(options.records - 1);
        assertThat(documents).hasSize(2);
        var json = new ObjectMapper();
        var values = documents.values().iterator();
        var start = json.readTree(values.next());
        var end = json.readTree(values.next());
        assertThat(start.path("sequence").asLong()).isEqualTo(17);
        assertThat(end.path("sequence").asLong()).isEqualTo(options.records - 1);
        assertThat(start.path("incarnation")).isEqualTo(end.path("incarnation"));
        assertThat(start.path("observation_seconds").asInt()).isEqualTo(180);
        assertThat(start.path("wall_millis").asLong()).isPositive();
        assertThat(documents.keySet()).anyMatch(name -> name.endsWith("last-mapped"));
    }

    @Test
    void sourceStopsOnEvidenceFailureAndLegacyModeDoesNotAccessStorage() throws Exception {
        MeasurementReceipts.Output failed =
                (name, data) -> {
                    throw new IOException("lost");
                };
        var input =
                new MeasurementInput(
                        MeasurementOptions.parse(MeasurementOptionsTest.windowArguments()), failed);
        assertThatThrownBy(() -> input.map(0L)).hasMessage("lost");
        var legacy =
                new MeasurementInput(
                        MeasurementOptions.parse(MeasurementOptionsTest.arguments()), failed);
        assertThat(legacy.map(0L)).isZero();
        assertThat(legacy.map(99L)).isEqualTo(99L);
    }
}
