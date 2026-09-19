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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

/** Separate storage receipts for discovering incarnations and reconciling exported logs. */
@Internal
final class MeasurementReceipts {
    private MeasurementReceipts() {}

    @FunctionalInterface
    interface Output {
        void write(String name, String document) throws IOException;
    }

    static Output storage(MeasurementOptions options) {
        return storage(options.receiptPrefix());
    }

    static Output storage(String prefix) {
        return (name, document) -> {
            Path path = new Path(prefix + name + ".json");
            try (var output =
                    path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
                output.write((document + "\n").getBytes(StandardCharsets.UTF_8));
            }
        };
    }

    static Consumer<ObservedTaskCreator.Terminal> creator(
            MeasurementOptions options, UUID incarnation, Output output) throws IOException {
        String identity = identity(options, "creator", incarnation);
        output.write(
                "creator-" + incarnation + "-start",
                identity
                        + ",\"attempt_limit\":"
                        + options.attemptLimit
                        + ",\"wall_millis\":"
                        + System.currentTimeMillis()
                        + ",\"monotonic_nanos\":"
                        + System.nanoTime()
                        + "}");
        return terminal -> {
            try {
                output.write(
                        "creator-" + incarnation + "-terminal",
                        identity
                                + ",\"attempts\":"
                                + terminal.attempts()
                                + ",\"completed\":"
                                + terminal.completed()
                                + ",\"observations\":"
                                + terminal.observations()
                                + ",\"evidence_failed\":"
                                + terminal.evidenceFailed()
                                + ",\"limit_reached\":"
                                + terminal.limitReached()
                                + ",\"client_close_failed\":"
                                + terminal.clientCloseFailed()
                                + ",\"rows_exported\":"
                                + terminal.rowsExported()
                                + ",\"parts_closed\":"
                                + terminal.partsClosed()
                                + ",\"rows_flush_failed\":"
                                + terminal.rowsFlushFailed()
                                + ",\"complete\":"
                                + terminal.complete()
                                + ",\"wall_millis\":"
                                + System.currentTimeMillis()
                                + ",\"monotonic_nanos\":"
                                + System.nanoTime()
                                + "}");
            } catch (IOException failure) {
                throw new UncheckedIOException("Measurement terminal receipt failed", failure);
            }
        };
    }

    static String identity(MeasurementOptions options, String role, UUID incarnation) {
        // Labels and the enum were validated before serialization; no arbitrary text is emitted.
        return "{\"version\":1,\"run_id\":\""
                + options.runId
                + "\",\"cell_id\":\""
                + options.cellId
                + "\",\"arm\":\""
                + options.arm
                + "\",\"role\":\""
                + role
                + "\",\"incarnation\":\""
                + incarnation
                + "\",\"process\":\""
                + MeasurementPayload.process()
                + "\""
                + ",\"control_delay_millis\":"
                + options.controlDelayMillis
                + ",\"csv_enabled\":"
                + options.emitAttempts;
    }
}
