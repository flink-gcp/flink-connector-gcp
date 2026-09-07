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

package io.github.flink.gcp.connector.bigtable.sink;

import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2EmulatorITCase extends AbstractBigtableEmulatorITCase {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void diskInventoryReadbackChecksBothTransportArms(boolean staged) throws Exception {
        TableDestination table =
                createTable(
                        "stage2-readback-" + staged, FAMILY, StagedMutationTestSink.MARKER_FAMILY);
        assertThat(EMULATOR.getHost()).isIn("localhost", "127.0.0.1");
        try (Stage2Harness run =
                new Stage2Harness(
                        table,
                        "127.0.0.1:" + EMULATOR.getEmulatorPort(),
                        directory.resolve("inventory"),
                        64,
                        1024,
                        true,
                        false,
                        4,
                        false,
                        null)) {
            run.startWindow(System.nanoTime(), 0, TimeUnit.HOURS.toNanos(1));
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run, directory, staged, true, 2, 24, 100, false, null, false)) {
                job.finish();
                assertThat(run.ledger.summary().count).isEqualTo(24);
                run.readback(staged, true);
                String row = run.input(0).row().toStringUtf8();
                if (staged) {
                    writeCell(
                            table,
                            row,
                            StagedMutationTestSink.MARKER_FAMILY,
                            run.ledger.entry(0).marker,
                            0,
                            "corrupted");
                    assertThatThrownBy(() -> run.readback(true, true))
                            .hasMessageContaining("Marker readback");
                } else {
                    writeCell(table, row, FAMILY, "q", 1000, "corrupted-payload");
                    assertThatThrownBy(() -> run.readback(false, true))
                            .hasMessageContaining("Payload");
                }
                assertThat(run.attempts).isEmpty();
                assertThat(run.originalFutures).isEmpty();
            }
        }
    }
}
