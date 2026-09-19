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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2LedgerTest {
    @TempDir Path directory;

    /** Below one scan block, exactly one block, and a final short block after two full blocks. */
    @ParameterizedTest
    @ValueSource(ints = {7, Stage2Ledger.SCAN_SLOTS, Stage2Ledger.SCAN_SLOTS * 2 + 7})
    void scanVisitsEverySlotInOrderWithTheSameContentAsPointReads(int capacity) throws Exception {
        try (Stage2Ledger ledger =
                new Stage2Ledger(directory.resolve("scan.bin"), capacity, 64L * capacity)) {
            ledger.window(1_000, 2_000);
            ledger.admit(0, 500);
            ledger.acknowledge(0, 600);
            List<Long> measured = new ArrayList<>(List.of(3L));
            if (capacity > Stage2Ledger.SCAN_SLOTS) {
                // Fully populated slots on both sides of the first block boundary.
                measured.add((long) Stage2Ledger.SCAN_SLOTS - 1);
                measured.add((long) Stage2Ledger.SCAN_SLOTS);
            }
            for (long sequence : measured) {
                // Distinct timestamps inside the measured window, 400 apart per slot.
                ledger.admit(sequence, 1_500 + sequence % 100);
                ledger.marker(sequence, String.format("%032x", sequence));
                ledger.acknowledge(sequence, 1_900 + sequence % 100);
            }
            // The last slot is populated so a block-position drift cannot hide in zero slots.
            ledger.admit(capacity - 1, 2_500);
            ledger.marker(capacity - 1, "0123456789abcdef0123456789abcdef");
            ledger.acknowledge(capacity - 1, 2_600);
            List<Long> visited = new ArrayList<>();
            ledger.scan(
                    (sequence, entry) -> {
                        visited.add(sequence);
                        // Positional reads let the visitor seek through the same file.
                        Stage2Ledger.Entry direct = ledger.entry(sequence);
                        assertThat(entry.status)
                                .as("status of slot %d", sequence)
                                .isEqualTo(direct.status);
                        assertThat(entry.phase)
                                .as("phase of slot %d", sequence)
                                .isEqualTo(direct.phase);
                        assertThat(entry.admittedAt)
                                .as("admittedAt of slot %d", sequence)
                                .isEqualTo(direct.admittedAt);
                        assertThat(entry.acknowledgedAt)
                                .as("acknowledgedAt of slot %d", sequence)
                                .isEqualTo(direct.acknowledgedAt);
                        assertThat(entry.marker)
                                .as("marker of slot %d", sequence)
                                .isEqualTo(direct.marker);
                    });
            assertThat(visited).isEqualTo(LongStream.range(0, capacity).boxed().toList());
            assertThat(ledger.entry(0).phase).isEqualTo(Stage2Ledger.WARMUP);
            assertThat(ledger.entry(0).marker).isEmpty();
            assertThat(ledger.entry(3).phase).isEqualTo(Stage2Ledger.MEASURED);
            assertThat(ledger.entry(3).marker).isEqualTo(String.format("%032x", 3));
            assertThat(ledger.entry(capacity - 1).phase).isEqualTo(Stage2Ledger.TAIL);
            assertThat(ledger.entry(1).status).isZero();
            // The summary scans the same blocks and counts only the acknowledged measured slots.
            Stage2Ledger.Summary summary = ledger.summary();
            assertThat(summary.count).isEqualTo(measured.size());
            assertThat(summary.p99).isEqualTo(400);
        }
    }

    @Test
    void scanPropagatesTheVisitorsFailureAndStopsEarly() throws Exception {
        try (Stage2Ledger ledger = new Stage2Ledger(directory.resolve("stop.bin"), 100, 6_400)) {
            int[] seen = {0};
            assertThatThrownBy(
                            () ->
                                    ledger.scan(
                                            (sequence, entry) -> {
                                                if (++seen[0] == 10) {
                                                    throw new IOException("stop at ten");
                                                }
                                            }))
                    .isInstanceOf(IOException.class)
                    .hasMessage("stop at ten");
            assertThat(seen[0]).isEqualTo(10);
        }
    }

    @Test
    void scanRefusesAnInventoryShorterThanItsCapacity() throws Exception {
        Path path = directory.resolve("short.bin");
        try (Stage2Ledger ledger = new Stage2Ledger(path, 100, 6_400)) {
            try (RandomAccessFile truncating = new RandomAccessFile(path.toFile(), "rw")) {
                truncating.setLength(64L * 50);
            }
            int[] seen = {0};
            assertThatThrownBy(() -> ledger.scan((sequence, entry) -> seen[0]++))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("shorter than its capacity");
            assertThat(seen[0]).isZero();
        }
    }
}
