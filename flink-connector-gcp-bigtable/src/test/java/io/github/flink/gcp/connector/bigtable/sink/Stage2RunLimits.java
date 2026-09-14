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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

/** Explicit local calibration capacities; none is an input-rate limiter or a service approval. */
final class Stage2RunLimits {
    final int inventoryEntries;
    final long inventoryBytes;
    final int stagedEntries;
    final long stagedBytes;
    final long workBytes;
    final long checkpointTimeoutMillis;
    final long drainMillis;

    Stage2RunLimits(
            int inventoryEntries,
            long inventoryBytes,
            int stagedEntries,
            long stagedBytes,
            long workBytes,
            long checkpointTimeoutMillis,
            long drainMillis) {
        if (inventoryEntries < 1
                || inventoryBytes < inventoryEntries * 64L
                || stagedEntries < 1
                || stagedBytes < 1
                || workBytes < 1
                || checkpointTimeoutMillis < 1
                || drainMillis < 1) {
            throw new IllegalArgumentException(
                    "Stage 2 capacities and deadlines must be positive; inventory needs 64 bytes per entry");
        }
        Math.multiplyExact(checkpointTimeoutMillis, 1_000_000L);
        Math.multiplyExact(Math.addExact(drainMillis, 360_000L), 1_000_000L);
        this.inventoryEntries = inventoryEntries;
        this.inventoryBytes = inventoryBytes;
        this.stagedEntries = stagedEntries;
        this.stagedBytes = stagedBytes;
        this.workBytes = workBytes;
        this.checkpointTimeoutMillis = checkpointTimeoutMillis;
        this.drainMillis = drainMillis;
    }

    static Stage2RunLimits historical(int capacity) {
        if (capacity > 1_000_000) {
            throw new IllegalArgumentException(
                    "Historical calibration inventory exceeds its limit");
        }
        return new Stage2RunLimits(
                capacity, 64L << 20, 100_000, 64L << 20, 2L << 30, 60_000, 120_000);
    }

    static Stage2RunLimits read(Path path) throws IOException {
        Properties values = new Properties();
        try (var input = Files.newInputStream(path)) {
            values.load(input);
        }
        if (!values.stringPropertyNames()
                .equals(
                        Set.of(
                                "inventoryEntries",
                                "inventoryBytes",
                                "stagedEntries",
                                "stagedBytes",
                                "workBytes",
                                "checkpointTimeoutMillis",
                                "drainMillis"))) {
            throw new IOException(
                    "Stage 2 limits require exactly the seven documented capacity/deadline fields");
        }
        try {
            return new Stage2RunLimits(
                    Integer.parseInt(values.getProperty("inventoryEntries")),
                    Long.parseLong(values.getProperty("inventoryBytes")),
                    Integer.parseInt(values.getProperty("stagedEntries")),
                    Long.parseLong(values.getProperty("stagedBytes")),
                    Long.parseLong(values.getProperty("workBytes")),
                    Long.parseLong(values.getProperty("checkpointTimeoutMillis")),
                    Long.parseLong(values.getProperty("drainMillis")));
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IOException("Invalid Stage 2 calibration limits", failure);
        }
    }

    String describe() {
        return "inventoryEntries="
                + inventoryEntries
                + " inventoryBytes="
                + inventoryBytes
                + " stagedEntries="
                + stagedEntries
                + " stagedBytes="
                + stagedBytes
                + " workBytes="
                + workBytes
                + " checkpointTimeoutMillis="
                + checkpointTimeoutMillis
                + " drainMillis="
                + drainMillis;
    }
}
