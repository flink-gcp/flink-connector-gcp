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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** The preregistered performance matrix; generating it never creates a service lease. */
final class Stage2AssessmentPlan {
    private Stage2AssessmentPlan() {}

    static final class Cell {
        final int payloadBytes;
        final boolean hot;
        final int parallelism;
        final int inFlight;
        final int checkpointSeconds;

        Cell(int payloadBytes, boolean hot, int parallelism, int inFlight, int checkpointSeconds) {
            if (!List.of(1024, 65536).contains(payloadBytes)
                    || !List.of(1, 4, 16).contains(parallelism)
                    || !List.of(1, 4, 16).contains(inFlight)
                    || !List.of(1, 10, 60).contains(checkpointSeconds)) {
                throw new IllegalArgumentException("Not a preregistered Stage 2 cell");
            }
            this.payloadBytes = payloadBytes;
            this.hot = hot;
            this.parallelism = parallelism;
            this.inFlight = inFlight;
            this.checkpointSeconds = checkpointSeconds;
        }

        String id() {
            return "b"
                    + payloadBytes
                    + "-"
                    + (hot ? "hot" : "even")
                    + "-p"
                    + parallelism
                    + "-i"
                    + inFlight
                    + "-c"
                    + checkpointSeconds;
        }

        int warmupSeconds() {
            return Math.max(10, checkpointSeconds);
        }

        int measurementSeconds() {
            return Math.max(30, 3 * checkpointSeconds);
        }
    }

    static final class Run {
        final Cell cell;
        final int repetition;
        final boolean staged;

        Run(Cell cell, int repetition, boolean staged) {
            this.cell = java.util.Objects.requireNonNull(cell, "cell");
            if (repetition < 1 || repetition > 3) {
                throw new IllegalArgumentException("Stage 2 requires repetitions 1 through 3");
            }
            this.repetition = repetition;
            this.staged = staged;
        }

        String table() {
            return cell.id() + "-r" + repetition + "-" + (staged ? "staged" : "bulk");
        }
    }

    static List<Run> runs() {
        List<Run> runs = new ArrayList<>();
        for (int bytes : List.of(1024, 65536)) {
            for (boolean hot : List.of(false, true)) {
                for (int parallelism : List.of(1, 4, 16)) {
                    for (int inFlight : List.of(1, 4, 16)) {
                        for (int interval : List.of(1, 10, 60)) {
                            Cell cell = new Cell(bytes, hot, parallelism, inFlight, interval);
                            for (int repetition = 1; repetition <= 3; repetition++) {
                                boolean stagedFirst = repetition == 2;
                                runs.add(new Run(cell, repetition, stagedFirst));
                                runs.add(new Run(cell, repetition, !stagedFirst));
                            }
                        }
                    }
                }
            }
        }
        return List.copyOf(runs);
    }

    static void write(Path output) throws IOException {
        try (var writer = Files.newBufferedWriter(output, StandardOpenOption.CREATE_NEW)) {
            writer.write(
                    "order,cell,table,arm,repetition,payloadBytes,keys,parallelism,inFlight,"
                            + "checkpointSeconds,warmupSeconds,measurementSeconds\n");
            int order = 0;
            for (Run run : runs()) {
                Cell cell = run.cell;
                writer.write(
                        String.format(
                                Locale.ROOT,
                                "%d,%s,%s,%s,%d,%d,%s,%d,%d,%d,%d,%d%n",
                                ++order,
                                cell.id(),
                                run.table(),
                                run.staged ? "staged" : "bulk",
                                run.repetition,
                                cell.payloadBytes,
                                cell.hot ? "hot" : "even",
                                cell.parallelism,
                                cell.inFlight,
                                cell.checkpointSeconds,
                                cell.warmupSeconds(),
                                cell.measurementSeconds()));
            }
        }
    }

    static Run run(String table) {
        return runs().stream()
                .filter(run -> run.table().equals(table))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Not a formal Stage 2 table: " + table));
    }
}
