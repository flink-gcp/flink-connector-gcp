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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowsOutputTest {
    @TempDir Path directory;
    private final UUID incarnation = UUID.randomUUID();
    private final AtomicLong clock = new AtomicLong(1_000);

    private ObservationLog.Output storage() {
        return RowsOutput.storage(directory.toUri().toString(), incarnation, clock::get);
    }

    private Path part(int index) {
        return directory.resolve(
                incarnation + "-" + String.format(Locale.ROOT, "%06d", index) + ".csv.gz");
    }

    private List<String> lines(int index) throws IOException {
        try (var reader =
                new BufferedReader(
                        new InputStreamReader(
                                new GZIPInputStream(Files.newInputStream(part(index))),
                                StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

    private List<String> parts() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    void writesGzipRowsToTheFirstPartAndCountsThemOnlyAfterClose() throws Exception {
        var output = storage();
        assertThat(parts()).isEmpty(); // A part is opened by its first row, not by construction.
        output.row("CT1246,one");
        output.row("CT1246,two,ünïcödé");
        assertThat(output.rowsExported()).isZero();
        assertThat(output.partsClosed()).isZero();
        output.close();
        assertThat(output.rowsExported()).isEqualTo(2);
        assertThat(output.partsClosed()).isEqualTo(1);
        assertThat(parts()).containsExactly(part(1).getFileName().toString());
        assertThat(lines(1)).containsExactly("CT1246,one", "CT1246,two,ünïcödé");
        assertThat(Files.readAllBytes(part(1))).startsWith((byte) 0x1f, (byte) 0x8b);
        output.close(); // Idempotent: no second part and no double count.
        assertThat(output.partsClosed()).isEqualTo(1);
    }

    @Test
    void rollsToTheNextPartOnceUncompressedBytesReachTheLimit() throws Exception {
        var output = storage();
        String line = "a".repeat((1 << 20) - 1); // One MiB per row including the newline.
        for (int row = 0; row < 8; row++) {
            output.row(line);
        }
        assertThat(parts()).hasSize(1);
        assertThat(output.rowsExported()).isZero();
        output.row("ninth");
        assertThat(parts()).hasSize(2);
        assertThat(output.rowsExported()).isEqualTo(8);
        assertThat(output.partsClosed()).isEqualTo(1);
        output.close();
        assertThat(output.rowsExported()).isEqualTo(9);
        assertThat(output.partsClosed()).isEqualTo(2);
        assertThat(lines(1)).hasSize(8);
        assertThat(lines(2)).containsExactly("ninth");
        assertThat(RowsOutput.ROLL_BYTES).isEqualTo(8L * 1024 * 1024);
    }

    @Test
    void rollsToTheNextPartOnceTheOpenPartIsOldEnough() throws Exception {
        var output = storage();
        output.row("first");
        clock.addAndGet(RowsOutput.ROLL_NANOS - 1);
        output.row("still first");
        assertThat(parts()).hasSize(1);
        clock.addAndGet(1);
        output.row("second");
        assertThat(parts()).hasSize(2);
        assertThat(output.rowsExported()).isEqualTo(2);
        assertThat(output.partsClosed()).isEqualTo(1);
        output.close();
        assertThat(lines(1)).containsExactly("first", "still first");
        assertThat(lines(2)).containsExactly("second");
        assertThat(RowsOutput.ROLL_NANOS).isEqualTo(60_000_000_000L);
    }

    @Test
    void partIndicesAreContiguousAcrossRolls() throws Exception {
        var output = storage();
        for (int row = 1; row <= 4; row++) {
            output.row("row " + row);
            clock.addAndGet(RowsOutput.ROLL_NANOS);
        }
        output.close();
        assertThat(parts())
                .containsExactly(
                        part(1).getFileName().toString(),
                        part(2).getFileName().toString(),
                        part(3).getFileName().toString(),
                        part(4).getFileName().toString());
        assertThat(output.partsClosed()).isEqualTo(4);
        assertThat(output.rowsExported()).isEqualTo(4);
    }

    @Test
    void refusesToOverwriteAnExistingPart() throws Exception {
        Files.writeString(part(1), "stale");
        var output = storage();
        assertThatThrownBy(() -> output.row("first")).isInstanceOf(IOException.class);
        assertThat(Files.readString(part(1))).isEqualTo("stale");
        assertThatThrownBy(() -> output.row("again"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unusable");
        assertThat(output.rowsExported()).isZero();
        assertThat(output.partsClosed()).isZero();
        output.close();
        assertThat(parts()).hasSize(1);
    }

    @Test
    void closeFailurePropagatesAndLeavesTheOpenPartUncounted() throws Exception {
        var closeFailure = new IOException("close lost");
        var output =
                new RowsOutput(
                        directory.toUri().toString(),
                        incarnation,
                        clock::get,
                        path ->
                                new FilterOutputStream(RowsOutput.create(path)) {
                                    @Override
                                    public void close() throws IOException {
                                        super.close();
                                        throw closeFailure;
                                    }
                                });
        output.row("first");
        clock.addAndGet(RowsOutput.ROLL_NANOS);
        assertThatThrownBy(() -> output.row("second")).isSameAs(closeFailure);
        assertThat(output.rowsExported()).isZero();
        assertThat(output.partsClosed()).isZero();
        assertThatThrownBy(() -> output.row("third")).hasMessageContaining("unusable");
        output.close();
        assertThat(output.rowsExported()).isZero();
        assertThat(parts()).hasSize(1);
    }

    @Test
    void closeFailureAtShutdownPropagatesUnchanged() throws Exception {
        var closeFailure = new IOException("close lost");
        var output =
                new RowsOutput(
                        directory.toUri().toString(),
                        incarnation,
                        clock::get,
                        path ->
                                new FilterOutputStream(RowsOutput.create(path)) {
                                    @Override
                                    public void close() throws IOException {
                                        super.close();
                                        throw closeFailure;
                                    }
                                });
        output.row("only");
        assertThatThrownBy(output::close).isSameAs(closeFailure);
        assertThat(output.rowsExported()).isZero();
        assertThat(output.partsClosed()).isZero();
        assertThatThrownBy(() -> output.row("late")).hasMessageContaining("unusable");
    }

    @Test
    void rejectsRowsAfterClose() throws Exception {
        var output = storage();
        output.close();
        assertThatThrownBy(() -> output.row("late")).hasMessageContaining("closed");
        assertThat(parts()).isEmpty();
    }
}
