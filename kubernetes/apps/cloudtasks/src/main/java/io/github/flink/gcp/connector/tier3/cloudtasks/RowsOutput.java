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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.zip.GZIPOutputStream;

/**
 * Exports evidence rows as gzip-compressed CSV parts through the Flink filesystem API.
 *
 * <p>Parts are named {@code <prefix><incarnation>-<NNNNNN>.csv.gz} with a contiguous index from
 * {@code 000001}, created with {@link FileSystem.WriteMode#NO_OVERWRITE} so a duplicate incarnation
 * fails instead of replacing evidence. A part is opened by the first row it receives and rolled
 * before the next row once it holds {@link #ROLL_BYTES} uncompressed bytes or has been open for
 * {@link #ROLL_NANOS}. Only a closed part is durable and counted; any failure leaves the output
 * unusable so the creator's evidence flags record the loss.
 */
@Internal
final class RowsOutput implements ObservationLog.Output {
    /** Uncompressed bytes after which the next row opens a new part: 8 MiB. */
    static final long ROLL_BYTES = 8L << 20;

    /** Age after which the next row opens a new part: 60 seconds of the injected clock. */
    static final long ROLL_NANOS = TimeUnit.SECONDS.toNanos(60);

    private static final int BUFFER_BYTES = 64 << 10;

    /** Opens the raw part stream; the seam lets tests inject a stream that fails on close. */
    @FunctionalInterface
    interface PartOpener {
        OutputStream open(Path path) throws IOException;
    }

    private final String prefix;
    private final UUID incarnation;
    private final LongSupplier clock;
    private final PartOpener opener;
    private OutputStream part;
    private long partBytes;
    private long partRows;
    private long partOpenedNanos;
    private int parts;
    private long rowsExported;
    private long partsClosed;
    private boolean failed;
    private boolean closed;

    RowsOutput(String prefix, UUID incarnation, LongSupplier clock, PartOpener opener) {
        this.prefix = prefix;
        this.incarnation = incarnation;
        this.clock = clock;
        this.opener = opener;
    }

    static ObservationLog.Output storage(String prefix, UUID incarnation, LongSupplier clock) {
        return new RowsOutput(prefix, incarnation, clock, RowsOutput::create);
    }

    static OutputStream create(Path path) throws IOException {
        return path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
    }

    @Override
    public void row(String line) throws IOException {
        if (failed) {
            throw new IOException("Measurement rows output is unusable after an earlier failure");
        }
        if (closed) {
            throw new IOException("Measurement rows output is closed");
        }
        byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        try {
            if (part != null
                    && (partBytes >= ROLL_BYTES
                            || clock.getAsLong() - partOpenedNanos >= ROLL_NANOS)) {
                closePart();
            }
            if (part == null) {
                openPart();
            }
            part.write(bytes);
            partBytes += bytes.length;
            partRows++;
        } catch (IOException | RuntimeException failure) {
            abandon(failure);
            throw failure;
        }
    }

    @Override
    public long rowsExported() {
        return rowsExported;
    }

    @Override
    public long partsClosed() {
        return partsClosed;
    }

    @Override
    public void close() throws IOException {
        if (closed || failed) {
            return;
        }
        closed = true;
        if (part == null) {
            return;
        }
        try {
            closePart();
        } catch (IOException | RuntimeException failure) {
            abandon(failure);
            throw failure;
        }
    }

    private void openPart() throws IOException {
        Path path =
                new Path(
                        prefix
                                + incarnation
                                + "-"
                                + String.format(Locale.ROOT, "%06d", parts + 1)
                                + ".csv.gz");
        OutputStream raw = opener.open(path);
        try {
            part = new GZIPOutputStream(new BufferedOutputStream(raw, BUFFER_BYTES), BUFFER_BYTES);
        } catch (IOException | RuntimeException failure) {
            try {
                raw.close();
            } catch (IOException | RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        parts++;
        partBytes = 0;
        partRows = 0;
        partOpenedNanos = clock.getAsLong();
    }

    private void closePart() throws IOException {
        OutputStream open = part;
        part = null;
        open.close();
        rowsExported += partRows;
        partsClosed++;
        partRows = 0;
        partBytes = 0;
    }

    private void abandon(Exception failure) {
        failed = true;
        OutputStream open = part;
        part = null;
        if (open != null) {
            try {
                open.close();
            } catch (IOException | RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
