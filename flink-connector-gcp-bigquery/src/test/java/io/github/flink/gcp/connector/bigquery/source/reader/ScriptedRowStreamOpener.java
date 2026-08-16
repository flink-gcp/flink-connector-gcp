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

package io.github.flink.gcp.connector.bigquery.source.reader;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.github.flink.gcp.connector.bigquery.source.TestRows;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * A {@link RowStreamOpener} over an in-memory table that <em>honours the requested offset</em>.
 *
 * <p>That is the whole reason it exists: the BigQuery emulator ignores the offset and answers every
 * call from row zero, so resume behaviour cannot be tested against it. Real BigQuery does honour it
 * (measured 2026-08-09), and this fake is what lets the reader's resume be tested
 * deterministically.
 *
 * <p>Serializable, so a job running on a MiniCluster can be built around it.
 */
public final class ScriptedRowStreamOpener implements RowStreamOpener {

    private static final long serialVersionUID = 1L;

    /**
     * Shared across the copies deserialization makes, so a test can read back what a job's readers
     * asked for. Keyed by the id the constructor takes.
     */
    private static final Map<String, List<String>> OPENS = new ConcurrentHashMap<>();

    private static final Map<String, AtomicInteger> CLOSES = new ConcurrentHashMap<>();

    /**
     * Holds a stream after its first block until the test says it may continue.
     *
     * <p>What makes a failover test deterministic rather than paced: the job cannot read the table
     * to its end before the condition the failure depends on is true. Waited on the fetcher thread,
     * which is where a real stream waits for the network — never on the thread a barrier travels.
     */
    private static final Map<String, BooleanSupplier> GATES = new ConcurrentHashMap<>();

    private final String id;

    /**
     * The rows each stream holds, as {@code {firstId, count}}.
     *
     * <p>Ranges rather than the records themselves: this opener travels in the job graph, and an
     * Avro {@code GenericData.Record} is not serializable.
     */
    private final Map<String, int[]> rangesByStream;

    private final int blockSize;
    private final long blockDelayMillis;

    /**
     * Creates the opener.
     *
     * @param id identifies this opener's recordings across serialization
     * @param rangesByStream the {@code {firstId, count}} range each stream holds
     * @param blockSize how many rows one response block carries
     */
    public ScriptedRowStreamOpener(String id, Map<String, int[]> rangesByStream, int blockSize) {
        this(id, rangesByStream, blockSize, 0);
    }

    /**
     * Creates an opener that paces its blocks.
     *
     * @param id identifies this opener's recordings across serialization
     * @param rangesByStream the {@code {firstId, count}} range each stream holds
     * @param blockSize how many rows one response block carries
     * @param blockDelayMillis how long each block takes to arrive, so a job over this opener runs
     *     long enough for checkpoints to complete while it reads. Waited on the fetcher thread,
     *     where a real one waits for the network, and never on the thread a barrier travels.
     */
    public ScriptedRowStreamOpener(
            String id, Map<String, int[]> rangesByStream, int blockSize, long blockDelayMillis) {
        this.id = id;
        this.rangesByStream = rangesByStream;
        this.blockSize = blockSize;
        this.blockDelayMillis = blockDelayMillis;
        OPENS.computeIfAbsent(id, key -> new CopyOnWriteArrayList<>());
        CLOSES.computeIfAbsent(id, key -> new AtomicInteger());
    }

    /** Creates an opener over one stream holding rows {@code 0..rowCount}. */
    public static ScriptedRowStreamOpener singleStream(
            String id, String streamName, int rowCount, int blockSize) {
        return new ScriptedRowStreamOpener(
                id, Collections.singletonMap(streamName, new int[] {0, rowCount}), blockSize);
    }

    @Override
    public RowStream open(String streamName, long offset) throws IOException {
        int[] range = rangesByStream.get(streamName);
        if (range == null) {
            throw new IOException("No such stream: " + streamName);
        }
        List<GenericRecord> rows = TestRows.rows(range[0], range[1]);
        OPENS.get(id).add(streamName + "@" + offset);
        List<GenericRecord> remaining =
                offset >= rows.size()
                        ? Collections.emptyList()
                        : new ArrayList<>(rows.subList((int) offset, rows.size()));
        return new ScriptedRowStream(
                TestRows.blocks(remaining, blockSize), blockDelayMillis, GATES.get(id));
    }

    @Override
    public void close() {
        CLOSES.get(id).incrementAndGet();
    }

    /** Returns the {@code stream@offset} pairs this opener was asked for, in order. */
    public static List<String> opens(String id) {
        return new ArrayList<>(OPENS.getOrDefault(id, Collections.emptyList()));
    }

    /** Returns the offsets this opener was asked to open at, in order. */
    public static List<Long> offsets(String id) {
        List<Long> offsets = new ArrayList<>();
        for (String open : OPENS.getOrDefault(id, Collections.emptyList())) {
            offsets.add(Long.parseLong(open.substring(open.lastIndexOf('@') + 1)));
        }
        return offsets;
    }

    /** Returns how many times {@link #close()} was called for the given id. */
    public static int closeCount(String id) {
        return CLOSES.getOrDefault(id, new AtomicInteger()).get();
    }

    /** Forgets the recordings of the given id, and any gate it had. */
    public static void reset(String id) {
        OPENS.put(id, new CopyOnWriteArrayList<>());
        CLOSES.put(id, new AtomicInteger());
        GATES.remove(id);
    }

    /**
     * Holds every stream of the given id after its first block until the condition is true.
     *
     * @param id the opener id
     * @param open answers whether the streams may continue
     */
    public static void gate(String id, BooleanSupplier open) {
        GATES.put(id, open);
    }

    /** A stream over a fixed list of blocks. */
    private static final class ScriptedRowStream implements RowStream {

        private final List<ReadRowsResponse> blocks;
        private final long blockDelayMillis;
        @Nullable private final BooleanSupplier gate;
        private int next;
        private volatile boolean cancelled;

        private ScriptedRowStream(
                List<ReadRowsResponse> blocks,
                long blockDelayMillis,
                @Nullable BooleanSupplier gate) {
            this.blocks = blocks;
            this.blockDelayMillis = blockDelayMillis;
            this.gate = gate;
        }

        @Override
        public ReadRowsResponse next() {
            if (next > 0 && gate != null) {
                awaitGate();
            }
            if (blockDelayMillis > 0) {
                sleep(blockDelayMillis);
            }
            if (cancelled) {
                throw new IllegalStateException("The stream was cancelled.");
            }
            return next < blocks.size() ? blocks.get(next++) : null;
        }

        private void awaitGate() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!gate.getAsBoolean() && !cancelled) {
                if (System.nanoTime() - deadline > 0) {
                    throw new IllegalStateException(
                            "The stream's gate never opened; the condition the test waits for did"
                                    + " not become true within 60s.");
                }
                sleep(5);
            }
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for a block.", e);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void close() {
            cancelled = true;
        }
    }
}
