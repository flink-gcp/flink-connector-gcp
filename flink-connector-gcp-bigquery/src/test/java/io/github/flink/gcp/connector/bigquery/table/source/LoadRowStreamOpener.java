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

package io.github.flink.gcp.connector.bigquery.table.source;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link RowStreamOpener} for full-cache loads that records the order in which streams and the
 * opener itself are opened and released.
 *
 * <p>It models what {@code ReadClientRowStreamOpener} does that a load depends on: it refuses to
 * open a stream once closed, a stream can break part-way through on {@code next()}, which is where
 * the real server stream fails, and releasing the client can fail. Each response block carries one
 * row, so a break can fall after any row.
 *
 * <p>Serializable, as the format that carries it is cloned for every load; the recordings are
 * shared across those copies, keyed by the constructor's id.
 */
final class LoadRowStreamOpener implements RowStreamOpener {

    private static final long serialVersionUID = 1L;

    private static final Map<String, List<String>> EVENTS = new ConcurrentHashMap<>();

    private final String id;
    private final HashMap<String, int[]> rangesByStream;
    private final HashMap<String, Integer> breakAfter;
    private final boolean interruptOnBreak;
    private final boolean failRelease;

    private transient boolean closed;

    private LoadRowStreamOpener(
            String id,
            Map<String, int[]> rangesByStream,
            Map<String, Integer> breakAfter,
            boolean interruptOnBreak,
            boolean failRelease) {
        this.id = id;
        this.rangesByStream = new HashMap<>(rangesByStream);
        this.breakAfter = new HashMap<>(breakAfter);
        this.interruptOnBreak = interruptOnBreak;
        this.failRelease = failRelease;
        EVENTS.put(id, new CopyOnWriteArrayList<>());
    }

    /** An opener whose streams hold the given {@code {firstId, count}} ranges and read cleanly. */
    static LoadRowStreamOpener of(String id, Map<String, int[]> rangesByStream) {
        return new LoadRowStreamOpener(id, rangesByStream, Collections.emptyMap(), false, false);
    }

    /** The same opener, whose given stream breaks on {@code next()} after that many rows. */
    LoadRowStreamOpener breakingAfter(String streamName, int rows) {
        Map<String, Integer> breaks = new HashMap<>(breakAfter);
        breaks.put(streamName, rows);
        return new LoadRowStreamOpener(id, rangesByStream, breaks, interruptOnBreak, failRelease);
    }

    /**
     * The same opener, whose breaks are interrupts of the waiting call: the client library's server
     * stream sets the interrupt again and fails the call, as {@code ServerStreamIterator} does.
     */
    LoadRowStreamOpener interruptingOnBreak() {
        return new LoadRowStreamOpener(id, rangesByStream, breakAfter, true, failRelease);
    }

    /** The same opener, whose release throws an unchecked exception. */
    LoadRowStreamOpener failingRelease() {
        return new LoadRowStreamOpener(id, rangesByStream, breakAfter, interruptOnBreak, true);
    }

    /** Returns what the copies of the opener with this id did, in order. */
    static List<String> events(String id) {
        return new ArrayList<>(EVENTS.getOrDefault(id, Collections.emptyList()));
    }

    @Override
    public RowStream open(String streamName, long offset) throws IOException {
        if (closed) {
            throw new IOException(
                    "The BigQuery read stream opener was closed; the reader is shutting down.");
        }
        int[] range = rangesByStream.get(streamName);
        if (range == null) {
            throw new IOException("No such stream: " + streamName);
        }
        EVENTS.get(id).add("open " + streamName + "@" + offset);
        return new Stream(
                streamName,
                TestRows.blocks(TestRows.rows(range[0], range[1]), 1),
                breakAfter.get(streamName));
    }

    @Override
    public void close() {
        closed = true;
        EVENTS.get(id).add("close opener");
        if (failRelease) {
            throw new IllegalStateException("release failed");
        }
    }

    private final class Stream implements RowStream {

        private final String name;
        private final List<ReadRowsResponse> blocks;
        @Nullable private final Integer breakAfter;
        private int next;

        private Stream(String name, List<ReadRowsResponse> blocks, @Nullable Integer breakAfter) {
            this.name = name;
            this.blocks = blocks;
            this.breakAfter = breakAfter;
        }

        @Override
        public ReadRowsResponse next() {
            if (breakAfter != null && next == breakAfter) {
                if (interruptOnBreak) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(new InterruptedException());
                }
                throw new IllegalStateException("the stream broke");
            }
            return next < blocks.size() ? blocks.get(next++) : null;
        }

        @Override
        public void cancel() {}

        @Override
        public void close() {
            EVENTS.get(id).add("close " + name);
        }
    }
}
