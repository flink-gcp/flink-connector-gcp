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

package io.github.flink.gcp.connector.datastore.source;

import com.google.auth.Credentials;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The production page reader against the emulator, which records the first page of every split a
 * reader starts and holds reads back after a few pages until the test opens the gate.
 *
 * <p>A split's first page is told apart by its query with the paging fields cleared: each task
 * attempt deserializes its own instance, so the first page an instance reads of a key range is the
 * start of that split in that attempt — a fresh split's at no cursor, a restored one's at the
 * cursor its checkpoint recorded.
 *
 * <p>The gate is what lets a checkpoint land mid-split: without it the emulator answers so fast
 * that the whole read finishes before the first checkpoint that covers an entity. The records are
 * static because the reader is shipped into the job, which runs in this JVM; one test uses it at a
 * time and {@link #reset()} clears it.
 */
final class GatedPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    static final List<RunQueryRequest> STARTED_SPLITS = new CopyOnWriteArrayList<>();
    static final AtomicBoolean GATE_OPEN = new AtomicBoolean();
    private static final AtomicInteger PAGES = new AtomicInteger();

    /**
     * When every held page gives up waiting: one deadline for the whole run, set by the first page
     * the gate holds, so a gate that never opens costs a minute rather than a minute per page.
     */
    private static final AtomicReference<Long> DEADLINE = new AtomicReference<>();

    private final ClientQueryPageReader delegate;
    private final int freePages;

    @Nullable private transient Set<Query> started;

    GatedPageReader(String emulatorEndpoint, int freePages) {
        this.delegate =
                new ClientQueryPageReader(EmulatorEndpoint.parse(emulatorEndpoint, "endpoint"));
        this.freePages = freePages;
    }

    static void reset() {
        STARTED_SPLITS.clear();
        GATE_OPEN.set(false);
        PAGES.set(0);
        DEADLINE.set(null);
    }

    @Override
    public QueryResultBatch read(DatabaseDestination database, RunQueryRequest page)
            throws IOException {
        recordStart(page);
        if (PAGES.incrementAndGet() > freePages) {
            // Not 0 as "unset": System.nanoTime() may be any long, zero and negatives included.
            DEADLINE.compareAndSet(null, System.nanoTime() + 60_000_000_000L);
            long deadline = DEADLINE.get();
            while (!GATE_OPEN.get() && System.nanoTime() - deadline < 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted at the gate.", e);
                }
            }
        }
        return delegate.read(database, page);
    }

    private synchronized void recordStart(RunQueryRequest page) {
        if (started == null) {
            started = new HashSet<>();
        }
        Query range =
                page.getQuery().toBuilder().clearStartCursor().clearLimit().clearOffset().build();
        if (started.add(range)) {
            STARTED_SPLITS.add(page);
        }
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        delegate.useCredentials(credentials);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
