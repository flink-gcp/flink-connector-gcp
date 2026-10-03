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

package io.github.flink.gcp.connector.firestore.source;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The production page reader against the emulator, which records the query of every split a reader
 * starts and holds reads back after a few pages until the test opens the gate.
 *
 * <p>The gate is what lets a checkpoint land mid-split: without it the emulator answers so fast
 * that the whole read finishes before the first checkpoint that covers a document. The records are
 * static because the reader is shipped into the job, which runs in this JVM; one test uses it at a
 * time and {@link #reset()} clears it.
 */
final class GatedPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    static final List<RunQueryRequest> STARTED_SPLITS = new CopyOnWriteArrayList<>();
    static final AtomicBoolean GATE_OPEN = new AtomicBoolean();
    private static final AtomicInteger PAGES = new AtomicInteger();

    private final ClientQueryPageReader delegate;
    private final int freePages;

    GatedPageReader(String emulatorEndpoint, int freePages) {
        this.delegate =
                new ClientQueryPageReader(EmulatorEndpoint.parse(emulatorEndpoint, "endpoint"));
        this.freePages = freePages;
    }

    static void reset() {
        STARTED_SPLITS.clear();
        GATE_OPEN.set(false);
        PAGES.set(0);
    }

    @Override
    public Query query(DatabaseDestination database, RunQueryRequest request) throws IOException {
        STARTED_SPLITS.add(request);
        return delegate.query(database, request);
    }

    @Override
    public List<? extends DocumentSnapshot> read(Query page, Timestamp readTime)
            throws IOException {
        if (PAGES.incrementAndGet() > freePages) {
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (!GATE_OPEN.get() && System.nanoTime() < deadline) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted at the gate.", e);
                }
            }
        }
        return delegate.read(page, readTime);
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
