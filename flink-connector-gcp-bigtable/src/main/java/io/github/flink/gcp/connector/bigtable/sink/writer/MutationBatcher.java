/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

/**
 * Accepts row mutations for one table and reports each one's outcome separately.
 *
 * <p>The production implementation wraps the client's bulk mutation batcher, which is what makes
 * the per-entry outcome available at all: an own {@code bulkMutateRowsAsync} loop would have to
 * re-implement the thresholding and the retry partitioning the client already does, and would lose
 * the per-entry future the sink's failure routing is built on.
 *
 * <p>This interface exists rather than the client's {@code Batcher} because that type is
 * {@code @InternalExtensionOnly}: implementing it — which the writer's unit tests must, to exercise
 * failures the emulator cannot produce — is not something its contract allows. It is also narrower,
 * naming only the four operations the writer performs.
 *
 * <p>Implementations are used from the Flink task thread only, matching the client batcher's own
 * single-thread contract, and are not thread-safe.
 */
@Internal
public interface MutationBatcher extends AutoCloseable {

    /**
     * Accepts a mutation, to be sent with the batch it lands in.
     *
     * @param entry the mutation
     * @return a future completing when this mutation has been applied, or exceptionally with this
     *     mutation's own error
     */
    ApiFuture<Void> add(RowMutationEntry entry);

    /**
     * Sends what has accumulated without waiting for it, so the caller can wait on the mailbox
     * instead of on this thread.
     */
    void sendOutstanding();

    /**
     * Starts the shutdown without waiting for it: sends what has accumulated and refuses further
     * mutations, so a caller holding several batchers can start every one before waiting on any.
     *
     * <p>This exists because {@link #close()} has no bound (see there), and a writer with
     * per-record destinations holds one batcher per table: closing them one after another costs the
     * sum of their waits, and a teardown that overruns Flink's {@code task.cancellation.timeout}
     * turns a cancelling task into a fatal TaskManager error. Calling this first makes those waits
     * overlap. The Pub/Sub sink's publisher teardown is two-phase for the same reason.
     *
     * <p>Idempotent, and calling it is optional: {@link #close()} alone still sends, waits and
     * releases. Implementations must not report a failure here — the report belongs to {@code
     * close()}, whose contract below covers it.
     */
    void shutdown();

    /**
     * Sends what has accumulated, waits for every outstanding mutation, and releases what this
     * batcher holds.
     *
     * <p>An implementation must not report a failure it has already delivered through the future
     * {@link #add} returned for that mutation. The writer consumes every one of those futures and
     * applies the sink's policy there, so a second report at shutdown would fail a job that policy
     * had deliberately kept running — which is why the production implementation absorbs the one
     * its client batcher raises (#238).
     *
     * <p>That report is a property of gax's {@code Batcher}, not of wrapping a client: its {@code
     * BatcherStats} reads every entry's result future and accumulates the failures for the
     * batcher's lifetime, never clearing them, so {@code close()} rebuilds them into one exception
     * — every failure by count, though only the first 50 messages of each kind, which it keeps in
     * an {@code EvictingQueue}. #325 measured the other connectors' clients for the same shape; the
     * rule, and what has it, are in the detailed repository guidance.
     */
    @Override
    void close() throws Exception;
}
