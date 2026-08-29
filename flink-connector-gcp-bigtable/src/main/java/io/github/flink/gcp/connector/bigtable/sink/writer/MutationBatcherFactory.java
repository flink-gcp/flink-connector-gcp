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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Creates one {@link MutationBatcher} per table the writer resolves, and owns whatever those
 * batchers share.
 *
 * <p>Serializable because the factory is shipped in the job graph; implementations must create all
 * client state at {@link #create(TableDestination)} time, not at construction time. Every method is
 * called from the Flink task thread only.
 *
 * <p>A batcher is bound to one table, so the writer holds one per destination. What sits under them
 * is the implementation's business: the production one shares a client across the tables of an
 * instance. Closing one batcher therefore cannot release the shared client by itself; the writer
 * follows it with {@link #release(TableDestination)}, and this factory remains {@link
 * AutoCloseable} for anything still held when the writer closes.
 */
@Internal
public interface MutationBatcherFactory extends Serializable, AutoCloseable {

    /**
     * Creates the mutation batcher for one table.
     *
     * @param destination the table the batcher writes to
     * @return the batcher, owned by the caller
     * @throws IOException if the client cannot be created
     * @throws InterruptedException if the caller is interrupted while waiting for a client slot
     */
    MutationBatcher create(TableDestination destination) throws IOException, InterruptedException;

    /**
     * Releases the factory's ownership associated with one successfully created batcher.
     *
     * <p>The caller closes that batcher before invoking this method. Production implementations may
     * keep an instance client alive while sibling tables still use it and close it when the last
     * table releases its batcher.
     *
     * @param destination the table whose batcher was closed
     * @throws Exception if the shared resource cannot be released
     */
    void release(TableDestination destination) throws Exception;

    /**
     * Releases anything still held behind the batchers it created — the shared clients.
     *
     * <p>Called after every batcher it created has been closed, so an implementation may assume
     * nothing is still writing through them. An implementation holding nothing does nothing.
     */
    @Override
    void close() throws Exception;
}
