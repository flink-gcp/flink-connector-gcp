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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Leases {@link SingleRowClient}s per table over instance-shared data clients, the single-row
 * family's counterpart of the batching sink's {@code MutationBatcherFactory}.
 *
 * <p>The contract is a lease: {@link #create} hands out a client for one table and counts that
 * table against its instance's client, {@link #release} gives the table's lease back, and the
 * instance's client closes once no table holds it. The runtime that owns the factory calls every
 * method from the task thread, and {@link #close} last, after every lease it took has been released
 * or every request over them has been cancelled.
 *
 * <p>Serializable because the runtime that carries it is built on the job manager and shipped to
 * every subtask; an implementation keeps its clients as transient runtime state.
 */
@Internal
public interface SingleRowClientFactory extends Serializable, AutoCloseable {

    /**
     * Leases a client for a table, building its instance's client on the instance's first lease.
     *
     * @param destination the table
     * @return the client, valid until the table's lease is released
     * @throws IOException if the instance's client cannot be built
     * @throws InterruptedException if interrupted while waiting for a client slot
     */
    SingleRowClient create(TableDestination destination) throws IOException, InterruptedException;

    /**
     * Releases a table's lease; the instance's client closes once no table holds it.
     *
     * @param destination the table
     * @throws Exception if the release fails
     */
    void release(TableDestination destination) throws Exception;

    /**
     * Closes every client this factory built, whether or not its leases were released.
     *
     * @throws Exception if a client fails to close
     */
    @Override
    void close() throws Exception;
}
