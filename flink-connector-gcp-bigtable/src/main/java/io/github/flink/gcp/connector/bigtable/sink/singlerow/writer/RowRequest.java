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

import com.google.api.core.ApiFuture;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;

import java.io.IOException;

/**
 * One single-row request, built from a record and started against a client once its destination is
 * known.
 *
 * <p>The table is a parameter of {@link #start} rather than of the request: the client's request
 * objects carry their own table id, but the connector routes by {@code DestinationResolver}, which
 * answers per record on the task thread. Building the client's object at start time with the
 * resolved table is what lets the two coexist, and it keeps the client's request object — the one
 * type that would carry a table id — out of what a serializer hands over.
 *
 * @param <R> the answer type: {@code Boolean} for {@code CheckAndMutateRow}, {@code BigtableRow}
 *     for {@code ReadModifyWriteRow}
 */
@Internal
public interface RowRequest<R> {

    /**
     * Returns the RPC this request issues.
     *
     * @return the operation
     */
    RowOperation operation();

    /**
     * Returns the row the request addresses, for failure reporting.
     *
     * @return the row key
     */
    ByteString rowKey();

    /**
     * Issues the request against the table.
     *
     * <p>A runtime counts the request as in flight only once this has returned: a synchronous throw
     * — the client refusing the call, or a rule the client's own builder rejects — means the
     * service was never asked, and is reported as such.
     *
     * @param client the leased client of the table's instance
     * @param destination the table
     * @return the answer, once the service responds; already transformed into connector-owned
     *     types, so no client type reaches a stream
     */
    ApiFuture<R> start(SingleRowClient client, TableDestination destination);

    /**
     * Interprets a successful answer after the runtime counts completion. The async surface calls
     * this on callback threads, so implementations must be thread-safe.
     *
     * @param answer the response to this request
     * @param metrics the runtime's counters
     * @throws IOException if an outcome policy fails the job
     */
    default void onSuccess(Object answer, SingleRowRequestMetrics metrics) throws IOException {}
}
