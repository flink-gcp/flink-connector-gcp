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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Public;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;

/**
 * Entry point for building a Bigtable sink.
 *
 * <p>By default the sink applies one row mutation per record through the client's bulk {@code
 * MutateRows} batcher, at-least-once, and waits for every outstanding mutation at each checkpoint
 * barrier. A replayed record overwrites the same cells only when the serializer sets explicit cell
 * timestamps; see {@code BigtableSerializationSchema}.
 *
 * <p>That at-least-once statement assumes the default {@code FailureHandler.failJob()} policy.
 * Under a dropping policy configured through {@link
 * BigtableSinkBuilder#failedMutationHandler(FailureHandler)}, a completed checkpoint means every
 * record up to the barrier was either applied, skipped by the serializer, or handed to that
 * handler.
 *
 * <p>The sink writes to one fixed table, or to a table it resolves per record: {@link
 * BigtableSinkBuilder#table(TableDestination)} names one, and {@link
 * BigtableSinkBuilder#destinationResolver(DestinationResolver)} routes each record to a table of
 * its own. By default it never creates a table — every table it writes to, and its column families,
 * must exist; {@link BigtableSinkBuilder#createDisposition(CreateDisposition)} with {@link
 * CreateDisposition#CREATE_IF_NEEDED} and {@link
 * BigtableSinkBuilder#tableCreateOptions(TableCreateOptions)} opts into creating them, from one
 * schema that serves every table the sink creates.
 *
 * <p>{@link BigtableSinkBuilder#deliveryGuarantee(BigtableDeliveryGuarantee)} can instead select
 * experimental checkpoint-owned writes. That mode stages immutable envelopes and applies each after
 * checkpoint completion through a same-row retained marker, with no cross-row atomicity or
 * rollback. It requires an explicit transactional profile, a pre-provisioned marker family without
 * a GC rule, and preserved checkpoint state. Final service recovery and performance acceptance
 * remain required before release; see {@link BigtableStagedOptions}.
 *
 * <p>The builder-returned sink implements {@code LineageVertexProvider}. Its effective fixed
 * destination reports namespace {@code bigtable://{project}/{instance}}, name {@code {table}} and a
 * {@code gcp} physical-resource facet. Dynamic destinations produce an empty dataset list, without
 * resolver evaluation. Extraction calls no user schema and opens no client. The supported Flink 2.x
 * versions extract the metadata automatically; Flink 1.20 supports direct inspection only.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocBigtableExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<OrderEvent> sink =
 *         BigtableSink.<OrderEvent>builder()
 *                 .table(TableDestination.of("my-project", "my-instance", "orders"))
 *                 .serializer(
 *                         (event, context) ->
 *                                 RowMutationEntry.create(event.getId())
 *                                         .setCell(
 *                                                 "cf",
 *                                                 "payload",
 *                                                 event.getTimestampMicros(),
 *                                                 event.getBody()))
 *                 .build();
 * }</pre>
 */
@Public
public final class BigtableSink {

    private BigtableSink() {}

    /**
     * Creates a new {@link BigtableSinkBuilder}.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> BigtableSinkBuilder<T> builder() {
        return new BigtableSinkBuilder<>();
    }
}
