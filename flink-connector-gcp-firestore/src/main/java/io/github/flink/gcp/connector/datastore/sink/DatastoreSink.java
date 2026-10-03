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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.annotation.PublicEvolving;

import io.github.flink.gcp.connector.base.failure.FailureHandler;

/**
 * Entry point for building a Datastore sink.
 *
 * <p>The sink applies one {@link DatastoreMutation} per record to a Firestore database in Datastore
 * mode through non-transactional commits, at-least-once, and commits everything it holds at each
 * checkpoint barrier. A write's key names its own kind and namespace, so one sink writes to as many
 * kinds of the configured database as its serializer produces; kinds need no creating, and the
 * database must exist.
 *
 * <p><b>Replay is the serializer's problem to make harmless.</b> A write may be applied more than
 * once — after a job restart, and also within one attempt, when a commit whose outcome never
 * arrived is retried or a refused commit is re-sent one write at a time. What that costs depends on
 * the operation the serializer built:
 *
 * <ul>
 *   <li>{@code upsert} and {@code delete} — idempotent for that write; deleting a missing entity
 *       succeeds.
 *   <li>{@code insert} — the replay is refused with {@code ALREADY_EXISTS}, which is routed to the
 *       failure handler as a per-write failure.
 *   <li>{@code update} — idempotent, <em>but</em> if the entity was deleted between the two
 *       attempts Datastore answers {@code NOT_FOUND}, which is routed to the failure handler once
 *       the sink has confirmed that the database itself is there.
 * </ul>
 *
 * <p>Within one subtask, writes to the same key are applied in the order the serializer returned
 * them, unless the service applies an attempt that timed out on the client after a later one.
 *
 * <p>That at-least-once statement assumes the default {@code FailureHandler.failJob()} policy.
 * Under a dropping policy configured through {@link
 * DatastoreSinkBuilder#failedMutationHandler(FailureHandler)}, a completed checkpoint means every
 * record up to the barrier was either applied, skipped by the serializer, or handed to that
 * handler.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocDatastoreExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<OrderEvent> sink =
 *         DatastoreSink.<OrderEvent>builder()
 *                 .database(DatabaseDestination.of("my-project"))
 *                 .serializer(
 *                         (event, context) ->
 *                                 DatastoreMutation.upsert(
 *                                         Entity.newBuilder(
 *                                                         Key.newBuilder(
 *                                                                         "my-project",
 *                                                                         "Order",
 *                                                                         event.getId())
 *                                                                 .build())
 *                                                 .set("total", event.getTotal())
 *                                                 .build()))
 *                 .build();
 * }</pre>
 */
@PublicEvolving
public final class DatastoreSink {

    private DatastoreSink() {}

    /**
     * Creates a new {@link DatastoreSinkBuilder}.
     *
     * <p>The sink returned by the builder implements {@link
     * org.apache.flink.streaming.api.lineage.LineageVertexProvider} and returns an empty physical
     * dataset list. A database does not establish the kinds a user serializer will write, and
     * lineage extraction never calls that serializer or opens a client. Flink 2.x extracts lineage
     * metadata automatically; Flink 1.20 supports direct inspection but not native listener
     * delivery.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> DatastoreSinkBuilder<T> builder() {
        return new DatastoreSinkBuilder<>();
    }
}
