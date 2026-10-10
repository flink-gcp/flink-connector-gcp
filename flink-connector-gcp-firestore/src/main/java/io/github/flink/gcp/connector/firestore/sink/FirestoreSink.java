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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;

import io.github.flink.gcp.connector.base.failure.FailureHandler;

/**
 * Entry point for building a Firestore sink.
 *
 * <p>The sink applies one {@link FirestoreWrite} per record to a Firestore database in Native mode
 * through the client library's {@code BulkWriter}, at-least-once, and waits for everything it holds
 * at each checkpoint barrier. A write names its own document path, so one sink writes to as many
 * collections of the configured database as its serializer produces; collections need no creating,
 * and the database must exist.
 *
 * <p><b>Replay is the serializer's problem to make harmless.</b> A write may be applied more than
 * once — after a job restart, and also within one attempt when a request whose outcome never
 * arrived is retried. What that costs depends on the operation the serializer built:
 *
 * <ul>
 *   <li>{@code set} and {@code setMerge} — idempotent for that write.
 *   <li>{@code delete} — idempotent, and deleting a missing document succeeds.
 *   <li>{@code create} — the replay is refused with {@code ALREADY_EXISTS}, which is routed to the
 *       failure handler as a per-write failure.
 *   <li>{@code add} — the record is stored again under another drawn id: a restart's serializer
 *       draws a new one, and a retried create is refused with {@code ALREADY_EXISTS} and sent again
 *       under a new id.
 *   <li>{@code update} — idempotent, <em>but</em> if the document was deleted between the two
 *       attempts Firestore answers {@code NOT_FOUND}, which is not a per-write refusal and <b>fails
 *       the job</b>.
 *   <li>a write carrying a {@code lastUpdateTime} precondition — the replay is refused with {@code
 *       FAILED_PRECONDITION}, which {@link
 *       FirestoreSinkBuilder#preconditionFailurePolicy(PreconditionFailurePolicy)} decides.
 * </ul>
 *
 * <p>This per-write property is not a same-document ordering guarantee: the sink sends several
 * requests at once and retries a failed write in a later one, so two writes to one document may be
 * applied in either order.
 *
 * <p>That at-least-once statement assumes the default {@code FailureHandler.failJob()} policy.
 * Under a dropping policy configured through {@link
 * FirestoreSinkBuilder#failedWriteHandler(FailureHandler)}, a completed checkpoint means every
 * record up to the barrier was either applied, skipped by the serializer, or handed to that
 * handler.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocFirestoreExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<OrderEvent> sink =
 *         FirestoreSink.<OrderEvent>builder()
 *                 .database(DatabaseDestination.of("my-project"))
 *                 .serializer(
 *                         (event, context) ->
 *                                 FirestoreWrite.set(
 *                                         "orders/" + event.getId(),
 *                                         Map.of("total", event.getTotal())))
 *                 .build();
 * }</pre>
 */
@PublicEvolving
public final class FirestoreSink {

    private FirestoreSink() {}

    /**
     * Creates a new {@link FirestoreSinkBuilder}.
     *
     * <p>The sink returned by the builder implements {@link
     * org.apache.flink.streaming.api.lineage.LineageVertexProvider} and returns an empty physical
     * dataset list. A database does not establish the collections a user serializer will write, and
     * lineage extraction never calls that serializer or opens a client. Flink 2.x extracts lineage
     * metadata automatically; Flink 1.20 supports direct inspection but not native listener
     * delivery.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> FirestoreSinkBuilder<T> builder() {
        return new FirestoreSinkBuilder<>();
    }
}
