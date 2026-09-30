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

package io.github.flink.gcp.connector.firestore.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a stream record into the {@link FirestoreWrite} the sink applies.
 *
 * <p>The write names its own document path, so one sink writes to as many collections as this
 * schema produces — everything the sink is configured with is the database around them.
 *
 * <p><b>Which operation to build is a delivery-guarantee decision, not a style one.</b> The sink is
 * at-least-once, so a write may be applied more than once — after a job restart, and within one
 * attempt when a request whose outcome never arrived is retried. Each operation answers a replay
 * differently:
 *
 * <ul>
 *   <li>{@link FirestoreWrite#set} and {@link FirestoreWrite#setMerge} — idempotent for that write.
 *   <li>{@link FirestoreWrite#delete(String)} — idempotent, and deleting a missing document
 *       succeeds.
 *   <li>{@link FirestoreWrite#create} — the replay is refused with {@code ALREADY_EXISTS}, which
 *       the sink routes to the configured failure handler as a per-write failure.
 *   <li>{@link FirestoreWrite#update(String, java.util.Map)} — idempotent, <em>but</em> a document
 *       deleted between the two attempts answers {@code NOT_FOUND}, which is not a per-write
 *       refusal and <b>fails the job</b>. {@link FirestoreWrite#setMerge} is the operation to reach
 *       for when that matters.
 *   <li>A write carrying a {@code lastUpdateTime} precondition — the first application changes the
 *       update time, so a replay is refused with {@code FAILED_PRECONDITION}.
 * </ul>
 *
 * <p>Writes to the same document are not ordered: the sink sends several requests at once and
 * retries a failed write in a later one, so two records for one document may be applied in either
 * order.
 *
 * <p>Returning {@code null} <b>skips</b> the record: it is written nowhere, is not a failure, never
 * reaches the failure handler, and is counted by the {@code recordsSkipped} metric. Throwing —
 * including the {@link IllegalArgumentException} a malformed {@link FirestoreWrite} raises — marks
 * the record as failed and routes it instead.
 *
 * <p>Implementations must be serializable — they travel to the task managers with the sink.
 *
 * @param <T> the record type
 */
@PublicEvolving
public interface FirestoreWriteSerializationSchema<T> extends Serializable {

    /**
     * Initializes the schema, once per subtask, before the first {@link #serialize} call.
     *
     * @param context the initialization context
     * @throws Exception if initialization fails, failing the job
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes one record into a document write.
     *
     * @param element the record
     * @param context the sink writer context
     * @return the write to apply, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized, routing it to the failure handler
     */
    @Nullable
    FirestoreWrite serialize(T element, SinkWriter.Context context) throws IOException;
}
