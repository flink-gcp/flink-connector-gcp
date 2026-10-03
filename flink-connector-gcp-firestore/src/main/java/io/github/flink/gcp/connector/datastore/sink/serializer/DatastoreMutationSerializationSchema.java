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

package io.github.flink.gcp.connector.datastore.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a stream record into the {@link DatastoreMutation} the sink applies.
 *
 * <p>The write's key names its own kind and namespace, so one sink writes to as many kinds as this
 * schema produces — everything the sink is configured with is the database around them.
 *
 * <p><b>Which operation to build is a delivery-guarantee decision, not a style one.</b> The sink is
 * at-least-once, so a write may be applied more than once — after a job restart, and within one
 * attempt when a request whose outcome never arrived is retried, or when a refused request is
 * re-sent one write at a time. Each operation answers a replay differently:
 *
 * <ul>
 *   <li>{@link DatastoreMutation#upsert} and {@link DatastoreMutation#delete} — idempotent for that
 *       write; deleting a missing entity succeeds.
 *   <li>{@link DatastoreMutation#insert} — the replay is refused with {@code ALREADY_EXISTS}, which
 *       the sink routes to the configured failure handler as a per-write failure.
 *   <li>{@link DatastoreMutation#update} — idempotent, <em>but</em> an entity deleted between the
 *       two attempts answers {@code NOT_FOUND}, which the sink routes once it has confirmed that
 *       the database itself is there. {@link DatastoreMutation#upsert} is the operation to reach
 *       for when that matters.
 * </ul>
 *
 * <p>Within one subtask, writes to the same key are applied in the order this schema returned them:
 * the sink sends one request at a time and never puts two writes to one key in the same request.
 * The exception is an attempt that timed out on the client, which the service may still apply after
 * a later request. Writes from different subtasks are not ordered against each other.
 *
 * <p>Returning {@code null} <b>skips</b> the record: it is written nowhere, is not a failure, never
 * reaches the failure handler, and is counted by the {@code recordsSkipped} metric. Throwing —
 * including the {@link IllegalArgumentException} a malformed {@link DatastoreMutation} raises —
 * marks the record as failed and routes it instead.
 *
 * <p>Implementations must be serializable — they travel to the task managers with the sink.
 *
 * @param <T> the record type
 */
@PublicEvolving
public interface DatastoreMutationSerializationSchema<T> extends Serializable {

    /**
     * Initializes the schema, once per subtask, before the first {@link #serialize} call.
     *
     * @param context the initialization context
     * @throws Exception if initialization fails, failing the job
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes one record into an entity write.
     *
     * @param element the record
     * @param context the sink writer context
     * @return the write to apply, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized, routing it to the failure handler
     */
    @Nullable
    DatastoreMutation serialize(T element, SinkWriter.Context context) throws IOException;
}
