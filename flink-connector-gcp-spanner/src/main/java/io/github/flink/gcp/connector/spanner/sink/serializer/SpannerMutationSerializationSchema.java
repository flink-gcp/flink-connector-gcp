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

package io.github.flink.gcp.connector.spanner.sink.serializer;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.spanner.Mutation;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Turns a stream record into the Spanner {@link Mutation} the sink writes.
 *
 * <p>The mutation names its own table, so one sink writes to as many tables as this schema produces
 * — everything the sink is configured with is the database around them.
 *
 * <p><b>Which mutation operation to build is a delivery-guarantee decision, not a style one.</b>
 * The sink is at-least-once and Spanner's batch write offers no replay protection, so a mutation
 * may be applied more than once. Each of the five operations answers a replay differently:
 *
 * <ul>
 *   <li>{@link Mutation#newInsertOrUpdateBuilder(String)} and {@link
 *       Mutation#newReplaceBuilder(String)} — idempotent for that mutation.
 *   <li>{@link Mutation#delete(String, com.google.cloud.spanner.Key)} — idempotent, and a delete of
 *       a row that is not there is simply applied.
 *   <li>{@link Mutation#newInsertBuilder(String)} — the replay is rejected with {@code
 *       ALREADY_EXISTS}, which the sink routes to the configured failure handler as a per-mutation
 *       failure.
 *   <li>{@link Mutation#newUpdateBuilder(String)} — idempotent, <em>but</em> a row deleted between
 *       the two attempts answers {@code NOT_FOUND}, which is not a per-mutation refusal and
 *       <b>fails the job</b>. An {@code insertOrUpdate} is the operation to reach for when that
 *       matters.
 * </ul>
 *
 * <p>Separate {@code BatchWrite} mutation groups may be applied in an unspecified order, so this
 * per-mutation property does not order successive records for the same key.
 *
 * <p>Returning {@code null} <b>skips</b> the record: it is written nowhere, is not a failure, never
 * reaches the failure handler, and is counted by the {@code recordsSkipped} metric. Throwing marks
 * the record as failed and routes it instead.
 *
 * <p>Implementations must be serializable — they travel to the task managers with the sink.
 *
 * @param <T> the record type
 */
@Public
public interface SpannerMutationSerializationSchema<T> extends Serializable {

    /**
     * Initializes the schema, once per subtask, before the first {@link #serialize} call.
     *
     * @param context the initialization context
     * @throws Exception if initialization fails, failing the job
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes one record into a mutation.
     *
     * @param element the record
     * @param context the sink writer context
     * @return the mutation to apply, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized, routing it to the failure handler
     */
    @Nullable
    Mutation serialize(T element, SinkWriter.Context context) throws IOException;
}
