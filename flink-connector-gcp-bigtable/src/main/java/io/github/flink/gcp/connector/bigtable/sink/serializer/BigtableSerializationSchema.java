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

package io.github.flink.gcp.connector.bigtable.sink.serializer;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializes sink records into Bigtable row mutations.
 *
 * <p>Implementations return a full {@link RowMutationEntry}, so the row key and every mutation a
 * row accepts — {@code setCell}, {@code deleteCells}, {@code deleteFamily}, {@code deleteRow}, and
 * the aggregate {@code addToCell} and {@code mergeToCell} — are expressible, and one record may
 * carry several of them. The two aggregate mutations take a value model the client library has not
 * settled, so a client upgrade may move their argument types. The mutations of one entry are
 * applied in their listed order and atomically; entries are not ordered against each other, even
 * for the same row or across concurrent requests.
 *
 * <p>SDK 2.82.0's {@code mergeToCell} convenience overload encodes accumulator input as {@code
 * raw_value}, which the service rejected for Int64 Sum. The <a
 * href="https://flink-gcp.github.io/flink-connector-gcp/docs/examples/bigtable/#updating-aggregate-cells">aggregate
 * example</a> builds {@code bytes_value} through the client's public beta protobuf wrappers and
 * explains their validation and timestamp caveats.
 * <!-- javadoc-example file="JavadocBigtableExamples.java" tag="serialization-schema" -->
 *
 * <pre>{@code
 * (record, context) ->
 *         RowMutationEntry.create(record.getId())
 *                 .setCell(
 *                         "cf",
 *                         "payload",
 *                         record.getTimestampMicros(),
 *                         record.getBody());
 * }</pre>
 *
 * <p><b>Cell timestamps select the version that a replay addresses.</b> The sink is at-least-once,
 * so a record may be written twice after a failure. A {@code setCell} carrying a stable timestamp
 * from the record — the event time, or {@code context.timestamp()} — addresses the same cell
 * version after Flink serializes the record again. The three-argument {@code setCell} instead
 * stamps the mutation from the writer's wall clock: the client reuses that mutation for its own RPC
 * retries, but a Flink recovery builds a new one with a new timestamp and can add another cell
 * version. The table's garbage-collection policy then decides the fate of that version.
 *
 * <p>For aggregate {@code addToCell} and {@code mergeToCell}, a stable timestamp selects the same
 * aggregate cell after replay; it does not deduplicate an input or accumulator. A Sum contribution
 * can be applied again. An immediate replacement combines an unbounded {@code deleteCells} with
 * {@code setCell} in one entry, in that order. Replaying that entry after a newer write can delete
 * the newer value even when the replacement has an explicit timestamp.
 *
 * <p>Returning {@code null} skips the record: it is written nowhere and is not a failure. Every
 * serializer of this connector family reads {@code null} that way, and so does the {@code
 * BaseRowMutationSerializer} of google/flink-connector-gcp, whose signature this one shares — a
 * serializer written against that connector ports by changing the interface name.
 *
 * @param <T> type of the records written by the sink
 */
@Public
public interface BigtableSerializationSchema<T> extends Serializable {

    /**
     * Initialization hook invoked once before serialization starts, on the task that runs the
     * writer. The default implementation does nothing.
     *
     * @param context contextual information for initialization (metrics, user code class loader)
     * @throws Exception if initialization fails; fails the writer creation
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes the given record into a row mutation.
     *
     * @param element the record
     * @param context the write context, carrying the record's timestamp and the current watermark
     * @return the mutation to apply, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized; the record is handed to the sink's
     *     failed-mutation handler, which fails the job by default
     */
    @Nullable
    RowMutationEntry serialize(T element, SinkWriter.Context context) throws IOException;
}
