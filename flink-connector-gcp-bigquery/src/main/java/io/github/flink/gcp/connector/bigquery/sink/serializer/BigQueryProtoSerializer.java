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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializes records into protobuf rows to be written to BigQuery.
 *
 * <p>{@link #getTableSchema(TableDestination)} is the source of truth for a destination's schema
 * and is available <em>before</em> any record is written to it: the sink uses it for table creation
 * ({@code CREATE_IF_NEEDED}), for load-job schemas, and — through {@link
 * #getDescriptor(TableDestination)} — for the Storage Write API stream schema. The bytes returned
 * by {@link #serialize(Object)} must be valid serialized messages of the descriptor returned for
 * the record's destination.
 *
 * <p>{@link #getDescriptor(TableDestination)} has a default implementation deriving the wire
 * descriptor from the table schema; implementations that cache the derived descriptor should
 * override it.
 *
 * <p>This is deliberately an abstract class rather than a functional interface: implementations are
 * shipped inside the Flink job graph and must be {@link Serializable}, while protobuf {@link
 * Descriptors.Descriptor} instances are <em>not</em> serializable. Descriptors must therefore be
 * held only in {@code transient} fields that are rebuilt lazily after deserialization, or obtained
 * statically (for example from generated message classes).
 *
 * <p>Exception contract: {@link #serialize(Object)} throws {@link IOException} for per-record
 * serialization failures (which sinks may route to error handling); configuration errors (for
 * example schema mapping problems) surface as unchecked exceptions at initialization time.
 *
 * <p>Returning {@code null} from {@link #serialize(Object)} skips the record: it is written nowhere
 * and is not a failure. Every serializer of this connector family reads {@code null} that way, so a
 * filter that depends on the row being built belongs here rather than upstream of the sink.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public abstract class BigQueryProtoSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Returns the BigQuery table schema of rows written to the given destination.
     *
     * <p>Invoked when the sink first prepares a destination (table creation, write-stream or
     * load-job schema derivation), not per record.
     *
     * @param destination the destination table
     * @return the table schema of that destination
     */
    public abstract TableSchema getTableSchema(TableDestination destination);

    /**
     * Returns the protobuf descriptor describing the serialized rows written to the given
     * destination.
     *
     * <p>The default implementation derives it from {@link #getTableSchema(TableDestination)} on
     * every call; implementations should override this to return a cached descriptor.
     *
     * @param destination the destination table
     * @return the descriptor of the rows written to that destination
     */
    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
        try {
            return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                    getTableSchema(destination));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor for " + destination,
                    e);
        }
    }

    /**
     * Returns a cheap token identifying the destination's current schema, or {@code null} (the
     * default) for serializers whose schema never changes while the job runs.
     *
     * <p>Sinks compare the token per record (with {@link java.util.Objects#equals}) against the
     * token captured when the destination's write stream was opened, and refresh the stream —
     * rebuilding the row descriptor via {@link #getDescriptor(TableDestination)} and, where schema
     * updates are enabled, reconciling the destination table's schema — <em>before</em> appending
     * rows serialized under a changed schema. Implementations with evolving schemas should return a
     * value that changes whenever {@link #getTableSchema(TableDestination)} would return a
     * different schema: a version counter, the cached descriptor instance, or similar. The call
     * must be O(1); deriving a fingerprint from the schema on every call defeats its purpose.
     *
     * <p>Schema evolution contract: rows already handed to the sink are retained as serialized
     * bytes and are never re-encoded, so an evolved schema must keep previously serialized bytes
     * valid. Appending new fields (at the end, including inside nested types) and relaxing {@code
     * REQUIRED} fields to {@code NULLABLE} are wire-compatible; removing, reordering or re-typing
     * fields is not and leads to corrupt rows or failed appends.
     *
     * @param destination the destination table
     * @return the schema fingerprint, or {@code null} for static schemas
     */
    public Object getSchemaFingerprint(TableDestination destination) {
        return null;
    }

    /**
     * Serializes a record into protobuf row bytes.
     *
     * <p>For record types that already are protobuf messages this is typically {@code
     * message.toByteString()}.
     *
     * @param element the record
     * @return the serialized protobuf row, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized; the record is handed to the sink's
     *     failure handler, which fails the job by default
     */
    @Nullable
    public abstract ByteString serialize(T element) throws IOException;
}
