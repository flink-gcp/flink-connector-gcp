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
import org.apache.flink.util.Preconditions;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

/**
 * A single row mutation that terminally failed to be written to Bigtable, as passed to a {@link
 * FailureHandler FailureHandler&lt;FailedMutation&gt;}.
 *
 * <p>Carries the {@link RowMutationEntry} the serializer produced rather than the original record:
 * the sink writer is stateless and retains only mutations, so by the time the service rejects one
 * the original record object no longer exists. When serialization itself failed, {@link
 * #getEntry()} and {@link #getRowKey()} are {@code null}.
 *
 * <p>{@link #getPayloadBytes()} is the serialized {@code MutateRowsRequest.Entry} — the row key and
 * every mutation of it — so a dead-letter consumer recovers the whole mutation with {@code
 * MutateRowsRequest.Entry.parseFrom(bytes)}.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@Public
public final class FailedMutation implements FailedElement {

    private final TableDestination destination;
    @Nullable private final RowMutationEntry entry;
    @Nullable private final MutateRowsRequest.Entry proto;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedMutation(
            TableDestination destination,
            @Nullable RowMutationEntry entry,
            String errorMessage,
            @Nullable Throwable cause) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.entry = entry;
        // Built once: both the row key and the payload bytes are read from it, and the entry
        // itself exposes neither.
        this.proto = entry == null ? null : entry.toProto();
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed mutation. Intended for the sink implementation (and tests of custom
     * handlers).
     *
     * @param destination the table the mutation was routed to
     * @param entry the mutation, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed mutation
     */
    public static FailedMutation of(
            TableDestination destination,
            @Nullable RowMutationEntry entry,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedMutation(destination, entry, errorMessage, cause);
    }

    /** Returns the table the mutation was routed to. */
    public TableDestination getDestination() {
        return destination;
    }

    /**
     * Returns the mutation the serializer produced, or {@code null} when the record could not be
     * serialized in the first place.
     */
    @Nullable
    public RowMutationEntry getEntry() {
        return entry;
    }

    /**
     * Returns the row key the mutation applies to, or {@code null} when the record could not be
     * serialized.
     */
    @Nullable
    public ByteString getRowKey() {
        return proto == null ? null : proto.getRowKey();
    }

    @Override
    public String getConnector() {
        return "bigtable";
    }

    /** Returns the table as {@code project.instance.table}. */
    @Override
    public String describeDestination() {
        return destination.toString();
    }

    /**
     * Returns the serialized mutation — row key and every mutation of it — or {@code null} when
     * serialization itself failed.
     */
    @Override
    @Nullable
    public ByteString getPayloadBytes() {
        return proto == null ? null : proto.toByteString();
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    @Nullable
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "FailedMutation{destination="
                + destination
                + ", rowKey="
                + (proto == null ? "null" : proto.getRowKey().toStringUtf8())
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
