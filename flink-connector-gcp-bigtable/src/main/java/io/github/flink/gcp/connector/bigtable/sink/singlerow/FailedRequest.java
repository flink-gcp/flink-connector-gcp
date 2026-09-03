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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

/**
 * A single-row request that Bigtable rejected as invalid, or that could not be built from its
 * record, as passed to a {@link FailureHandler FailureHandler&lt;FailedRequest&gt;} on the sink
 * surface.
 *
 * <p>Only row-level failures arrive here — an {@code INVALID_ARGUMENT} the service returned, or an
 * exception the request builder threw. An ambiguous failure, where the service may have applied the
 * request, and every other failure fail the job instead, since a handler that drops one could hide
 * a write that happened.
 *
 * <p>{@link #getPayloadBytes()} returns {@code null}: this runtime holds the request as the
 * client's own object, whose serialized form is the client's internal API, and the connector-owned
 * request models that the per-operation sinks add decide the dead-letter encoding. {@link
 * #getOperation()} and {@link #getRowKey()} identify the request until then.
 *
 * <p>Instances are created by the runtime and are not serializable.
 */
@PublicEvolving
public final class FailedRequest implements FailedElement {

    private final TableDestination destination;
    @Nullable private final RowOperation operation;
    @Nullable private final ByteString rowKey;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedRequest(
            TableDestination destination,
            @Nullable RowOperation operation,
            @Nullable ByteString rowKey,
            String errorMessage,
            @Nullable Throwable cause) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.operation = operation;
        this.rowKey = rowKey;
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed request. Intended for the runtime (and tests of custom handlers).
     *
     * @param destination the table the request was routed to
     * @param operation the RPC, or {@code null} when the request could not be built from its record
     * @param rowKey the row the request addressed, or {@code null} when the request could not be
     *     built
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed request
     */
    public static FailedRequest of(
            TableDestination destination,
            @Nullable RowOperation operation,
            @Nullable ByteString rowKey,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedRequest(destination, operation, rowKey, errorMessage, cause);
    }

    /**
     * Returns the table the request was routed to.
     *
     * @return the destination
     */
    public TableDestination getDestination() {
        return destination;
    }

    /**
     * Returns the RPC the request would have issued, or {@code null} when the request could not be
     * built from its record in the first place.
     *
     * @return the operation, or {@code null}
     */
    @Nullable
    public RowOperation getOperation() {
        return operation;
    }

    /**
     * Returns the row the request addressed, or {@code null} when the request could not be built.
     *
     * @return the row key, or {@code null}
     */
    @Nullable
    public ByteString getRowKey() {
        return rowKey;
    }

    /**
     * Returns {@code "bigtable"}.
     *
     * @return the connector name
     */
    @Override
    public String getConnector() {
        return "bigtable";
    }

    /**
     * Returns the table as {@code project.instance.table}.
     *
     * @return the destination rendering
     */
    @Override
    public String describeDestination() {
        return destination.toString();
    }

    /**
     * Returns {@code null}: the request's serialized form is the client's internal API, and the
     * connector-owned request models of the per-operation sinks decide the dead-letter encoding.
     *
     * @return {@code null}
     */
    @Override
    @Nullable
    public ByteString getPayloadBytes() {
        return null;
    }

    /**
     * Returns the failure description.
     *
     * @return the error message
     */
    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns the underlying failure, or {@code null}.
     *
     * @return the cause, or {@code null}
     */
    @Override
    @Nullable
    public Throwable getCause() {
        return cause;
    }

    /**
     * Renders the failure as its siblings do: the destination, the operation, the length of the row
     * key rather than the key itself, and the message. A row key is arbitrary bytes and the row's
     * own data; a handler that wants it has {@link #getRowKey()}.
     *
     * @return the rendering
     */
    @Override
    public String toString() {
        return "FailedRequest{destination="
                + destination
                + ", operation="
                + operation
                + ", rowKey="
                + (rowKey == null ? "null" : rowKey.size() + " bytes")
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
