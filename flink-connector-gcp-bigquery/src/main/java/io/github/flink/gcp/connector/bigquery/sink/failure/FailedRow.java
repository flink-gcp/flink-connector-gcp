/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.failure;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

/**
 * A single row that terminally failed to be written to BigQuery, as passed to a {@link
 * FailureHandler FailureHandler&lt;BigQueryFailure&gt;}.
 *
 * <p>Carries the serialized protobuf row bytes rather than the original record: the sink writer is
 * stateless and only retains serialized append batches, so by the time a server-side row error is
 * reported the original record object no longer exists. When serialization itself failed, {@link
 * #getRowBytes()} is {@code null}.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@PublicEvolving
public final class FailedRow implements BigQueryFailure {

    private final TableDestination destination;
    private final ByteString rowBytes;
    private final String errorMessage;
    private final Throwable cause;

    private FailedRow(
            TableDestination destination,
            ByteString rowBytes,
            String errorMessage,
            Throwable cause) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.rowBytes = rowBytes;
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed row. Intended for the sink implementation (and tests of custom handlers).
     *
     * @param destination the destination table the row was routed to
     * @param rowBytes the serialized row, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed row
     */
    public static FailedRow of(
            TableDestination destination,
            ByteString rowBytes,
            String errorMessage,
            Throwable cause) {
        return new FailedRow(destination, rowBytes, errorMessage, cause);
    }

    /** Returns the destination table the row was routed to. */
    public TableDestination getDestination() {
        return destination;
    }

    /**
     * Returns the serialized protobuf row bytes, or {@code null} when the record could not be
     * serialized in the first place.
     */
    public ByteString getRowBytes() {
        return rowBytes;
    }

    @Override
    public String getConnector() {
        return "bigquery";
    }

    @Override
    public String describeDestination() {
        return destination.toString();
    }

    /** Returns {@link #getRowBytes()} under the shared {@link FailedElement} contract. */
    @Override
    public ByteString getPayloadBytes() {
        return rowBytes;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "FailedRow{destination="
                + destination
                + ", rowBytes="
                + (rowBytes == null ? "null" : rowBytes.size() + " bytes")
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
