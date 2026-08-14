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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;

import java.io.IOException;

/**
 * An explicit, record-specific destination-resolution failure.
 *
 * <p>The resolver owns the payload because the sink cannot serialize a record without first knowing
 * its destination schema. The configured BigQuery failure handler decides whether this failure
 * fails the job, is dropped, or is sent to a dead-letter queue.
 *
 * <p>Instances reach the failure handler on the task thread and are not serializable.
 */
@PublicEvolving
public final class UnroutableRecord extends DestinationResolution implements BigQueryFailure {

    private static final String UNRESOLVED_DESTINATION = "unresolved";

    private final ByteString payloadBytes;
    private final String errorMessage;

    private UnroutableRecord(ByteString payloadBytes, String errorMessage) {
        this.payloadBytes =
                Preconditions.checkNotNull(payloadBytes, "payloadBytes must not be null");
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
    }

    /**
     * Creates an explicit routing failure.
     *
     * @param payloadBytes bytes a failure handler or dead-letter consumer can inspect or replay
     * @param errorMessage why this record could not be routed
     * @return the routing failure
     */
    public static UnroutableRecord of(ByteString payloadBytes, String errorMessage) {
        return new UnroutableRecord(payloadBytes, errorMessage);
    }

    @Override
    <T> void accept(
            T element,
            SinkWriter.Context context,
            DestinationResolutionDispatcher.Visitor<T> visitor)
            throws IOException {
        visitor.visit(this, element, context);
    }

    @Override
    public String getConnector() {
        return "bigquery";
    }

    @Override
    public String describeDestination() {
        return UNRESOLVED_DESTINATION;
    }

    @Override
    public ByteString getPayloadBytes() {
        return payloadBytes;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public Throwable getCause() {
        return null;
    }

    @Override
    public String toString() {
        return "UnroutableRecord{payloadBytes="
                + payloadBytes.size()
                + " bytes, errorMessage="
                + errorMessage
                + "}";
    }
}
