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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.annotation.PublicEvolving;

import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

/**
 * An element that terminally failed to be written by one of this project's sinks — the read-only
 * contract every connector's concrete failure type (BigQuery's {@code FailedRow}, Pub/Sub's {@code
 * FailedMessage}, Cloud Tasks' {@code FailedTask}) implements, and the view a cross-connector
 * {@link DeadLetterQueue} sees.
 *
 * <p>Carries the serialized payload rather than the original record: the sink writers are
 * stateless, so by the time a server-side failure is reported the original record object no longer
 * exists. Connector-specific detail beyond this contract (a typed destination, the full request
 * proto) lives on the concrete type.
 *
 * <p>Instances are created by the sinks on the task thread and are not serializable.
 */
@PublicEvolving
public interface FailedElement {

    /**
     * Returns the connector that produced the failure, as a lower-case identifier ({@code
     * "bigquery"}, {@code "pubsub"}, {@code "cloudtasks"}) — stable, so dead-letter consumers can
     * key on it.
     */
    String getConnector();

    /**
     * Returns the destination the element was routed to as a stable resource string, for example
     * {@code my-project.my_dataset.my_table} or {@code projects/p/topics/t}.
     */
    String describeDestination();

    /**
     * Returns the serialized payload, or {@code null} when serialization itself failed and no
     * payload was ever produced.
     */
    @Nullable
    ByteString getPayloadBytes();

    /** Returns the failure description. */
    String getErrorMessage();

    /** Returns the underlying failure, or {@code null} when none is available. */
    @Nullable
    Throwable getCause();
}
