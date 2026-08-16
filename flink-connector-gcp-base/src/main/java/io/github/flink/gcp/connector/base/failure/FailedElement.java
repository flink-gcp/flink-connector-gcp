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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.annotation.Public;

import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

/**
 * An element that terminally failed to be written by one of this project's sinks — the read-only
 * contract every connector's concrete failure type implements, and the view a cross-connector
 * {@link DeadLetterQueue} sees.
 *
 * <p>Carries portable payload bytes chosen by the concrete failure type. A sink-created failure
 * normally carries the serialized service request because the original record may no longer exist
 * when an asynchronous failure arrives; a failure created during destination resolution can carry
 * resolver-supplied bytes instead. Connector-specific detail beyond this contract (a typed
 * destination, the full request proto) lives on the concrete type.
 *
 * <p>Failures reach handlers on the task thread and are not serializable. Depending on the
 * connector contract, the sink or a user-supplied resolver may create the concrete instance.
 */
@Public
public interface FailedElement {

    /**
     * Returns the connector that produced the failure, as a lower-case identifier ({@code
     * "bigquery"}, {@code "pubsub"}, {@code "cloudtasks"}) — stable, so dead-letter consumers can
     * key on it.
     */
    String getConnector();

    /**
     * Returns the destination the element was routed to as a stable resource string, for example
     * {@code my-project.my_dataset.my_table} or {@code projects/p/topics/t}. A connector may return
     * a stable sentinel such as {@code unresolved} when destination resolution itself failed.
     */
    String describeDestination();

    /**
     * Returns portable payload bytes for inspection or replay, or {@code null} when no payload was
     * available. The concrete failure type defines whether these are a serialized service request
     * or record bytes supplied at an earlier boundary.
     */
    @Nullable
    ByteString getPayloadBytes();

    /** Returns the failure description. */
    String getErrorMessage();

    /** Returns the underlying failure, or {@code null} when none is available. */
    @Nullable
    Throwable getCause();
}
