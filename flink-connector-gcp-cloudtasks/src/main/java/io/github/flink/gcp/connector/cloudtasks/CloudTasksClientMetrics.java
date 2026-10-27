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

package io.github.flink.gcp.connector.cloudtasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;

/**
 * The Cloud Tasks client's RPC metrics, registered as {@code rpcMethod.CreateTask.leaf} on a
 * component's metric group through an {@link ClientMetrics}.
 *
 * <p>The Cloud Tasks client has no metrics of its own. gax records them for it once the client's
 * settings carry its public {@code OpenTelemetryMetricsFactory} (gax 2.87.0): one double histogram,
 * {@code gcp.client.request.duration} in seconds, recorded once per call as it succeeds, fails or
 * is cancelled, with the full gRPC method name as {@code rpc.method}. A call is gax's whole
 * operation, retries included, but the client retries {@code CreateTask} on nothing, so each one is
 * a single attempt. The histogram maps to its count and its millisecond sum.
 *
 * <p>The connector's components call one RPC through the client, so only {@code CreateTask} is
 * registered.
 *
 * <p>This file is a {@code [[subgroups]]} source of {@code scripts/config/metric-docs.toml}: the
 * group and every leaf below are read from it, so each leaf owes a row on the connector's page.
 */
@Internal
public final class CloudTasksClientMetrics {

    /** Group name carrying the RPC method, so the method is a metric variable, not a name. */
    static final String RPC_METHOD_GROUP = "rpcMethod";

    static final String OPERATIONS = "operations";
    static final String OPERATION_TIME_MILLIS = "operationTimeMillis";

    /** The one RPC the connector's clients call. */
    static final String CREATE_TASK = "CreateTask";

    /** The attribute gax labels the histogram's method with. */
    private static final String METHOD_ATTRIBUTE = "rpc.method";

    /** The service prefix of the method attribute's values, the gRPC full method name's. */
    private static final String METHOD_PREFIX = "google.cloud.tasks.v2.CloudTasks/";

    private static final String REQUEST_DURATION = "gcp.client.request.duration";

    private CloudTasksClientMetrics() {}

    /**
     * Creates the client metrics and registers the {@code CreateTask} series on the owner's group.
     * Call it when the component opens, before any reporter reads the group.
     *
     * @param owner the component's metric group
     * @return the client metrics, which go to {@code TaskCreatorFactory.create} and which the task
     *     creator closes after its client
     */
    public static ClientMetrics register(MetricGroup owner) {
        Preconditions.checkNotNull(owner, "owner");
        ClientMetrics metrics = ClientMetrics.create(METHOD_ATTRIBUTE);
        String rpc = METHOD_PREFIX + CREATE_TASK;
        MetricGroup group = owner.addGroup(RPC_METHOD_GROUP, CREATE_TASK);
        group.counter(OPERATIONS, metrics.histogramCount(REQUEST_DURATION, rpc));
        group.counter(OPERATION_TIME_MILLIS, metrics.histogramSumMillis(REQUEST_DURATION, rpc));
        return metrics;
    }
}
