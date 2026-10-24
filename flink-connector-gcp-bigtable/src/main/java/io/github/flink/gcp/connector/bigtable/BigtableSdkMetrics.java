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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.metrics.sdk.SdkMetricsBridge;

/**
 * The Bigtable client's own metrics, registered as {@code sdkMethod.METHOD.leaf} on a component's
 * metric group through an {@link SdkMetricsBridge}.
 *
 * <p>The instruments are the public block {@code google-cloud-bigtable} records into an injected
 * {@code OpenTelemetry} (2.85.0, {@code MetricRegistry.newUserRecorderRegistry}). The client
 * records its other instruments, the per-connection and channel-pool ones, only into its built-in
 * Cloud Monitoring exporter. A histogram maps to its count and its millisecond sum, except where
 * its count repeats another one: the client records application and first-response latency once per
 * operation and throttling latency once per attempt, so only their sums are kept, to be divided by
 * {@code operations} or {@code attempts}.
 *
 * <p>This file is a {@code [[subgroups]]} source of {@code scripts/config/metric-docs.toml}: the
 * group and every leaf below are read from it, so each leaf owes a row on the connector's page.
 */
@Internal
public final class BigtableSdkMetrics {

    /** Group name carrying the RPC method, so the method is a metric variable, not a name. */
    static final String SDK_METHOD_GROUP = "sdkMethod";

    static final String OPERATIONS = "operations";
    static final String OPERATION_TIME_MILLIS = "operationTimeMillis";
    static final String ATTEMPTS = "attempts";
    static final String ATTEMPT_TIME_MILLIS = "attemptTimeMillis";
    static final String RETRIES = "retries";
    static final String CONNECTIVITY_ERRORS = "connectivityErrors";
    static final String SERVER_TIMED_ATTEMPTS = "serverTimedAttempts";
    static final String SERVER_TIME_MILLIS = "serverTimeMillis";
    static final String FIRST_RESPONSE_TIME_MILLIS = "firstResponseTimeMillis";
    static final String APPLICATION_TIME_MILLIS = "applicationTimeMillis";
    static final String THROTTLING_TIME_MILLIS = "throttlingTimeMillis";

    /** The attribute the client labels every instrument's method with. */
    private static final String METHOD_ATTRIBUTE = "method";

    /** The service prefix of the method attribute's values, such as {@code Bigtable.MutateRows}. */
    private static final String METHOD_PREFIX = "Bigtable.";

    private static final String INSTRUMENT_PREFIX = "bigtable.googleapis.com/internal/client/";
    private static final String OPERATION_LATENCIES = INSTRUMENT_PREFIX + "operation_latencies";
    private static final String ATTEMPT_LATENCIES = INSTRUMENT_PREFIX + "attempt_latencies";
    private static final String RETRY_COUNT = INSTRUMENT_PREFIX + "retry_count";
    private static final String CONNECTIVITY_ERROR_COUNT =
            INSTRUMENT_PREFIX + "connectivity_error_count";
    private static final String SERVER_LATENCIES = INSTRUMENT_PREFIX + "server_latencies";
    private static final String FIRST_RESPONSE_LATENCIES =
            INSTRUMENT_PREFIX + "first_response_latencies";
    private static final String APPLICATION_LATENCIES = INSTRUMENT_PREFIX + "application_latencies";
    private static final String THROTTLING_LATENCIES = INSTRUMENT_PREFIX + "throttling_latencies";

    private BigtableSdkMetrics() {}

    /**
     * Creates a bridge and registers every series of the given methods on the owner's group. Call
     * it when the component opens, before any reporter reads the group.
     *
     * @param owner the component's metric group
     * @param methods the RPC methods the component's client calls, without the {@code Bigtable.}
     *     prefix, such as {@code MutateRows}
     * @return the bridge, whose {@link SdkMetricsBridge#openTelemetry()} goes to the client and
     *     which the caller closes after the client
     */
    public static SdkMetricsBridge register(MetricGroup owner, String... methods) {
        Preconditions.checkNotNull(owner, "owner");
        SdkMetricsBridge bridge = SdkMetricsBridge.create(METHOD_ATTRIBUTE);
        for (String method : methods) {
            String rpc = METHOD_PREFIX + method;
            MetricGroup group = owner.addGroup(SDK_METHOD_GROUP, method);
            group.counter(OPERATIONS, bridge.histogramCount(OPERATION_LATENCIES, rpc));
            group.counter(
                    OPERATION_TIME_MILLIS, bridge.histogramSumMillis(OPERATION_LATENCIES, rpc));
            group.counter(ATTEMPTS, bridge.histogramCount(ATTEMPT_LATENCIES, rpc));
            group.counter(ATTEMPT_TIME_MILLIS, bridge.histogramSumMillis(ATTEMPT_LATENCIES, rpc));
            group.counter(RETRIES, bridge.sum(RETRY_COUNT, rpc));
            group.counter(CONNECTIVITY_ERRORS, bridge.sum(CONNECTIVITY_ERROR_COUNT, rpc));
            group.counter(SERVER_TIMED_ATTEMPTS, bridge.histogramCount(SERVER_LATENCIES, rpc));
            group.counter(SERVER_TIME_MILLIS, bridge.histogramSumMillis(SERVER_LATENCIES, rpc));
            group.counter(
                    FIRST_RESPONSE_TIME_MILLIS,
                    bridge.histogramSumMillis(FIRST_RESPONSE_LATENCIES, rpc));
            group.counter(
                    APPLICATION_TIME_MILLIS, bridge.histogramSumMillis(APPLICATION_LATENCIES, rpc));
            group.counter(
                    THROTTLING_TIME_MILLIS, bridge.histogramSumMillis(THROTTLING_LATENCIES, rpc));
        }
        return bridge;
    }
}
