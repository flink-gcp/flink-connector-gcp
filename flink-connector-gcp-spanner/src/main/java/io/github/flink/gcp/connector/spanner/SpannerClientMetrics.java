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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.source.EnumeratorMetrics;

import javax.annotation.Nullable;

/**
 * The Spanner client's own metrics, registered as {@code rpcMethod.METHOD.leaf} on a component's
 * metric group through a {@link ClientMetrics}.
 *
 * <p>The instruments are what {@code google-cloud-spanner} records into the {@code OpenTelemetry}
 * of a {@code CustomOpenTelemetryMetricsProvider} (6.124.0, {@code BuiltInMetricsRecorder} and its
 * gax parent {@code OpenTelemetryMetricsRecorder}), all named under {@code spanner/client/}: the
 * operation and attempt counts and latencies gax records for every data-client RPC, and the Google
 * front end (GFE) and Spanner API front end (AFE) latencies and missing-header counts Spanner adds
 * per attempt. The client's gRPC-layer metrics need an OpenTelemetry SDK instance and are not
 * recorded here. The operation and attempt counts come from the client's counters; the latency
 * histograms, which the client records at the same points, keep only their sums.
 *
 * <p>The client records nothing into a provider when it runs in its emulator mode, which is how the
 * connector reaches an emulator ({@link SpannerClients}).
 *
 * <p>This file is a {@code [[subgroups]]} source of {@code scripts/config/metric-docs.toml}: the
 * group and every leaf below are read from it, so each leaf owes a row on the connector's page.
 */
@Internal
public final class SpannerClientMetrics {

    /** Group name carrying the RPC method, so the method is a metric variable, not a name. */
    static final String RPC_METHOD_GROUP = "rpcMethod";

    /** The key of the enumerators' shared client metrics; see {@link EnumeratorMetrics}. */
    private static final String ENUMERATOR_KEY = SpannerClientMetrics.class.getName();

    static final String OPERATIONS = "operations";
    static final String OPERATION_TIME_MILLIS = "operationTimeMillis";
    static final String ATTEMPTS = "attempts";
    static final String ATTEMPT_TIME_MILLIS = "attemptTimeMillis";
    static final String ATTEMPTS_WITH_GFE_TIMING = "attemptsWithGfeTiming";
    static final String GFE_TIME_MILLIS = "gfeTimeMillis";
    static final String GFE_CONNECTIVITY_ERRORS = "gfeConnectivityErrors";
    static final String ATTEMPTS_WITH_AFE_TIMING = "attemptsWithAfeTiming";
    static final String AFE_TIME_MILLIS = "afeTimeMillis";
    static final String AFE_CONNECTIVITY_ERRORS = "afeConnectivityErrors";

    /** The RPC every client's database handle sends once, for its multiplexed session. */
    public static final String CREATE_SESSION = "CreateSession";

    /** The RPC of every query: the dialect, schema reads, Change Streams queries, partitions. */
    public static final String EXECUTE_STREAMING_SQL = "ExecuteStreamingSql";

    /** The RPC of a point or table read, including a partition of one. */
    public static final String STREAMING_READ = "StreamingRead";

    /** The RPC the sink writes through. */
    public static final String BATCH_WRITE = "BatchWrite";

    /** The RPC that opens a batch read's snapshot. */
    public static final String BEGIN_TRANSACTION = "BeginTransaction";

    /** The RPC that partitions a query. */
    public static final String PARTITION_QUERY = "PartitionQuery";

    /** The RPC that partitions a table read. */
    public static final String PARTITION_READ = "PartitionRead";

    /** The attribute the client labels every instrument's method with. */
    private static final String METHOD_ATTRIBUTE = "method";

    /** The service prefix of the method attribute's values, such as {@code Spanner.BatchWrite}. */
    private static final String METHOD_PREFIX = "Spanner.";

    private static final String INSTRUMENT_PREFIX = "spanner/client/";
    private static final String OPERATION_COUNT = INSTRUMENT_PREFIX + "operation_count";
    private static final String OPERATION_LATENCY = INSTRUMENT_PREFIX + "operation_latency";
    private static final String ATTEMPT_COUNT = INSTRUMENT_PREFIX + "attempt_count";
    private static final String ATTEMPT_LATENCY = INSTRUMENT_PREFIX + "attempt_latency";
    private static final String GFE_LATENCIES = INSTRUMENT_PREFIX + "gfe_latencies";
    private static final String GFE_CONNECTIVITY_ERROR_COUNT =
            INSTRUMENT_PREFIX + "gfe_connectivity_error_count";
    private static final String AFE_LATENCIES = INSTRUMENT_PREFIX + "afe_latencies";
    private static final String AFE_CONNECTIVITY_ERROR_COUNT =
            INSTRUMENT_PREFIX + "afe_connectivity_error_count";

    private SpannerClientMetrics() {}

    /**
     * Creates the client metrics and registers every series of the given methods on the owner's
     * group. Call it when the component opens, before its client is built and before any reporter
     * reads the group.
     *
     * @param owner the component's metric group
     * @param methods the RPC methods the component's client calls, without the {@code Spanner.}
     *     prefix, such as {@link #BATCH_WRITE}
     * @return the client metrics, whose {@link ClientMetrics#openTelemetry()} goes to the client
     *     and which the client's owner closes after the client
     */
    public static ClientMetrics register(MetricGroup owner, String... methods) {
        Preconditions.checkNotNull(owner, "owner");
        ClientMetrics metrics = create();
        for (String method : methods) {
            registerMethod(metrics, owner, method);
        }
        return metrics;
    }

    /** Creates the client metrics, with no series registered yet. */
    private static ClientMetrics create() {
        ClientMetrics metrics = ClientMetrics.create(METHOD_ATTRIBUTE);
        return metrics;
    }

    /** Registers every series of one method on the owner's group. */
    private static void registerMethod(ClientMetrics metrics, MetricGroup owner, String method) {
        String rpc = METHOD_PREFIX + method;
        MetricGroup group = owner.addGroup(RPC_METHOD_GROUP, method);
        group.counter(OPERATIONS, metrics.counterSum(OPERATION_COUNT, rpc));
        group.counter(OPERATION_TIME_MILLIS, metrics.histogramSumMillis(OPERATION_LATENCY, rpc));
        group.counter(ATTEMPTS, metrics.counterSum(ATTEMPT_COUNT, rpc));
        group.counter(ATTEMPT_TIME_MILLIS, metrics.histogramSumMillis(ATTEMPT_LATENCY, rpc));
        group.counter(ATTEMPTS_WITH_GFE_TIMING, metrics.histogramCount(GFE_LATENCIES, rpc));
        group.counter(GFE_TIME_MILLIS, metrics.histogramSumMillis(GFE_LATENCIES, rpc));
        group.counter(
                GFE_CONNECTIVITY_ERRORS, metrics.counterSum(GFE_CONNECTIVITY_ERROR_COUNT, rpc));
        group.counter(ATTEMPTS_WITH_AFE_TIMING, metrics.histogramCount(AFE_LATENCIES, rpc));
        group.counter(AFE_TIME_MILLIS, metrics.histogramSumMillis(AFE_LATENCIES, rpc));
        group.counter(
                AFE_CONNECTIVITY_ERRORS, metrics.counterSum(AFE_CONNECTIVITY_ERROR_COUNT, rpc));
    }

    /**
     * Returns the client metrics of a source's enumerators: created on the enumerator metric group
     * the first time and handed to every later enumerator built on the same group, with the series
     * of each given method registered once.
     *
     * <p>A coordinator reset builds the next enumerator on the group the first one registered on,
     * where a second registration of a name would be dropped and the series would freeze; see
     * {@link EnumeratorMetrics}. Shared, they keep counting across resets. Each method is
     * registered on its own, so the sub-sources of a {@code HybridSource}, which share one group,
     * each get the series of the methods they call. The client metrics are never closed: a client
     * an enumerator is still closing may record into them, which is no error.
     *
     * @param group the enumerator's metric group, or {@code null} where the context offers none
     * @param methods the RPC methods the enumerators' clients call, without the {@code Spanner.}
     *     prefix
     * @return the shared client metrics, or {@code null} where there is no group
     */
    @Nullable
    public static ClientMetrics registerForEnumerators(
            @Nullable SplitEnumeratorMetricGroup group, String... methods) {
        Preconditions.checkNotNull(methods, "methods");
        ClientMetrics metrics = EnumeratorMetrics.shared(group, ENUMERATOR_KEY, owner -> create());
        if (metrics != null) {
            for (String method : methods) {
                // One key per method: inside a HybridSource, sub-sources calling different
                // methods share the group, and each registers the series it records.
                EnumeratorMetrics.shared(
                        group,
                        ENUMERATOR_KEY + "." + method,
                        owner -> {
                            registerMethod(metrics, owner, method);
                            return method;
                        });
            }
        }
        return metrics;
    }
}
