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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSink;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalFilter;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.RpcMethodSeriesReporter;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteAsync;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteResult;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRule;
import io.github.flink.gcp.connector.bigtable.source.BigtableSource;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The method names the components other than the at-least-once writer register, held against what
 * the real client records into them against the emulator.
 *
 * <p>A component that registered a name the client does not use would report zeros forever, and the
 * client's recordings under the name it does use would be dropped with a {@code WARN}: so each case
 * asserts both a moving {@code operations} series and a silent {@link ClientMetrics} log. The
 * series are read after the job, from the counters themselves; those components do not close their
 * client metrics, so the values are what the client recorded. The Change Streams source has no
 * case: the emulator implements neither of its RPCs.
 */
class BigtableClientMetricsComponentsITCase extends AbstractBigtableEmulatorITCase {

    @BeforeEach
    void resetReporter() {
        RpcMethodSeriesReporter.reset();
    }

    @AfterEach
    void clearReporter() {
        RpcMethodSeriesReporter.reset();
    }

    @Test
    void theScanSourceRecordsReadRowsOnItsReadersAndSampleRowKeysOnItsEnumerator()
            throws Exception {
        TableDestination table = createTable("client-metrics-scan");
        seedRows(table, "a", "b", "c");

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            List<String> keys = new ArrayList<>();
            try (CloseableIterator<String> rows =
                    environment()
                            .fromSource(
                                    BigtableSource.<String>builder()
                                            .table(table)
                                            .deserializer(new TestSources.RowKeyDeserializer())
                                            .emulatorEndpoint(emulatorEndpoint())
                                            .exportClientMetricsToFlink(true)
                                            .build(),
                                    WatermarkStrategy.noWatermarks(),
                                    "scan",
                                    Types.STRING)
                            .executeAndCollect()) {
                rows.forEachRemaining(keys::add);
            }

            assertThat(keys).containsExactlyInAnyOrder("a", "b", "c");
            Map<String, Long> series = RpcMethodSeriesReporter.current();
            assertThat(series.get("ReadRows.operations")).isPositive();
            assertThat(series.get("SampleRowKeys.operations")).isPositive();
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theConditionalSinkRecordsCheckAndMutateRow() throws Exception {
        TableDestination table = createTable("client-metrics-conditional");

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            StreamExecutionEnvironment env = environment();
            env.fromData("x", "y")
                    .sinkTo(
                            BigtableConditionalSink.<String>builder()
                                    .table(table)
                                    .emulatorEndpoint(endpoint())
                                    .serializer(
                                            (key, context) ->
                                                    ConditionalRequest.of(
                                                            ByteString.copyFromUtf8(key),
                                                            ConditionalFilter.rowExists(),
                                                            List.of(),
                                                            List.of(
                                                                    ConditionalMutation.setCell(
                                                                            FAMILY,
                                                                            ByteString.copyFromUtf8(
                                                                                    "q"),
                                                                            1000,
                                                                            ByteString.copyFromUtf8(
                                                                                    key)))))
                                    .exportClientMetricsToFlink(true)
                                    .build());
            env.execute("bigtable-client-metrics-conditional-it");

            assertThat(readRows(table)).hasSize(2);
            assertThat(RpcMethodSeriesReporter.current().get("CheckAndMutateRow.operations"))
                    .isEqualTo(2);
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void theReadModifyWriteHelperRecordsReadModifyWriteRow() throws Exception {
        TableDestination table = createTable("client-metrics-read-modify-write");

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            BigtableReadModifyWriteAsync<String> async =
                    BigtableReadModifyWriteAsync.<String>builder()
                            .table(table)
                            .emulatorEndpoint(endpoint())
                            .serializer(
                                    (key, context) ->
                                            ReadModifyWriteRequest.of(
                                                    ByteString.copyFromUtf8(key),
                                                    List.of(
                                                            ReadModifyWriteRule.increment(
                                                                    FAMILY,
                                                                    ByteString.copyFromUtf8("n"),
                                                                    1))))
                            .exportClientMetricsToFlink(true)
                            .build();
            List<Tuple2<String, ReadModifyWriteResult>> results = new ArrayList<>();
            try (CloseableIterator<Tuple2<String, ReadModifyWriteResult>> iterator =
                    async.orderedWait(environment().fromData("x", "x", "y"), Duration.ofSeconds(30))
                            .executeAndCollect()) {
                iterator.forEachRemaining(results::add);
            }

            assertThat(results).hasSize(3);
            assertThat(RpcMethodSeriesReporter.current().get("ReadModifyWriteRow.operations"))
                    .isEqualTo(3);
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    private static EmulatorEndpoint endpoint() {
        return EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint");
    }

    private static StreamExecutionEnvironment environment() {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        configuration.setString(
                "metrics.reporter.sdk.factory.class", RpcMethodSeriesReporter.class.getName());
        configuration.setString("metrics.reporter.sdk.interval", "100 ms");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        return env;
    }
}
