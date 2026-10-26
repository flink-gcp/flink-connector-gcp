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

package io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client-library metrics of {@code exportClientMetricsToFlink(true)}, observed the way a user
 * observes them: through a configured Flink metric reporter, while a job writes to the emulator.
 *
 * <p>The real client records them. The emulator builder disables a client's metrics provider, and
 * the connector sets its own after it, so the client records its public instruments into the client
 * metrics against the emulator as it would against the service.
 */
class BigtableClientMetricsITCase extends AbstractBigtableEmulatorITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    @BeforeEach
    void resetReporter() {
        RpcMethodSeriesReporter.reset();
    }

    @AfterEach
    void clearReporter() {
        RpcMethodSeriesReporter.reset();
    }

    @Test
    void aReporterSeesTheClientsMutateRowsSeries() throws Exception {
        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            runJob("client-metrics-on", true);

            Map<String, Long> reported = RpcMethodSeriesReporter.reported();
            // Every leaf, registered for the one method the writer's batchers send.
            assertThat(reported)
                    .containsOnlyKeys(
                            "MutateRows.operations",
                            "MutateRows.operationTimeMillis",
                            "MutateRows.attempts",
                            "MutateRows.attemptTimeMillis",
                            "MutateRows.retries",
                            "MutateRows.connectivityErrors",
                            "MutateRows.attemptsWithServerTiming",
                            "MutateRows.serverTimeMillis",
                            "MutateRows.firstResponseTimeMillis",
                            "MutateRows.applicationTimeMillis");
            // A report read the values while the job ran, which is what a reporter ever sees:
            // the writer's group is unregistered as the task closes.
            assertThat(reported.get("MutateRows.operations")).isPositive();
            assertThat(reported.get("MutateRows.attemptTimeMillis")).isPositive();

            // The relations are read from the values the client metrics kept when the writer
            // closed, after
            // the clients had closed, rather than from the maxima of separate reports, which a
            // report reading one series and then another during recording could set apart.
            Map<String, Long> last = RpcMethodSeriesReporter.current();
            assertThat(last.get("MutateRows.attempts"))
                    .isGreaterThanOrEqualTo(last.get("MutateRows.operations"));
            assertThat(last.get("MutateRows.operationTimeMillis")).isPositive();
            // The emulator sends none of the Google front end's headers, so the client counts
            // every attempt as one that never reached a Google data center and records no server
            // time. The documentation tells an emulator user to expect exactly this.
            assertThat(last.get("MutateRows.connectivityErrors"))
                    .isEqualTo(last.get("MutateRows.attempts"));
            assertThat(last.get("MutateRows.attemptsWithServerTiming")).isZero();
            // Every RPC the client reported was one the writer registered a series for.
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void nothingIsRegisteredWhenTheExportIsOff() throws Exception {
        runJob("client-metrics-off", false);

        assertThat(RpcMethodSeriesReporter.reported()).isEmpty();
    }

    private static void runJob(String tableId, boolean exportToFlink) throws Exception {
        TableDestination table = createTable(tableId);
        String endpoint = emulatorEndpoint();

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        configuration.setString(
                "metrics.reporter.sdk.factory.class", RpcMethodSeriesReporter.class.getName());
        configuration.setString("metrics.reporter.sdk.interval", "100 ms");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(1_000);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) index -> "record-" + index,
                        RECORD_COUNT,
                        RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                        Types.STRING);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(
                        BigtableSink.<String>builder()
                                .table(table)
                                .serializer(
                                        (element, context) ->
                                                RowMutationEntry.create(element)
                                                        .setCell(
                                                                FAMILY, "payload", 1_000L, element))
                                .emulatorEndpoint(endpoint)
                                .exportClientMetricsToFlink(exportToFlink)
                                .build());

        env.execute("bigtable-client-metrics-it");
        assertThat(readRows(table)).hasSize((int) RECORD_COUNT);
    }
}
