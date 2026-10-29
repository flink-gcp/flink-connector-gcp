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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A scan job under the adaptive scheduler, failing one task, read through a metric reporter: the
 * enumerator metrics must keep moving after the restart (#1908).
 *
 * <p>The adaptive scheduler rebuilds the coordinator at every restart, deserializing a new source
 * object, and builds the next enumerator on the enumerator metric group the first one registered
 * on. Before the fix every name the second enumerator registered collided there, and the reporter
 * kept the first enumerator's series: {@code rowKeySamplesTaken} read 1 after two samplings. The
 * default scheduler reaches the same group only through a global failover, which {@code
 * SpannerSourceGlobalFailoverITCase} drives.
 */
@Timeout(180)
class BigtableEnumeratorMetricsRestartITCase extends AbstractBigtableEmulatorITCase {

    /** Static because the map function is shipped into the job and runs in this same JVM. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    @BeforeEach
    void forgetTheLastRun() {
        FAILED_ONCE.set(false);
        EnumeratorSeriesReporter.REPORTED.clear();
    }

    @Test
    void enumeratorSeriesKeepMovingAcrossARestartThatRebuildsTheEnumerator() throws Exception {
        TableDestination table = createTable("enumerator-metrics-restart");
        seedRows(table, "a", "b", "c");

        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        configuration.setString(
                "metrics.reporter.enumerator.factory.class",
                EnumeratorSeriesReporter.class.getName());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.fromSource(
                        BigtableSource.<String>builder()
                                .table(table)
                                .deserializer(new TestSources.RowKeyDeserializer())
                                .emulatorEndpoint(emulatorEndpoint())
                                .exportClientMetricsToFlink(true)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "bigtable",
                        Types.STRING)
                .map(new FailOnce())
                .sinkTo(new DiscardingSink<>());

        List<String> collisions;
        try (LogCapture metricGroups = LogCapture.of(MetricGroup.class)) {
            env.execute("enumerator metrics across a restart");
            collisions = metricGroups.getMessages();
        }

        assertThat(FAILED_ONCE).as("the job failed once on purpose").isTrue();
        assertThat(collisions)
                .as("no enumerator registration collided with the first enumerator's")
                .noneMatch(message -> message.contains("Name collision"));
        // Both enumerators sampled the table: the enumerator's own counter and the client's series
        // for the call each count both, and neither is a second, unreported registration.
        assertThat(EnumeratorSeriesReporter.count("rowKeySamplesTaken")).isEqualTo(2);
        assertThat(EnumeratorSeriesReporter.count("SampleRowKeys.operations")).isEqualTo(2);
    }

    /** Fails the first record's task once, which the adaptive scheduler answers with a restart. */
    private static final class FailOnce implements MapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String map(String rowKey) {
            if (FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return rowKey;
        }
    }

    /**
     * Keeps the enumerator series a reporter is handed, by name, so the test reads exactly what a
     * reporter reads. Flink offers a reporter only a registration that did not collide.
     */
    public static final class EnumeratorSeriesReporter
            implements MetricReporter, MetricReporterFactory {

        static final Map<String, Metric> REPORTED = new ConcurrentHashMap<>();

        /** Required by the service loader. */
        public EnumeratorSeriesReporter() {}

        static long count(String name) {
            Metric metric = REPORTED.get(name);
            assertThat(metric).as("a reported counter named %s", name).isInstanceOf(Counter.class);
            return ((Counter) metric).getCount();
        }

        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return this;
        }

        @Override
        public void open(MetricConfig config) {}

        @Override
        public void close() {}

        @Override
        public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
            String method = group.getAllVariables().get("<rpcMethod>");
            if ("SampleRowKeys".equals(method)) {
                REPORTED.put(method + "." + name, metric);
            } else if (name.equals("rowKeySamplesTaken") && metric instanceof Counter) {
                REPORTED.put(name, metric);
            }
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {}
    }
}
