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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Measures writer fanout through the actual application graph without contacting BigQuery. */
@Timeout(60)
class RecoveryRoutingITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static final Map<Integer, Set<Long>> VALUES = new ConcurrentHashMap<>();

    @ParameterizedTest
    @CsvSource({"ALO,10", "ALO,50", "EO,10", "EO,50"})
    void eachWriterReceivesEveryDestination(String mode, int destinations) throws Exception {
        VALUES.clear();
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--mode",
                                mode,
                                "--destinations",
                                Integer.toString(destinations),
                                "--records",
                                Integer.toString(2 * destinations)));
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        BigQueryRecoveryJob.configure(env, options, new CaptureSink());
        var job = env.executeAsync("BigQuery writer fanout");
        try {
            job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
        } finally {
            if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                job.cancel().get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(VALUES.keySet()).containsExactlyInAnyOrder(0, 1);
        for (int writer = 0; writer < 2; writer++) {
            assertThat(VALUES.get(writer))
                    .containsExactlyInAnyOrderElementsOf(
                            LongStream.range(
                                            (long) writer * destinations,
                                            (long) (writer + 1) * destinations)
                                    .boxed()
                                    .toList());
            assertThat(VALUES.get(writer).stream().map(options::destination).distinct().count())
                    .isEqualTo(destinations);
        }
    }

    private static final class CaptureSink implements Sink<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<Long> createWriter(WriterInitContext context) {
            int subtask = context.getTaskInfo().getIndexOfThisSubtask();
            return new SinkWriter<>() {
                @Override
                public void write(Long value, Context ignored) {
                    assertThat(
                                    VALUES.computeIfAbsent(
                                                    subtask, key -> ConcurrentHashMap.newKeySet())
                                            .add(value))
                            .isTrue();
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }
}
