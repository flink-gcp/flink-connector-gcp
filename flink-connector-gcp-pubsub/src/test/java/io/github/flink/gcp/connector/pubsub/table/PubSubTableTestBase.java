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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.testutils.Drains;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.flink.gcp.connector.testutils.Drains.drainDistinct;

/**
 * Shared harness for the table integration tests: the emulator container and its helpers from the
 * source harness, plus the two things a SQL test needs on top — a {@link TableEnvironment} and a
 * {@code WITH} clause carrying the emulator endpoint.
 *
 * <p>Endpoint injection is plain interpolation into the DDL, so the tests exercise the production
 * factory and its {@code emulator-endpoint} option rather than a test-only factory.
 */
abstract class PubSubTableTestBase extends AbstractPubSubSourceEmulatorITCase {

    static TableEnvironment streamingTableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    /**
     * A {@link TableEnvironment} a Pub/Sub source can actually run in. The source acknowledges on
     * checkpoint completion and its missing-checkpoint detector fails the job when none arrives, so
     * checkpointing is not optional here the way it is for a sink.
     *
     * <p>Restarts are off so a permanent failure fails the test rather than looping inside {@code
     * collect()} until the class timeout.
     */
    static TableEnvironment checkpointingTableEnvironment() {
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.getConfig()
                .set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(500))
                .set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        return tEnv;
    }

    /**
     * Drains rows out of an unbounded query until {@code count} <em>distinct</em> ones have arrived
     * or {@link #COLLECT_TIMEOUT} passes, then closes the iterator, which cancels the job.
     *
     * <p>The mechanics of the bounded drain and the reasons for its shape — distinct rows, a
     * shortfall returned rather than blocked on — live on {@link Drains#drainDistinct}. Callers
     * should assert with {@code containsAll} rather than an exact multiset, because the source is
     * at-least-once.
     *
     * @param result the query to drain
     * @param count how many distinct rows to wait for
     * @param distinguisher what makes a row distinct — normally the field a duplicate would repeat
     */
    static List<Row> collect(TableResult result, int count, Function<Row, Object> distinguisher)
            throws Exception {
        try (CloseableIterator<Row> iterator = result.collect()) {
            return drainDistinct(iterator, count, COLLECT_TIMEOUT, distinguisher);
        }
    }

    /**
     * Renders a {@code WITH} clause for the {@code pubsub} connector: the connector, the emulator
     * endpoint and the project are always present, and the given pairs are added on top.
     *
     * @param keysAndValues alternating option keys and values
     * @return the rendered clause, including the {@code WITH} keyword
     */
    static String withOptions(String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", PubSubDynamicTableFactory.IDENTIFIER);
        options.put("project", PROJECT);
        options.put("emulator-endpoint", emulatorEndpoint());
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return options.entrySet().stream()
                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }
}
