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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.SpannerMutationsSink;
import io.github.flink.gcp.connector.spanner.table.sink.SpannerDynamicSink;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.BIGINT().notNull()),
                    Column.physical("name", DataTypes.STRING()));

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", SpannerDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("instance", "my-instance");
        options.put("database", "my-database");
        options.put("table", "people");
        return options;
    }

    private static ResolvedSchema withPrimaryKey() {
        return new ResolvedSchema(
                SCHEMA.getColumns(),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Arrays.asList("id")));
    }

    private static DynamicTableSink sink(ResolvedSchema schema, Map<String, String> options) {
        return FactoryMocks.createTableSink(schema, options);
    }

    private static SpannerMutationsSink<?> built(
            ResolvedSchema schema, Map<String, String> options) {
        Sink<?> sink =
                ((SinkV2Provider)
                                sink(schema, options)
                                        .getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                        .createSink();
        return (SpannerMutationsSink<?>) sink;
    }

    @Test
    void buildsTheConnectorsOwnSinkFromMinimalOptions() {
        DynamicTableSink dynamic = sink(SCHEMA, options());
        SpannerMutationsSink<?> sink = built(SCHEMA, options());

        assertThat(dynamic).isInstanceOf(SpannerDynamicSink.class);
        assertThat(sink.getConfig().getDatabase())
                .isEqualTo(SpannerDatabase.of("my-project", "my-instance", "my-database"));
        assertThat(sink.getConfig().getWriterOptions().getMaxBatchCells()).isEqualTo(5_000);
    }

    @Test
    void mapsEveryWriterOptionAndTheEndpoint() {
        Map<String, String> options = options();
        options.put("sink.buffer-flush.max-cells", "100");
        options.put("sink.buffer-flush.max-mutations", "90");
        options.put("sink.buffer-flush.max-size", "2 mb");
        options.put("sink.buffer-flush.max-commit-delay", "25 ms");
        options.put("sink.rpc-priority", "low");
        options.put("sink.retry.initial-backoff", "2 s");
        options.put("sink.retry.max-backoff", "8 s");
        options.put("sink.retry.max-attempts", "4");
        options.put("emulator-endpoint", "localhost:9010");

        SpannerMutationsSink<?> sink = built(withPrimaryKey(), options);

        assertThat(sink.getConfig().getWriterOptions().getMaxBatchCells()).isEqualTo(100);
        assertThat(sink.getConfig().getWriterOptions().getMaxBatchMutations()).isEqualTo(90);
        assertThat(sink.getConfig().getWriterOptions().getMaxBatchBytes())
                .isEqualTo(2L * 1024 * 1024);
        assertThat(sink.getConfig().getWriterOptions().getMaxCommitDelay())
                .isEqualTo(Duration.ofMillis(25));
        assertThat(sink.getConfig().getWriterOptions().getRpcPriority())
                .isEqualTo(SpannerRpcPriority.LOW);
        assertThat(sink.getConfig().getWriterOptions().getRetryInitialBackoff())
                .isEqualTo(Duration.ofSeconds(2));
        assertThat(sink.getConfig().getWriterOptions().getRetryMaxBackoff())
                .isEqualTo(Duration.ofSeconds(8));
        assertThat(sink.getConfig().getWriterOptions().getRetryMaxAttempts()).isEqualTo(4);
        assertThat(sink.getConfig().getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:9010"));
    }

    @Test
    void carriesSinkParallelismAndCopyState() {
        Map<String, String> options = options();
        options.put("sink.parallelism", "3");
        DynamicTableSink original = sink(withPrimaryKey(), options);
        SinkV2Provider provider =
                (SinkV2Provider)
                        original.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider.getParallelism()).hasValue(3);
        assertThat(original.copy()).isEqualTo(original).hasSameHashCodeAs(original);
    }

    @Test
    void factoryWrappingKeepsTheActionableMarkerErrorInTheCause() {
        Map<String, String> options = options();
        options.put("schema.json-field-paths", "missing");

        assertThatThrownBy(() -> sink(SCHEMA, options))
                .hasStackTraceContaining("unknown field paths")
                .hasStackTraceContaining("missing");
    }
}
