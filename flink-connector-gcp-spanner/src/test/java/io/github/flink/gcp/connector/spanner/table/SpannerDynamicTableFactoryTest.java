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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.SpannerMutationsSink;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceConfig;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.table.sink.SpannerDynamicSink;
import io.github.flink.gcp.connector.spanner.table.source.SpannerDynamicSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

    @Test
    void rejectsAUuidMarkerOnANonStringCarrierDuringFactoryValidation() {
        Map<String, String> options = options();
        options.put("schema.uuid-field-paths", "id");

        assertThatThrownBy(() -> sink(SCHEMA, options))
                .hasStackTraceContaining("UUID must be declared as STRING");
    }

    private static DynamicTableSource source(ResolvedSchema schema, Map<String, String> options) {
        return FactoryMocks.createTableSource(schema, options);
    }

    private static SpannerSourceConfig<?> builtSource(
            ResolvedSchema schema, Map<String, String> options) {
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) source(schema, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return ((SpannerBatchReadSource<?>) provider.createSource()).getConfig();
    }

    @Test
    void buildsTheConnectorsOwnBoundedSource() {
        DynamicTableSource dynamic = source(SCHEMA, options());
        SpannerSourceConfig<?> config = builtSource(SCHEMA, options());

        assertThat(dynamic).isInstanceOf(SpannerDynamicSource.class);
        assertThat(config.getDatabase())
                .isEqualTo(SpannerDatabase.of("my-project", "my-instance", "my-database"));
        assertThat(config.getReadOperation().getColumns()).containsExactly("id", "name");
        assertThat(config.getTimestampBound().getMode().name()).isEqualTo("STRONG");
    }

    @Test
    void mapsEveryScanOptionAndSourceParallelism() {
        Map<String, String> options = options();
        options.put("scan.partition.max-partitions", "12");
        options.put("scan.partition.size", "2 mb");
        options.put("scan.data-boost-enabled", "true");
        options.put("scan.rpc-priority", "low");
        options.put("scan.timestamp-bound.exact-staleness", "15 s");
        options.put("scan.parallelism", "3");
        ScanTableSource source = (ScanTableSource) source(SCHEMA, options);
        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        SpannerSourceConfig<?> config =
                ((SpannerBatchReadSource<?>) provider.createSource()).getConfig();

        assertThat(config.getPartitionOptions().getMaxPartitions()).isEqualTo(12);
        assertThat(config.getPartitionOptions().getPartitionSizeBytes())
                .isEqualTo(2L * 1024 * 1024);
        assertThat(config.isDataBoostEnabled()).isTrue();
        assertThat(config.getRpcPriority()).isEqualTo(SpannerRpcPriority.LOW);
        assertThat(config.getTimestampBound().getExactStaleness(TimeUnit.SECONDS)).isEqualTo(15);
        assertThat(provider.getParallelism()).contains(3);
        assertThat(source.copy()).isEqualTo(source).hasSameHashCodeAs(source);
    }

    @Test
    void projectionChangesThePhysicalReadAndZeroProjectionUsesACarrier() {
        SpannerDynamicSource projected = (SpannerDynamicSource) source(SCHEMA, options());
        projected.applyProjection(
                new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())));
        assertThat(built(projected).getReadOperation().getColumns()).containsExactly("name");

        SpannerDynamicSource zero = (SpannerDynamicSource) source(SCHEMA, options());
        zero.applyProjection(new int[0][], DataTypes.ROW());
        assertThat(built(zero).getReadOperation().getColumns()).containsExactly("id");
    }

    private static SpannerSourceConfig<?> built(SpannerDynamicSource source) {
        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return ((SpannerBatchReadSource<?>) provider.createSource()).getConfig();
    }

    @Test
    void rejectsConflictingTimestampBounds() {
        Map<String, String> options = options();
        options.put("scan.timestamp-bound.read-timestamp", "2026-08-11T00:00:00Z");
        options.put("scan.timestamp-bound.exact-staleness", "1 s");

        assertThatThrownBy(() -> source(SCHEMA, options))
                .hasStackTraceContaining("mutually exclusive");
    }

    @Test
    void mapsAReadTimestampAndRejectsInvalidSnapshotBounds() {
        Map<String, String> timestamp = options();
        timestamp.put("scan.timestamp-bound.read-timestamp", "2026-08-11T00:00:00.123456789Z");
        assertThat(builtSource(SCHEMA, timestamp).getTimestampBound().getReadTimestamp().toString())
                .isEqualTo("2026-08-11T00:00:00.123456789Z");

        Map<String, String> invalidTimestamp = options();
        invalidTimestamp.put("scan.timestamp-bound.read-timestamp", "not-a-timestamp");
        assertThatThrownBy(() -> source(SCHEMA, invalidTimestamp))
                .hasStackTraceContaining("Invalid scan timestamp");

        Map<String, String> zeroStaleness = options();
        zeroStaleness.put("scan.timestamp-bound.exact-staleness", "0 s");
        assertThatThrownBy(() -> source(SCHEMA, zeroStaleness))
                .hasStackTraceContaining("exact-staleness must be positive");
    }
}
