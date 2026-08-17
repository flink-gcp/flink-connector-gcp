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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.SpannerMutationsSink;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceConfig;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceConfig;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.table.sink.RowDataSerializationSchema;
import io.github.flink.gcp.connector.spanner.table.sink.SpannerDynamicSink;
import io.github.flink.gcp.connector.spanner.table.source.SpannerChangeStreamDynamicSource;
import io.github.flink.gcp.connector.spanner.table.source.SpannerDynamicSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

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
    void qualifiesNamedSchemaSinksAndScans() throws Exception {
        Map<String, String> options = options();
        options.put("schema", "analytics");

        SpannerMutationsSink<?> sink = built(SCHEMA, options);
        Mutation mutation =
                ((RowDataSerializationSchema) sink.getConfig().getSerializer())
                        .serialize(GenericRowData.of(1L, null), null);
        SpannerSourceConfig<?> source = builtSource(SCHEMA, options);

        assertThat(mutation.getTable()).isEqualTo("analytics.people");
        assertThat(source.getReadOperation().getTable()).isEqualTo("analytics.people");
    }

    @Test
    void appliesPostgresqlIdentifierFoldingToNamedSchemaPaths() {
        Map<String, String> options = options();
        options.put("dialect", Dialect.POSTGRESQL.name());
        options.put("schema", "Analytics");
        options.put("table", "People");
        options.put("scan.index", "ByName");

        SpannerSourceConfig<?> source = builtSource(SCHEMA, options);

        assertThat(source.getReadOperation().toString())
                .contains("analytics.people")
                .contains("analytics.byname");
    }

    @Test
    void rejectsMalformedNamedSchemaComponentsDuringFactoryValidation() {
        Map<String, String> multipartTable = options();
        multipartTable.put("schema", "analytics");
        multipartTable.put("table", "analytics.people");
        assertThatThrownBy(() -> source(SCHEMA, multipartTable))
                .hasStackTraceContaining("table must be one non-blank GoogleSQL identifier");

        Map<String, String> multipartIndex = options();
        multipartIndex.put("schema", "analytics");
        multipartIndex.put("scan.index", "analytics.by_name");
        assertThatThrownBy(() -> source(SCHEMA, multipartIndex))
                .hasStackTraceContaining("scan.index must be one non-blank GoogleSQL identifier");
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
                .isEqualTo(EmulatorEndpoint.parse("localhost:9010", "emulatorEndpoint"));
    }

    @Test
    void mapsTheCredentialPathToSinkAndScan() {
        Map<String, String> options = options();
        options.put("service-account-key-file", "/var/run/secrets/spanner.json");

        assertThat(built(SCHEMA, options).getConfig().getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/spanner.json");
        assertThat(builtSource(SCHEMA, options).getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/spanner.json");
    }

    @Test
    void rejectsInvalidCredentialOptionsForSinkAndSource() {
        Map<String, String> blank = options();
        blank.put("service-account-key-file", "  ");
        assertThatThrownBy(() -> sink(SCHEMA, blank))
                .hasStackTraceContaining("service-account-key-file must not be blank");
        assertThatThrownBy(() -> source(SCHEMA, blank))
                .hasStackTraceContaining("service-account-key-file must not be blank");

        Map<String, String> conflict = options();
        conflict.put("service-account-key-file", "key.json");
        conflict.put("emulator-endpoint", "localhost:9010");
        assertThatThrownBy(() -> sink(SCHEMA, conflict))
                .hasStackTraceContaining(
                        "service-account-key-file cannot be combined with emulator-endpoint");
        assertThatThrownBy(() -> source(SCHEMA, conflict))
                .hasStackTraceContaining(
                        "service-account-key-file cannot be combined with emulator-endpoint");
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
    void buildsAnUnboundedChangeStreamSourceAndKeepsItsCopyState() {
        Map<String, String> options = options();
        options.put("scan.mode", "change-stream");
        options.put("scan.change-stream.name", "people_changes");
        options.put("scan.change-stream.changelog-mode", "full");
        options.put("scan.startup.mode", "earliest");
        options.put("scan.resume-fallback.mode", "timestamp");
        options.put("scan.resume-fallback.timestamp-millis", "1000");
        options.put("scan.change-stream.absent-retention-fallback", "3 d");
        options.put("scan.change-stream.heartbeat-interval", "1500 ms");
        options.put("scan.max-concurrent-queries-per-subtask", "4");
        options.put("scan.rpc-priority", "low");
        options.put("scan.parallelism", "3");
        ScanTableSource dynamic = (ScanTableSource) source(SCHEMA, options);
        SourceProvider provider =
                (SourceProvider)
                        dynamic.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        Source<?, ?, ?> runtime = provider.createSource();
        SpannerChangeStreamSourceConfig<?> config = TestSources.changeStreamConfig(runtime);

        assertThat(dynamic).isInstanceOf(SpannerChangeStreamDynamicSource.class);
        assertThat(runtime).isInstanceOf(SpannerChangeStreamSource.class);
        assertThat(runtime.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(provider.getParallelism()).contains(3);
        assertThat(config.getStartPosition()).isEqualTo(StartPosition.earliest());
        assertThat(config.getResumeFallback())
                .isEqualTo(StartPosition.at(Instant.ofEpochMilli(1000)));
        assertThat(config.getAbsentRetentionFallback()).isEqualTo(Duration.ofDays(3));
        assertThat(config.getHeartbeatMillis()).isEqualTo(1500L);
        assertThat(config.getRpcPriority()).isEqualTo(SpannerRpcPriority.LOW);
        assertThat(config.getMaxConcurrentQueriesPerSubtask()).isEqualTo(4);
        assertThat(dynamic.copy()).isEqualTo(dynamic).hasSameHashCodeAs(dynamic);
        assertThat(dynamic.getChangelogMode().getContainedKinds())
                .containsExactlyInAnyOrder(
                        org.apache.flink.types.RowKind.INSERT,
                        org.apache.flink.types.RowKind.UPDATE_BEFORE,
                        org.apache.flink.types.RowKind.UPDATE_AFTER,
                        org.apache.flink.types.RowKind.DELETE);
    }

    @Test
    void exposesChangeStreamMetadataAndKeepsAppliedAbilityState() {
        SpannerChangeStreamDynamicSource dynamic =
                (SpannerChangeStreamDynamicSource) source(SCHEMA, changeStreamOptions("full"));
        SupportsReadingMetadata metadata = dynamic;

        assertThat(metadata.listReadableMetadata())
                .containsExactly(
                        entry("commit-timestamp", DataTypes.TIMESTAMP_LTZ(9).notNull()),
                        entry("sequence", DataTypes.STRING().notNull()),
                        entry("server-transaction-id", DataTypes.STRING().notNull()),
                        entry(
                                "is-last-record-in-transaction-in-partition",
                                DataTypes.BOOLEAN().notNull()),
                        entry("table", DataTypes.STRING().notNull()),
                        entry("mod-type", DataTypes.STRING().notNull()),
                        entry("value-capture-type", DataTypes.STRING().notNull()),
                        entry("number-of-records-in-transaction", DataTypes.BIGINT().notNull()),
                        entry("number-of-partitions-in-transaction", DataTypes.BIGINT().notNull()),
                        entry("transaction-tag", DataTypes.STRING().notNull()),
                        entry("system-transaction", DataTypes.BOOLEAN().notNull()),
                        entry("mod-number", DataTypes.INT().notNull()));

        DynamicTableSource beforeAbilities = dynamic.copy();
        metadata.applyReadableMetadata(
                Arrays.asList("mod-number", "commit-timestamp"),
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("mod_number", DataTypes.INT().notNull()),
                        DataTypes.FIELD("commit_timestamp", DataTypes.TIMESTAMP_LTZ(9).notNull())));
        ((SupportsSourceWatermark) dynamic).applySourceWatermark();

        assertThat(dynamic).isNotEqualTo(beforeAbilities);
        assertThat(dynamic.copy()).isEqualTo(dynamic).hasSameHashCodeAs(dynamic);
        assertThat(
                        ((SourceProvider)
                                        dynamic.getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                                .createSource())
                .isInstanceOf(SpannerChangeStreamSource.class);
    }

    @Test
    void validatesChangeStreamModeOptionsAndUpsertKeys() throws Exception {
        Map<String, String> missingName = options();
        missingName.put("scan.mode", "change-stream");
        missingName.put("scan.change-stream.changelog-mode", "full");
        assertThatThrownBy(() -> source(SCHEMA, missingName))
                .hasStackTraceContaining("scan.change-stream.name is required");

        Map<String, String> missingMode = options();
        missingMode.put("scan.mode", "change-stream");
        missingMode.put("scan.change-stream.name", "people_changes");
        assertThatThrownBy(() -> source(SCHEMA, missingMode))
                .hasStackTraceContaining("scan.change-stream.changelog-mode is required");

        Map<String, String> upsert = changeStreamOptions("upsert");
        assertThatThrownBy(() -> source(SCHEMA, upsert))
                .hasStackTraceContaining("upsert requires a PRIMARY KEY");
        org.apache.flink.table.connector.ChangelogMode upsertMode =
                ((ScanTableSource) source(withPrimaryKey(), upsert)).getChangelogMode();
        assertThat(upsertMode.getContainedKinds())
                .contains(
                        org.apache.flink.types.RowKind.INSERT,
                        org.apache.flink.types.RowKind.UPDATE_AFTER,
                        org.apache.flink.types.RowKind.DELETE)
                .doesNotContain(org.apache.flink.types.RowKind.UPDATE_BEFORE);
        try {
            assertThat(
                            org.apache.flink.table.connector.ChangelogMode.class
                                    .getMethod("keyOnlyDeletes")
                                    .invoke(upsertMode))
                    .isEqualTo(true);
        } catch (NoSuchMethodException ignored) {
            // Flink 1.20 does not expose the Flink 2 key-only-delete declaration bit.
        }

        Map<String, String> bounded = options();
        bounded.put("scan.change-stream.name", "people_changes");
        assertThatThrownBy(() -> source(SCHEMA, bounded))
                .hasStackTraceContaining("incompatible with scan.mode=bounded");

        Map<String, String> incompatible = changeStreamOptions("full");
        incompatible.put("scan.index", "by_name");
        assertThatThrownBy(() -> source(SCHEMA, incompatible))
                .hasStackTraceContaining("scan.index is incompatible with scan.mode=change-stream");
    }

    @Test
    void rejectsEveryOptionOwnedByTheOtherScanMode() {
        Map<String, String> changeStreamOnly =
                Map.ofEntries(
                        Map.entry("scan.change-stream.name", "people_changes"),
                        Map.entry("scan.change-stream.changelog-mode", "full"),
                        Map.entry("scan.startup.mode", "latest"),
                        Map.entry("scan.startup.timestamp-millis", "1000"),
                        Map.entry("scan.resume-fallback.mode", "earliest"),
                        Map.entry("scan.resume-fallback.timestamp-millis", "1000"),
                        Map.entry("scan.change-stream.absent-retention-fallback", "3 d"),
                        Map.entry("scan.change-stream.heartbeat-interval", "2 s"),
                        Map.entry("scan.max-concurrent-queries-per-subtask", "4"));
        for (Map.Entry<String, String> entry : changeStreamOnly.entrySet()) {
            Map<String, String> bounded = options();
            bounded.put(entry.getKey(), entry.getValue());
            assertThatThrownBy(() -> source(SCHEMA, bounded))
                    .as(entry.getKey())
                    .hasStackTraceContaining(
                            entry.getKey() + " is incompatible with scan.mode=bounded");
        }

        Map<String, String> boundedOnly =
                Map.ofEntries(
                        Map.entry("scan.index", "by_name"),
                        Map.entry("scan.partition.max-partitions", "12"),
                        Map.entry("scan.partition.size", "2 mb"),
                        Map.entry("scan.data-boost-enabled", "true"),
                        Map.entry("scan.timestamp-bound.read-timestamp", "2026-08-13T00:00:00Z"),
                        Map.entry("scan.timestamp-bound.exact-staleness", "15 s"),
                        Map.entry("lookup.async", "false"),
                        Map.entry("lookup.cache", "NONE"),
                        Map.entry("lookup.max-retries", "3"),
                        Map.entry("lookup.partial-cache.expire-after-access", "1 min"),
                        Map.entry("lookup.partial-cache.expire-after-write", "1 min"),
                        Map.entry("lookup.partial-cache.cache-missing-key", "true"),
                        Map.entry("lookup.partial-cache.max-rows", "100"));
        for (Map.Entry<String, String> entry : boundedOnly.entrySet()) {
            Map<String, String> changeStream = changeStreamOptions("full");
            changeStream.put(entry.getKey(), entry.getValue());
            assertThatThrownBy(() -> source(SCHEMA, changeStream))
                    .as(entry.getKey())
                    .hasStackTraceContaining(
                            entry.getKey() + " is incompatible with scan.mode=change-stream");
        }
    }

    @Test
    void validatesChangeStreamTimestampOptionPairs() {
        Map<String, String> missing = changeStreamOptions("full");
        missing.put("scan.startup.mode", "timestamp");
        assertThatThrownBy(() -> source(SCHEMA, missing))
                .hasStackTraceContaining("scan.startup.timestamp-millis is required");

        Map<String, String> stray = changeStreamOptions("full");
        stray.put("scan.startup.timestamp-millis", "1000");
        assertThatThrownBy(() -> source(SCHEMA, stray)).hasStackTraceContaining("may be set only");

        Map<String, String> fallback = changeStreamOptions("full");
        fallback.put("scan.resume-fallback.timestamp-millis", "1000");
        assertThatThrownBy(() -> source(SCHEMA, fallback))
                .hasStackTraceContaining("requires scan.resume-fallback.mode=timestamp");
    }

    private static Map<String, String> changeStreamOptions(String changelogMode) {
        Map<String, String> options = options();
        options.put("scan.mode", "change-stream");
        options.put("scan.change-stream.name", "people_changes");
        options.put("scan.change-stream.changelog-mode", changelogMode);
        return options;
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
    void scanIndexDefersThePhysicalReadUntilLiveMetadataIsAvailable() {
        Map<String, String> options = options();
        options.put("scan.index", "records_by_name");

        SpannerSourceConfig<?> config = builtSource(SCHEMA, options);

        assertThat(config.getReadOperation().toString())
                .contains("deferred read")
                .contains("records_by_name");
    }

    @Test
    void rejectsABlankScanIndex() {
        Map<String, String> options = options();
        options.put("scan.index", "  ");

        assertThatThrownBy(() -> source(SCHEMA, options))
                .hasStackTraceContaining("scan.index must not be blank");
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
