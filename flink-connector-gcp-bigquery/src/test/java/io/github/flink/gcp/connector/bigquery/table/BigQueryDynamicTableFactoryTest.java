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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcSequenceNumberProvider;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.BigQueryStorageReadSource;
import io.github.flink.gcp.connector.bigquery.source.query.QuerySpec;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.table.sink.BigQueryDynamicSink;
import io.github.flink.gcp.connector.bigquery.table.source.BigQueryDynamicSource;
import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link BigQueryDynamicTableFactory}. */
class BigQueryDynamicTableFactoryTest {

    @TempDir Path tempDir;

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()));

    private static ResolvedSchema withPrimaryKey(ResolvedSchema schema) {
        return new ResolvedSchema(
                schema.getColumns(),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
    }

    /** {@link #SCHEMA} plus a column {@code sink.table-create.*} can partition on. */
    private static final ResolvedSchema PARTITIONABLE =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()),
                    Column.physical("event_ts", DataTypes.TIMESTAMP_LTZ(6)));

    /** The destination {@link #minimalOptions()} names, for reading creation options back. */
    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static Map<String, String> minimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", BigQueryDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("dataset", "my_dataset");
        options.put("table", "my_table");
        return options;
    }

    private static DynamicTableSink sink(Map<String, String> options) {
        return FactoryMocks.createTableSink(SCHEMA, options);
    }

    private static DynamicTableSource source(Map<String, String> options) {
        return FactoryMocks.createTableSource(SCHEMA, options);
    }

    private static BigQuerySourceConfig<?> sourceConfig(Map<String, String> options) {
        return sourceConfig(source(options));
    }

    private static BigQuerySourceConfig<?> sourceConfig(DynamicTableSource source) {
        return io.github.flink.gcp.connector.bigquery.source.TestSources.configOf(
                builtSource(source));
    }

    private static BigQueryStorageReadSource<?> builtSource(DynamicTableSource source) {
        SourceProvider provider =
                (SourceProvider)
                        ((org.apache.flink.table.connector.source.ScanTableSource) source)
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return (BigQueryStorageReadSource<?>) provider.createSource();
    }

    /**
     * The connector's own sink, as the planner would obtain it.
     *
     * <p>Every option assertion reads off this rather than off the {@link DynamicTableSink}: a
     * value dropped on the way to {@code BigQuerySink.builder()} is invisible everywhere else.
     */
    private static Sink<?> built(Map<String, String> options) {
        return built(SCHEMA, options);
    }

    private static Sink<?> built(ResolvedSchema schema, Map<String, String> options) {
        return ((SinkV2Provider)
                        FactoryMocks.createTableSink(schema, options)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                .createSink();
    }

    private static Sink<?> builtWithMetadata(
            ResolvedSchema schema, Map<String, String> options, String... metadataKeys) {
        BigQueryDynamicSink dynamic =
                (BigQueryDynamicSink) FactoryMocks.createTableSink(schema, options);
        dynamic.applyWritableMetadata(Arrays.asList(metadataKeys), schema.toSinkRowDataType());
        return ((SinkV2Provider)
                        dynamic.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                .createSink();
    }

    private static BigQuerySinkConfig<?> configOf(Sink<?> sink) {
        if (sink instanceof BigQueryDefaultStreamSink) {
            return ((BigQueryDefaultStreamSink<?>) sink).getConfig();
        }
        if (sink instanceof BigQueryBufferedStreamSink) {
            return ((BigQueryBufferedStreamSink<?>) sink).getConfig();
        }
        if (sink instanceof BigQueryFileLoadsSink) {
            return ((BigQueryFileLoadsSink<?>) sink).getConfig();
        }
        throw new AssertionError("Unexpected BigQuery sink: " + sink.getClass().getName());
    }

    /** {@link #minimalOptions()} plus the write method and whatever that method requires. */
    private static Map<String, String> optionsFor(WriteMethod writeMethod) {
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", writeMethod.toString());
        if (writeMethod == WriteMethod.FILE_LOADS) {
            options.put("sink.file-loads.staging-path", "gs://bucket/prefix");
        }
        return options;
    }

    @Test
    void buildsASinkFromTheMinimalOptions() {
        assertThat(sink(minimalOptions()))
                .isInstanceOf(BigQueryDynamicSink.class)
                .extracting(DynamicTableSink::asSummaryString)
                .isEqualTo("BigQuery table sink");
    }

    @Test
    void buildsATableSourceFromTheMinimalOptions() {
        DynamicTableSource source = source(minimalOptions());

        assertThat(source)
                .isInstanceOf(BigQueryDynamicSource.class)
                .extracting(DynamicTableSource::asSummaryString)
                .isEqualTo("BigQuery table source");
        BigQuerySourceConfig<?> config = sourceConfig(source);
        assertThat(config.getTable()).isEqualTo(DESTINATION);
        assertThat(config.getQuery()).isNull();
        assertThat(config.getParentProject()).isEqualTo("my-project");
        assertThat(config.getSelectedFields()).containsExactly("id", "amount");
    }

    @Test
    void flinkSqlTimePrecisionMatchesTheSupportedVersion() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE times (clock TIME(3)) WITH ('connector'='bigquery', "
                        + "'project'='my-project', 'dataset'='my_dataset', 'table'='my_table')");

        DataType expected =
                FlinkVersions.retainsSqlTimePrecision() ? DataTypes.TIME(3) : DataTypes.TIME(0);
        assertThat(table.from("times").getResolvedSchema().getColumnDataTypes())
                .containsExactly(expected);
    }

    @Test
    void usesAnIndependentParentProjectForStorageReadBilling() {
        Map<String, String> options = minimalOptions();
        options.put("project", "table-owner");
        options.put("source.parent-project", "billing-project");

        BigQuerySourceConfig<?> config = sourceConfig(options);

        assertThat(config.getTable())
                .isEqualTo(TableDestination.of("table-owner", "my_dataset", "my_table"));
        assertThat(config.getParentProject()).isEqualTo("billing-project");
    }

    @Test
    void aQueryMayNameOnlyItsParentProject() {
        Map<String, String> options = minimalOptions();
        options.remove("project");
        options.remove("dataset");
        options.remove("table");
        options.put("source.query", "SELECT 1");
        options.put("source.parent-project", "billing-project");

        BigQuerySourceConfig<?> config = sourceConfig(options);

        assertThat(config.getTable()).isNull();
        assertThat(config.getParentProject()).isEqualTo("billing-project");
    }

    @Test
    void buildsAQuerySourceWithoutDatasetOrTable() {
        Map<String, String> options = minimalOptions();
        options.remove("dataset");
        options.remove("table");
        options.put("source.query", "SELECT id, amount FROM `p.d.t`");
        options.put("source.query-location", "US");
        options.put("source.query-result-dataset", "scratch");
        options.put("source.reuse-query-result-within", "10 min");
        options.put("source.row-restriction", "amount > 0");
        options.put("source.max-stream-count", "7");
        options.put("source.preferred-min-stream-count", "3");
        options.put("source.max-records-per-fetch", "200");
        options.put("source.retry-max-attempts", "9");

        BigQuerySourceConfig<?> config = sourceConfig(options);

        assertThat(config.getTable()).isNull();
        assertThat(config.getQuery()).isEqualTo("SELECT id, amount FROM `p.d.t`");
        assertThat(config.getParentProject()).isEqualTo("my-project");
        assertThat(config.getQueryLocation()).isEqualTo("US");
        assertThat(config.getQueryResultDataset()).isEqualTo("scratch");
        assertThat(config.getReuseQueryResultWithin()).hasToString("PT10M");
        assertThat(config.getRowRestriction()).isEqualTo("amount > 0");
        assertThat(config.getMaxStreamCount()).isEqualTo(7);
        assertThat(config.getPreferredMinStreamCount()).isEqualTo(3);
        assertThat(config.getMaxRecordsPerFetch()).isEqualTo(200);
        assertThat(((ReadClientRowStreamOpener) config.getRowStreamOpener()).retryMaxAttempts())
                .isEqualTo(9);
    }

    @Test
    void mapsViewAndSnapshotSourceOptions() {
        Map<String, String> options = minimalOptions();
        options.put("source.materialize-views", "true");
        options.put("source.query-location", "asia-northeast1");
        options.put("source.snapshot-time", "2026-08-01T00:00:00Z");

        // snapshotTime and materializeViews are deliberately incompatible in the DataStream
        // builder, so pin each mapping independently.
        Map<String, String> view = new HashMap<>(options);
        view.remove("source.snapshot-time");
        BigQuerySourceConfig<?> viewConfig = sourceConfig(view);
        assertThat(viewConfig.isMaterializeViewsEnabled()).isTrue();
        assertThat(viewConfig.getQueryLocation()).isEqualTo("asia-northeast1");

        options.remove("source.materialize-views");
        options.remove("source.query-location");
        assertThat(sourceConfig(options).getSnapshotTime()).hasToString("2026-08-01T00:00:00Z");
    }

    @Test
    void directTableSourceLeavesTheSinkRestEndpointUnused() {
        Map<String, String> options = minimalOptions();
        options.put("emulator-rest-endpoint", "localhost:9050");

        assertThat(sourceConfig(options).getQueryRunner()).isNull();
    }

    @Test
    void queryAndViewSourcesCarryTheRestEmulatorEndpoint() {
        Map<String, String> query = minimalOptions();
        query.put("source.query", "SELECT id, amount FROM `p.d.t`");
        Map<String, String> view = minimalOptions();
        view.put("source.materialize-views", "true");

        for (Map<String, String> options : Arrays.asList(query, view)) {
            options.put("emulator-endpoint", "localhost:9060");
            options.put("emulator-rest-endpoint", "localhost:9050");
            assertThat(
                            io.github.flink.gcp.connector.bigquery.source.query.TestQueryRunners
                                    .emulatorEndpoint(sourceConfig(options).getQueryRunner()))
                    .isEqualTo("localhost:9050");
        }
    }

    @Test
    void projectionBecomesStorageReadSelectedFields() {
        DynamicTableSource source = source(minimalOptions());
        ((SupportsProjectionPushDown) source)
                .applyProjection(
                        new int[][] {{1}},
                        DataTypes.ROW(DataTypes.FIELD("amount", DataTypes.INT())));

        assertThat(sourceConfig(source).getSelectedFields()).containsExactly("amount");
    }

    @Test
    void emptyProjectionReadsOneCarrierColumn() {
        DynamicTableSource source = source(minimalOptions());
        ((SupportsProjectionPushDown) source).applyProjection(new int[0][], DataTypes.ROW());

        assertThat(sourceConfig(source).getSelectedFields()).containsExactly("id");
    }

    @Test
    void sourceCredentialPathSurvivesCopyAndReachesEveryClientForEveryMode() throws Exception {
        String missing = tempDir.resolve("missing-table-source-key.json").toString();
        Map<String, String> table = minimalOptions();
        Map<String, String> query = minimalOptions();
        query.put("source.query", "SELECT id, amount FROM `p.d.t`");
        Map<String, String> view = minimalOptions();
        view.put("source.materialize-views", "true");

        for (Map<String, String> options : Arrays.asList(table, query, view)) {
            options.put("service-account-key-file", missing);
            BigQueryDynamicSource dynamicSource = (BigQueryDynamicSource) source(options);
            assertThat(dynamicSource.copy()).isEqualTo(dynamicSource);
            BigQuerySourceConfig<?> config =
                    io.github.flink.gcp.connector.bigquery.source.TestSources.configOf(
                            InstantiationUtil.clone(builtSource(dynamicSource)));

            assertThatThrownBy(
                            () ->
                                    config.getSessionCreatorFactory()
                                            .create()
                                            .create(
                                                    com.google.cloud.bigquery.storage.v1
                                                            .CreateReadSessionRequest
                                                            .getDefaultInstance()))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessage("Failed to load the configured BigQuery service-account key file.")
                    .hasMessageNotContaining(missing);
            assertThatThrownBy(() -> config.getRowStreamOpener().open("stream", 0))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessage("Failed to load the configured BigQuery service-account key file.")
                    .hasMessageNotContaining(missing);
            if (config.getQueryRunner() != null) {
                assertThatThrownBy(
                                () ->
                                        config.getQueryRunner()
                                                .run(
                                                        new QuerySpec(
                                                                "SELECT 1",
                                                                "my-project",
                                                                null,
                                                                null)))
                        .isInstanceOf(java.io.IOException.class)
                        .hasMessage(
                                "Failed to load the configured BigQuery service-account key file.")
                        .hasMessageNotContaining(missing);
            }
        }
    }

    @Test
    void sourceCopyPreservesProjectionAndEveryOptionalFamily() {
        Map<String, String> table = minimalOptions();
        table.put("source.row-restriction", "amount > 0");
        table.put("source.parent-project", "billing-project");
        table.put("source.snapshot-time", "2026-08-01T00:00:00Z");
        table.put("source.max-stream-count", "7");
        table.put("source.preferred-min-stream-count", "3");
        table.put("source.max-records-per-fetch", "200");
        table.put("source.retry-max-attempts", "9");
        table.put("emulator-endpoint", "localhost:9060");
        table.put("scan.parallelism", "4");
        BigQueryDynamicSource tableSource = (BigQueryDynamicSource) source(table);
        tableSource.applyProjection(
                new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("amount", DataTypes.INT())));
        assertThat(tableSource.copy()).isNotSameAs(tableSource).isEqualTo(tableSource);

        Map<String, String> query = minimalOptions();
        query.put("source.query", "SELECT id, amount FROM `p.d.t`");
        query.put("source.query-location", "US");
        query.put("source.query-result-dataset", "scratch");
        query.put("source.reuse-query-result-within", "10 min");
        query.put("emulator-endpoint", "localhost:9060");
        query.put("emulator-rest-endpoint", "localhost:9050");
        BigQueryDynamicSource querySource = (BigQueryDynamicSource) source(query);
        assertThat(querySource.copy()).isNotSameAs(querySource).isEqualTo(querySource);

        Map<String, String> view = minimalOptions();
        view.put("source.materialize-views", "true");
        view.put("emulator-endpoint", "localhost:9060");
        view.put("emulator-rest-endpoint", "localhost:9050");
        BigQueryDynamicSource viewSource = (BigQueryDynamicSource) source(view);
        assertThat(viewSource.copy()).isNotSameAs(viewSource).isEqualTo(viewSource);
    }

    @Test
    void validatesSourceSpecificDestinationAndQueryOptions() {
        ResolvedSchema noPhysicalColumns =
                ResolvedSchema.of(Column.metadata("metadata", DataTypes.STRING(), null, true));
        assertThatThrownBy(
                        () -> FactoryMocks.createTableSource(noPhysicalColumns, minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must declare at least one physical column");

        for (String missing : List.of("dataset", "table")) {
            Map<String, String> options = minimalOptions();
            options.remove(missing);
            assertThatThrownBy(() -> source(options))
                    .as("table source without '%s'", missing)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(missing);
        }

        Map<String, String> missingProject = minimalOptions();
        missingProject.remove("project");
        missingProject.remove("dataset");
        missingProject.remove("table");
        missingProject.put("source.query", "SELECT 1");
        assertThatThrownBy(() -> source(missingProject))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("option 'project' or 'source.parent-project'");

        Map<String, String> blankQuery = minimalOptions();
        blankQuery.put("source.query", "  ");
        assertThatThrownBy(() -> source(blankQuery))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("source.query")
                .hasStackTraceContaining("must not be blank");

        Map<String, String> queryAndView = minimalOptions();
        queryAndView.put("source.query", "SELECT 1");
        queryAndView.put("source.materialize-views", "true");
        assertThatThrownBy(() -> source(queryAndView))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined");

        Map<String, String> malformedSnapshot = minimalOptions();
        malformedSnapshot.put("source.snapshot-time", "yesterday");
        assertThatThrownBy(() -> source(malformedSnapshot))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("source.snapshot-time")
                .hasStackTraceContaining("ISO-8601 instant");
    }

    @Test
    void carriesSourceParallelism() {
        Map<String, String> options = minimalOptions();
        options.put("scan.parallelism", "4");
        org.apache.flink.table.connector.source.ScanTableSource source =
                (org.apache.flink.table.connector.source.ScanTableSource) source(options);

        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider.getParallelism()).hasValue(4);
    }

    @Test
    void theSinkItBuildsIsTheConnectorsOwn() {
        SinkV2Provider provider =
                (SinkV2Provider)
                        sink(minimalOptions())
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        // Without sink.write-method, the connector's own default write method.
        assertThat(provider.createSink()).isInstanceOf(BigQueryDefaultStreamSink.class);
        assertThat(provider.getParallelism()).isEmpty();
    }

    @Test
    void carriesTheSinkParallelismWhenItIsSet() {
        Map<String, String> options = minimalOptions();
        options.put("sink.parallelism", "3");
        SinkV2Provider provider =
                (SinkV2Provider)
                        sink(options).getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider.getParallelism()).hasValue(3);
    }

    @Test
    void rejectsATableWithoutItsThreeDestinationParts() {
        for (String missing : new String[] {"project", "dataset", "table"}) {
            Map<String, String> options = minimalOptions();
            options.remove(missing);
            assertThatThrownBy(() -> sink(options))
                    .as("without '%s'", missing)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(missing);
        }
    }

    @Test
    void rejectsAnUnknownOption() {
        Map<String, String> options = minimalOptions();
        // A near miss of a real key, which is how one is usually written.
        options.put("sink.write_method", "storage-api-at-least-once");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.write_method");
    }

    @Test
    void everyWriteMethodBuildsItsOwnSink() {
        // Also pins the DDL spelling against the sink it selects: a WriteMethod whose toString()
        // drifted would build the wrong one of these rather than fail.
        assertThat(built(optionsFor(WriteMethod.STORAGE_API_AT_LEAST_ONCE)))
                .isInstanceOf(BigQueryDefaultStreamSink.class);
        assertThat(built(optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE)))
                .isInstanceOf(BigQueryBufferedStreamSink.class);
        assertThat(built(optionsFor(WriteMethod.FILE_LOADS)))
                .isInstanceOf(BigQueryFileLoadsSink.class);
    }

    @Test
    void aWriteMethodThatTunesNothingStillGetsItsRequiredOptions() {
        // The reason the two required families are built from the write method rather than from
        // key presence: the builder demands the object, the DDL is correct, and there is no key to
        // trigger a presence scan. Every knob is then the connector's own default.
        assertThat(
                        ((BigQueryBufferedStreamSink<?>)
                                        built(optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE)))
                                .getOptions())
                .isEqualTo(BufferedStreamOptions.builder().build());
        assertThat(
                        ((BigQueryFileLoadsSink<?>) built(optionsFor(WriteMethod.FILE_LOADS)))
                                .getOptions())
                .isEqualTo(FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build());
    }

    @Test
    void bufferedStreamKeysReachTheBuiltSink() {
        Map<String, String> options = optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE);
        options.put("sink.buffered-stream.retry.max-attempts", "7");
        options.put("sink.buffered-stream.max-append-request-bytes", "1 mb");
        BufferedStreamOptions built = ((BigQueryBufferedStreamSink<?>) built(options)).getOptions();
        assertThat(built.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(built.getMaxAppendRequestBytes()).isEqualTo(1024L * 1024L);
    }

    @Test
    void fileLoadsKeysReachTheBuiltSink() {
        // No sink.file-loads.write-disposition here, deliberately: FactoryMocks builds its context
        // over an empty Configuration, where execution.runtime-mode takes its STREAMING default,
        // and the factory refuses a non-append disposition there. That the key maps onto the knob
        // at all is FileLoadsOptionsMapperTest's mapsEveryOptionOntoItsKnob; what this test is for
        // — that the mapper's output reaches the built sink rather than being dropped on the way
        // to BigQuerySink.builder() — the two knobs below carry unchanged.
        Map<String, String> options = optionsFor(WriteMethod.FILE_LOADS);
        options.put("sink.file-loads.temp-dataset", "staging_dataset");
        options.put("sink.file-loads.schema-reconcile.max-attempts", "3");
        FileLoadsOptions built = ((BigQueryFileLoadsSink<?>) built(options)).getOptions();
        assertThat(built.getStagingPath()).isEqualTo("gs://bucket/prefix");
        assertThat(built.getTempDataset()).isEqualTo("staging_dataset");
        assertThat(built.getSchemaReconcileMaxAttempts()).isEqualTo(3);
    }

    @Test
    void aNonAppendWriteDispositionUnderFileLoadsIsRejectedByKeyName() {
        // What this adds over its planner-level twin, which asserts the same two strings: the
        // mode here is *defaulted* rather than set, since FactoryMocks builds over an empty
        // Configuration and execution.runtime-mode's own default is STREAMING. A regression that
        // only broke the defaulted path would be caught here alone. It also configures no
        // checkpointing, which is what pins that this rule speaks regardless — unlike its
        // sibling, an interval below the floor, which has nothing to compare when there is no
        // interval and which FactoryMocks cannot reach anyway (no sink overload carries a session
        // Configuration).
        Map<String, String> options = optionsFor(WriteMethod.FILE_LOADS);
        options.put("sink.file-loads.write-disposition", "write-truncate");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                // The clause is the factory's; BigQueryFileLoadsSink's own message for this rule
                // says "supports WriteDisposition.WRITE_APPEND only".
                .hasStackTraceContaining("cannot be used in streaming execution")
                // Key and value are both in the WITH clause, so what separates this from
                // FactoryUtil's dump is only the spaces around '='; the clause above is what
                // actually discriminates, and this asserts the DDL spelling reached the message.
                .hasStackTraceContaining(
                        "Option 'sink.file-loads.write-disposition' = 'write-truncate'");
    }

    @Test
    void aTuningKeyOfAnotherWriteMethodIsRejectedByKeyName() {
        // Each family under each write method that does not own it — six cases, the whole matrix,
        // because a check written for one family is a check that could have missed the others. The
        // builder rejects the pair too, naming bufferedStreamOptions(...): a method a SQL user
        // cannot call.
        Map<WriteMethod, String> familyKeys = new LinkedHashMap<>();
        familyKeys.put(
                WriteMethod.STORAGE_API_AT_LEAST_ONCE, "sink.default-stream.max-inflight-requests");
        familyKeys.put(
                WriteMethod.STORAGE_API_EXACTLY_ONCE, "sink.buffered-stream.retry.max-attempts");
        familyKeys.put(WriteMethod.FILE_LOADS, "sink.file-loads.schema-reconcile.max-attempts");

        familyKeys.forEach(
                (owner, key) -> {
                    for (WriteMethod selected : WriteMethod.values()) {
                        if (selected == owner) {
                            continue;
                        }
                        Map<String, String> options = optionsFor(selected);
                        options.put(key, "7");
                        assertThatThrownBy(() -> sink(options))
                                .as("'%s' under '%s'", key, selected)
                                .isInstanceOf(ValidationException.class)
                                // A phrase only this connector's message carries. FactoryUtil
                                // attaches a dump of the whole WITH clause to anything the factory
                                // throws, so asserting the key alone would pass with the check
                                // deleted — measured.
                                .hasStackTraceContaining("but this table's write method is")
                                .hasStackTraceContaining(key);
                    }
                });
    }

    @Test
    void aTuningKeyIsRejectedWhenNoWriteMethodIsNamedEither() {
        // The case the matrix above cannot reach, because it always writes sink.write-method: a
        // table that names no write method is on the connector's default, and a key of another
        // family is as wrong there as anywhere. Without this a check that simply returned when the
        // option was absent would pass the whole suite — and the keys would then be dropped in
        // silence, since the sink only builds a family whose write method matches.
        Map<String, String> options = minimalOptions();
        options.put("sink.buffered-stream.retry.max-attempts", "7");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("but this table's write method is")
                .hasStackTraceContaining("sink.buffered-stream.retry.max-attempts");
    }

    @Test
    void aMissingStagingPathUnderFileLoadsIsRejectedByKeyName() {
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", "file-loads");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("no default location to stage them in")
                .hasStackTraceContaining("sink.file-loads.staging-path");
    }

    @Test
    void theWriteMethodBeingUnusableIsReportedAheadOfWhatIsConfiguredUnderIt() {
        // Ordering, and it is load-bearing: TableCreateOptionsMapper throws too, and while the
        // staging-path check sat inside the builder chain it was evaluated first — so a FILE_LOADS
        // table with nowhere to stage was told about its create disposition instead.
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", "file-loads");
        options.put("sink.create-disposition", "create-never");
        options.put("sink.table-create.clustered-fields", "id");

        Throwable thrown = catchThrowable(() -> sink(options));

        assertThat(thrown).isInstanceOf(ValidationException.class);
        assertThat(Throwables.getStackTrace(thrown))
                .contains("no default location to stage them in")
                .doesNotContain("configure a table this sink never creates");
    }

    @Test
    void schemaEvolutionUnderExactlyOnceIsAccepted() {
        Map<String, String> options = optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE);
        options.put("sink.schema-update.allow-field-relaxation", "true");
        assertThat(built(options)).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void aSchemaUpdateKeySetToFalseIsAcceptedUnderExactlyOnce() {
        // The check fires on the same condition the builder uses — an *enabled* options object —
        // so a key present and false is no more a schema update here than it is there. Without
        // this the check could tighten to mere presence and nothing would notice.
        Map<String, String> options = optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE);
        options.put("sink.schema-update.allow-new-fields", "false");
        assertThat(built(options)).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void anEmulatorEndpointUnderFileLoadsIsRejectedByKeyName() {
        for (String key : new String[] {"emulator-endpoint", "emulator-rest-endpoint"}) {
            Map<String, String> options = optionsFor(WriteMethod.FILE_LOADS);
            options.put(key, "localhost:9060");
            assertThatThrownBy(() -> sink(options))
                    .as("with '%s'", key)
                    .isInstanceOf(ValidationException.class)
                    // Again the opening clause: the builder says "which the BigQuery emulator
                    // does not provide", one word away from the tail of this one.
                    .hasStackTraceContaining("point at a BigQuery emulator")
                    .hasStackTraceContaining(key);
        }
    }

    @Test
    void rejectsAnUpdatingQueryByBeingInsertOnly() {
        assertThat(
                        sink(minimalOptions())
                                .getChangelogMode(
                                        org.apache.flink.table.connector.ChangelogMode.all()))
                .isEqualTo(org.apache.flink.table.connector.ChangelogMode.insertOnly());
    }

    @Test
    void cdcRequiresADeclaredPrimaryKeyAndTheDefaultStream() {
        Map<String, String> noPrimaryKey = minimalOptions();
        noPrimaryKey.put("sink.cdc.enabled", "true");
        assertThatThrownBy(() -> sink(noPrimaryKey))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires the sink table to declare a PRIMARY KEY");

        for (WriteMethod unsupported :
                Arrays.asList(WriteMethod.STORAGE_API_EXACTLY_ONCE, WriteMethod.FILE_LOADS)) {
            Map<String, String> options = optionsFor(unsupported);
            options.put("sink.cdc.enabled", "true");
            assertThatThrownBy(() -> FactoryMocks.createTableSink(withPrimaryKey(SCHEMA), options))
                    .as("CDC under %s", unsupported)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(
                            "accepted only on the Storage Write API default stream")
                    .hasStackTraceContaining("sink.cdc.enabled")
                    .hasStackTraceContaining("sink.write-method");
        }

        Map<String, String> autoCreate = minimalOptions();
        autoCreate.put("sink.cdc.enabled", "true");
        autoCreate.put("sink.cdc.max-staleness", "10 min");
        BigQueryDefaultStreamSink<?> sink =
                (BigQueryDefaultStreamSink<?>) built(withPrimaryKey(SCHEMA), autoCreate);
        CdcTableOptions cdcTable =
                sink.getConfig().getCdcTableOptionsProvider().optionsFor(DESTINATION);
        assertThat(cdcTable.getPrimaryKeyColumns()).containsExactly("id");
        assertThat(cdcTable.getMaxStaleness()).isEqualTo(Duration.ofMinutes(10));
        assertThat(sink.getConfig().getCdcTableReconciliationPolicy())
                .isEqualTo(CdcTableReconciliationPolicy.VERIFY_ONLY);
    }

    @Test
    void cdcTableOptionsRequireCdcButNotCreationPermission() {
        Map<String, String> withoutCdc = minimalOptions();
        withoutCdc.put("sink.cdc.max-staleness", "10 min");
        assertThatThrownBy(() -> built(withoutCdc))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("CDC table options require")
                .hasStackTraceContaining("sink.cdc.enabled");

        Map<String, String> createNever = minimalOptions();
        createNever.put("sink.cdc.enabled", "true");
        createNever.put("sink.create-disposition", "create-never");
        createNever.put("sink.cdc.max-staleness", "10 min");
        createNever.put("sink.cdc.table-reconciliation", "reconcile");
        BigQueryDefaultStreamSink<?> sink =
                (BigQueryDefaultStreamSink<?>) built(withPrimaryKey(SCHEMA), createNever);
        assertThat(sink.getConfig().getCreateDisposition().name()).isEqualTo("CREATE_NEVER");
        assertThat(sink.getConfig().getCdcTableReconciliationPolicy())
                .isEqualTo(CdcTableReconciliationPolicy.RECONCILE);
    }

    @Test
    void mysqlSourceUuidEpochsRequireCdcAndAreValidatedDuringPlanning() {
        Map<String, String> withoutCdc = minimalOptions();
        withoutCdc.put(
                "sink.cdc.debezium-mysql.source-uuids", "24bc7850-2c16-11e6-a073-0242ac110002");
        assertThatThrownBy(() -> built(withoutCdc))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.cdc.debezium-mysql.source-uuids' requires")
                .hasStackTraceContaining("sink.cdc.enabled");

        Map<String, String> invalid = minimalOptions();
        invalid.put("sink.cdc.enabled", "true");
        invalid.put("sink.cdc.debezium-mysql.source-uuids", "not-a-uuid");
        assertThatThrownBy(() -> built(withPrimaryKey(SCHEMA), invalid))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("canonical UUID");
    }

    @Test
    @SuppressWarnings("unchecked")
    void mysqlSourceUuidListSyntaxPreservesFailoverEpochOrder() {
        String firstSid = "24bc7850-2c16-11e6-a073-0242ac110002";
        String secondSid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        ResolvedSchema metadataSchema =
                withPrimaryKey(
                        ResolvedSchema.of(
                                Column.physical("id", DataTypes.STRING()),
                                Column.metadata(
                                        "source_properties",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                        "debezium-source-properties",
                                        false)));
        Map<String, String> options = minimalOptions();
        options.put("sink.cdc.enabled", "true");
        options.put("sink.create-disposition", "create-never");
        options.put("sink.cdc.debezium-mysql.source-uuids", firstSid + ";" + secondSid);
        BigQueryDefaultStreamSink<RowData> sink =
                (BigQueryDefaultStreamSink<RowData>)
                        builtWithMetadata(metadataSchema, options, "debezium-source-properties");
        CdcSequenceNumberProvider<? super RowData> provider =
                sink.getConfig().getCdcOptions().getSequenceNumberProvider();
        Map<StringData, StringData> properties = new LinkedHashMap<>();
        properties.put(StringData.fromString("connector"), StringData.fromString("mysql"));
        properties.put(StringData.fromString("snapshot"), StringData.fromString("false"));
        properties.put(StringData.fromString("gtid"), StringData.fromString(secondSid + ":1"));
        properties.put(StringData.fromString("pos"), StringData.fromString("2"));
        properties.put(StringData.fromString("row"), StringData.fromString("3"));

        assertThat(
                        provider.getSequenceNumber(
                                GenericRowData.of(
                                        StringData.fromString("id"),
                                        new GenericMapData(properties))))
                .isEqualTo("0000000000000002/0000000000000001/0000000000000002/0000000000000003");
    }

    @Test
    void theTiCdcClusterIdRequiresCdcAndIsValidatedDuringPlanning() {
        Map<String, String> withoutCdc = minimalOptions();
        withoutCdc.put("sink.cdc.ticdc.cluster-id", "test_cluster");
        assertThatThrownBy(() -> built(withoutCdc))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.cdc.ticdc.cluster-id' requires")
                .hasStackTraceContaining("sink.cdc.enabled");

        Map<String, String> empty = minimalOptions();
        empty.put("sink.cdc.enabled", "true");
        empty.put("sink.cdc.ticdc.cluster-id", "");
        assertThatThrownBy(() -> built(withPrimaryKey(SCHEMA), empty))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.cdc.ticdc.cluster-id' must name the cluster");
    }

    @Test
    @SuppressWarnings("unchecked")
    void theConfiguredTiCdcClusterIdReachesTheSequenceProvider() {
        ResolvedSchema metadataSchema =
                withPrimaryKey(
                        ResolvedSchema.of(
                                Column.physical("id", DataTypes.STRING()),
                                Column.metadata(
                                        "source_properties",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                        "debezium-source-properties",
                                        false)));
        Map<String, String> options = minimalOptions();
        options.put("sink.cdc.enabled", "true");
        options.put("sink.create-disposition", "create-never");
        options.put("sink.cdc.ticdc.cluster-id", "test_cluster");
        BigQueryDefaultStreamSink<RowData> sink =
                (BigQueryDefaultStreamSink<RowData>)
                        builtWithMetadata(metadataSchema, options, "debezium-source-properties");
        CdcSequenceNumberProvider<? super RowData> provider =
                sink.getConfig().getCdcOptions().getSequenceNumberProvider();

        assertThat(provider.getSequenceNumber(tiCdcRow("test_cluster")))
                .isEqualTo("063D35BACF7D0003");
        assertThatThrownBy(() -> provider.getSequenceNumber(tiCdcRow("other_cluster")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("other_cluster");
    }

    private static GenericRowData tiCdcRow(String clusterId) {
        Map<StringData, StringData> properties = new LinkedHashMap<>();
        properties.put(StringData.fromString("connector"), StringData.fromString("TiCDC"));
        properties.put(StringData.fromString("snapshot"), StringData.fromString("false"));
        properties.put(
                StringData.fromString("commit_ts"), StringData.fromString("449574614268182531"));
        properties.put(StringData.fromString("cluster_id"), StringData.fromString(clusterId));
        return GenericRowData.of(StringData.fromString("id"), new GenericMapData(properties));
    }

    @Test
    void cdcReachesTheBuiltDefaultStreamAndIsDisabledByDefault() {
        BigQueryDefaultStreamSink<?> ordinary =
                (BigQueryDefaultStreamSink<?>) built(withPrimaryKey(SCHEMA), minimalOptions());
        assertThat(ordinary.getConfig().getCdcOptions()).isNull();

        Map<String, String> options = minimalOptions();
        options.put("sink.cdc.enabled", "true");
        options.put("sink.create-disposition", "create-never");
        BigQueryDefaultStreamSink<?> cdc =
                (BigQueryDefaultStreamSink<?>) built(withPrimaryKey(SCHEMA), options);
        assertThat(cdc.getConfig().getCdcOptions()).isNotNull();
        assertThat(cdc.getConfig().getCdcOptions().hasSequenceNumberProvider()).isFalse();
    }

    @Test
    void plannerSelectedSequenceMetadataReachesTheCdcConfiguration() {
        ResolvedSchema sequenceSchema =
                withPrimaryKey(
                        ResolvedSchema.of(
                                Column.physical("id", DataTypes.STRING()),
                                Column.physical("amount", DataTypes.INT()),
                                Column.metadata(
                                        "sequence",
                                        DataTypes.STRING(),
                                        "change-sequence-number",
                                        false)));
        Map<String, String> options = minimalOptions();
        options.put("sink.cdc.enabled", "true");
        options.put("sink.create-disposition", "create-never");

        BigQueryDefaultStreamSink<?> cdc =
                (BigQueryDefaultStreamSink<?>)
                        builtWithMetadata(sequenceSchema, options, "change-sequence-number");

        assertThat(cdc.getConfig().getCdcOptions().hasSequenceNumberProvider()).isTrue();
        assertThat(cdc.getConfig().getSerializer().getTableSchema(DESTINATION).getFieldsList())
                .extracting(com.google.cloud.bigquery.storage.v1.TableFieldSchema::getName)
                .containsExactly("id", "amount");
    }

    @Test
    void selectingBothSequenceMetadataSourcesFailsAtPlanning() {
        ResolvedSchema both =
                withPrimaryKey(
                        ResolvedSchema.of(
                                Column.physical("id", DataTypes.STRING()),
                                Column.metadata(
                                        "sequence",
                                        DataTypes.STRING(),
                                        "change-sequence-number",
                                        false),
                                Column.metadata(
                                        "source_properties",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                        "debezium-source-properties",
                                        false)));
        Map<String, String> options = minimalOptions();
        options.put("sink.cdc.enabled", "true");
        options.put("sink.create-disposition", "create-never");

        assertThatThrownBy(
                        () ->
                                builtWithMetadata(
                                        both,
                                        options,
                                        "change-sequence-number",
                                        "debezium-source-properties"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Select exactly one BigQuery CDC sequence source");
    }

    @Test
    void aDefaultStreamKeyReachesTheBuiltSink() {
        Map<String, String> options = minimalOptions();
        options.put("sink.default-stream.max-inflight-requests", "7");
        BigQueryDefaultStreamSink<?> built = (BigQueryDefaultStreamSink<?>) built(options);
        assertThat(built.getOptions().getMaxInflightRequests()).isEqualTo(7);
    }

    @Test
    void theSchemaAndEmulatorOptionsReachTheBuiltSink() {
        Map<String, String> options = minimalOptions();
        options.put("sink.location", "asia-northeast1");
        options.put("sink.create-disposition", "create-never");
        options.put("emulator-endpoint", "localhost:9060");
        options.put("emulator-rest-endpoint", "localhost:9050");
        BigQueryDefaultStreamSink<?> built = (BigQueryDefaultStreamSink<?>) built(options);
        assertThat(built.getConfig().getLocation()).isEqualTo("asia-northeast1");
        assertThat(built.getConfig().getCreateDisposition().name()).isEqualTo("CREATE_NEVER");
        assertThat(built.getConfig().getEmulatorEndpoint().getTarget()).isEqualTo("localhost:9060");
        assertThat(built.getConfig().getEmulatorRestEndpoint().getTarget())
                .isEqualTo("localhost:9050");
    }

    @Test
    void serviceAccountKeyFileReachesTheBuiltSink() {
        for (WriteMethod writeMethod : WriteMethod.values()) {
            Map<String, String> options = optionsFor(writeMethod);
            options.put("service-account-key-file", "/var/run/secrets/bigquery-key.json");

            assertThat(configOf(built(options)).getServiceAccountKeyFile())
                    .as("credential path under %s", writeMethod)
                    .isEqualTo("/var/run/secrets/bigquery-key.json");
        }
    }

    @Test
    void blankServiceAccountKeyFileIsRejectedByKeyName() {
        Map<String, String> options = minimalOptions();
        options.put("service-account-key-file", "  ");

        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'service-account-key-file' must not be blank.");
    }

    @Test
    void serviceAccountKeyFileCannotBeCombinedWithEitherEmulatorEndpoint() {
        for (String key : new String[] {"emulator-endpoint", "emulator-rest-endpoint"}) {
            Map<String, String> options = minimalOptions();
            options.put("service-account-key-file", "/var/run/secrets/bigquery-key.json");
            options.put(key, "localhost:9060");

            assertThatThrownBy(() -> sink(options))
                    .as("with '%s'", key)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("service-account-key-file")
                    .hasStackTraceContaining("emulator connections are credential-free");
        }
    }

    /**
     * Issue #1019: the rejection names the option key the DDL carried rather than the {@code
     * emulatorEndpoint(...)} or {@code emulatorRestEndpoint(...)} setter the value used to reach.
     * Each key names itself, which is why the factory makes two calls rather than one.
     *
     * <p>Asserted on the root cause. {@code FactoryUtil} dumps every {@code WITH} option into its
     * own message, so a needle of just the key would pass with the parse deleted; the root cause is
     * the {@code IllegalArgumentException} the parse throws and carries nothing else. The needle
     * also discriminates the fix, since {@code emulator-endpoint must be} is not a substring of
     * {@code emulatorEndpoint must be}.
     *
     * <p>The direct-table source arm is the one that changes most: it leaves {@code
     * emulator-rest-endpoint} unused, so before this nothing parsed that value at all. A
     * well-formed one is still accepted and still unused, which {@code
     * directTableSourceLeavesTheSinkRestEndpointUnused} holds.
     *
     * <p>Two values, not a catalogue. {@code "localhost"} exercises the shape, and {@code ""} the
     * one thing that is this layer's rather than the parser's: whether an option written {@code ''}
     * arrives as present-and-empty rather than absent, so the check sees it at all. The rejection
     * set itself belongs to {@code EmulatorEndpointTest}.
     */
    @Test
    void rejectsAMalformedEmulatorEndpointByKeyNameOnEveryPath() {
        for (String key : new String[] {"emulator-endpoint", "emulator-rest-endpoint"}) {
            for (String malformed : new String[] {"localhost", ""}) {
                String message = key + " must be host:port, was '" + malformed + "'";

                Map<String, String> sinkOptions = minimalOptions();
                sinkOptions.put(key, malformed);
                assertThatThrownBy(() -> sink(sinkOptions))
                        .as("sink, '%s' = '%s'", key, malformed)
                        .isInstanceOf(ValidationException.class)
                        .rootCause()
                        .hasMessage(message);

                Map<String, String> tableSource = minimalOptions();
                tableSource.put(key, malformed);
                assertThatThrownBy(() -> source(tableSource))
                        .as("direct-table source, '%s' = '%s'", key, malformed)
                        .isInstanceOf(ValidationException.class)
                        .rootCause()
                        .hasMessage(message);

                Map<String, String> querySource = minimalOptions();
                querySource.put("source.query", "SELECT id, amount FROM `p.d.t`");
                querySource.put(key, malformed);
                assertThatThrownBy(() -> source(querySource))
                        .as("query source, '%s' = '%s'", key, malformed)
                        .isInstanceOf(ValidationException.class)
                        .rootCause()
                        .hasMessage(message);
            }
        }
    }

    /**
     * Pins the endpoint parse behind every check this factory makes that refuses an option outright
     * — a DDL told to remove an option is not helped by an answer about that option's shape. The
     * option pre-empted need not be an endpoint: the source arm here is refused for combining a
     * query with view materialization.
     *
     * <p>The last two arms pin something else: this is the one factory that declares no required
     * options, because one factory serves a sink, a direct table source and a query source, which
     * need different ones. Everywhere else {@code helper.validate()} reports a missing required
     * option before any connector check runs, so the parse follows {@code destination(...)} and
     * {@code parentProject(...)} here to keep that answer the same.
     *
     * <p>Asserted on the root cause and paired with the negative: with the parse moved above {@code
     * checkEmulatorEndpointsAreSupported}, {@code checkCredentials}, the query checks or the
     * destination checks, the root cause becomes the {@code IllegalArgumentException}, whose
     * message these phrases do not appear in.
     *
     * <p>Green on {@code origin/main} by construction. It guards the ordering, not the fix.
     */
    @Test
    void refusesAnOptionOutrightBeforeReportingTheEndpointShape() {
        Map<String, String> fileLoads = optionsFor(WriteMethod.FILE_LOADS);
        fileLoads.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> sink(fileLoads))
                .as("an emulator endpoint under file-loads")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("point at a BigQuery emulator")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> credentials = minimalOptions();
        credentials.put("service-account-key-file", "/var/run/secrets/bigquery-key.json");
        credentials.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> sink(credentials))
                .as("an emulator endpoint beside a key file")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("emulator connections are credential-free")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> queriedView = minimalOptions();
        queriedView.put("source.query", "SELECT id, amount FROM `p.d.t`");
        queriedView.put("source.materialize-views", "true");
        queriedView.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> source(queriedView))
                .as("a query beside view materialization")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("a query is already materialized")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> sinkWithoutTable = minimalOptions();
        sinkWithoutTable.remove("table");
        sinkWithoutTable.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> sink(sinkWithoutTable))
                .as("a sink that has not said where it points")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("A 'bigquery' sink requires options")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> queryWithoutProject = minimalOptions();
        queryWithoutProject.remove("project");
        queryWithoutProject.put("source.query", "SELECT id, amount FROM `p.d.t`");
        queryWithoutProject.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> source(queryWithoutProject))
                .as("a query source with no billing project")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("query source requires option")
                .hasMessageNotContaining("must be host:port");
    }

    @Test
    void schemaUpdateKeysReachTheBuiltSinkAndTheirAbsenceLeavesTheDefault() {
        BigQueryDefaultStreamSink<?> defaults =
                (BigQueryDefaultStreamSink<?>) built(minimalOptions());
        assertThat(defaults.getConfig().getSchemaUpdateOptions().isEnabled()).isFalse();

        Map<String, String> options = minimalOptions();
        options.put("sink.schema-update.allow-new-fields", "true");
        BigQueryDefaultStreamSink<?> built = (BigQueryDefaultStreamSink<?>) built(options);
        assertThat(built.getConfig().getSchemaUpdateOptions().isAllowNewFields()).isTrue();
        assertThat(built.getConfig().getSchemaUpdateOptions().isAllowFieldRelaxation()).isFalse();
    }

    @Test
    void tableCreateKeysReachTheBuiltSinkAndTheirAbsenceLeavesAPlainTable() {
        BigQueryDefaultStreamSink<?> defaults =
                (BigQueryDefaultStreamSink<?>) built(minimalOptions());
        assertThat(defaults.getConfig().getTableCreateOptionsProvider().optionsFor(DESTINATION))
                .isEqualTo(TableCreateOptions.defaults());

        Map<String, String> options = minimalOptions();
        options.put("sink.table-create.time-partitioning.type", "day");
        options.put("sink.table-create.time-partitioning.field", "event_ts");
        options.put("sink.table-create.clustered-fields", "id");
        BigQueryDefaultStreamSink<?> built =
                (BigQueryDefaultStreamSink<?>) built(PARTITIONABLE, options);
        TableCreateOptions created =
                built.getConfig().getTableCreateOptionsProvider().optionsFor(DESTINATION);
        assertThat(created.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.DAY);
        assertThat(created.getTimePartitioningField()).isEqualTo("event_ts");
        assertThat(created.getClusteredFields()).containsExactly("id");
    }

    @Test
    void aTableCreateColumnOutsideTheDdlIsRejected() {
        // The check only this layer can make: the emulator would accept the create request and
        // real BigQuery would refuse it, so a plan-time failure is what keeps the two apart.
        Map<String, String> options = minimalOptions();
        options.put("sink.table-create.clustered-fields", "no_such_column");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                // A phrase only the connector's own message carries. Asserting the option key or
                // the column name would pass with the check deleted, because FactoryUtil dumps
                // every WITH option into the ValidationException it wraps this in — measured.
                .hasStackTraceContaining("which the table does not declare")
                .hasStackTraceContaining("sink.table-create.clustered-fields")
                .hasStackTraceContaining("no_such_column");
    }

    @Test
    void aSchemaProblemFailsWhenTheJobGraphIsBuilt() {
        // The eager-derivation rule: an unmappable column must not wait until serialize() runs
        // inside the writers' failure handler.
        ResolvedSchema unmappable =
                ResolvedSchema.of(Column.physical("v", DataTypes.INTERVAL(DataTypes.DAY())));
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(unmappable, minimalOptions())
                                        .getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
    }

    @Test
    void namesTheOptionKeyWhenASourceValueIsRejected() {
        // The source has no mapper: the factory hands raw values to the dynamic source, whose
        // builder setters run at getScanRuntimeProvider — the rename must reach that path too.
        Map<String, String> options = minimalOptions();
        options.put("source.max-records-per-fetch", "0");

        assertThatThrownBy(() -> builtSource(source(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'source.max-records-per-fetch' is invalid")
                .hasMessageContaining("maxRecordsPerFetch must be positive");
    }

    @Test
    void namesTheOptionKeyWhenTheSinkLocationIsRejected() {
        Map<String, String> options = minimalOptions();
        options.put("sink.location", "   ");

        assertThatThrownBy(() -> built(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.location' is invalid")
                .hasMessageContaining("location must not be blank");
    }

    /**
     * The billing project is the one value fed by either of two options, so its rename happens in
     * the factory at the point that knows which key supplied it, not at the setter seam. Both arms
     * are driven because the fallback is the one the seam could never attribute: a caller who wrote
     * only {@code project} must be answered about {@code project}, not about the {@code
     * source.parent-project} their DDL does not contain — which is exactly why the fallback ran.
     *
     * <p>Fires at the factory rather than at {@code getScanRuntimeProvider}, so {@code source} is
     * enough; the builder's own check remains behind it for the DataStream API.
     */
    @Test
    void namesTheSupplyingOptionKeyWhenTheBillingProjectIsRejected() {
        Map<String, String> parent = minimalOptions();
        parent.put("source.query", "SELECT 1");
        parent.put("source.parent-project", "a/b");
        assertThatThrownBy(() -> source(parent))
                .as("source.parent-project supplied it")
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'source.parent-project' is invalid")
                .hasStackTraceContaining("parentProject must not contain '/': 'a/b'");

        Map<String, String> fallback = minimalOptions();
        fallback.put("source.query", "SELECT 1");
        fallback.put("project", "a/b");
        assertThatThrownBy(() -> source(fallback))
                .as("project supplied it, as the fallback")
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'project' is invalid")
                .hasStackTraceContaining("parentProject must not contain '/': 'a/b'");
    }
}
