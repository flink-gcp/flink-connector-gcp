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
import org.apache.flink.table.api.ValidationException;
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
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
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

    /**
     * Every option change-stream mode owns, one legal value each. Bounded mode rejects all of them,
     * and so does a sink: neither can act on one, and accepting it silently is what lets a copied
     * setting plan successfully while doing nothing. Adding one to {@code
     * rejectChangeStreamOptions} without adding it here leaves it untested in <em>both</em>
     * directions rather than failing.
     */
    private static final Map<String, String> CHANGE_STREAM_OWNED_OPTIONS =
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
                .isEqualTo(DatabaseDestination.of("my-project", "my-instance", "my-database"));
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
    void rejectsWhitespaceThatTrimDoesNotStrip() {
        // U+2028 is the value that tells the two blank idioms apart: Character.isWhitespace calls
        // it whitespace, and String.trim() leaves it alone because it sits above U+0020. The
        // "  " assertions elsewhere in this class pass under either idiom; these two fail if the
        // checks behind them return to trim().isEmpty(). SpannerIdentifier has no test class of
        // its own and is reached only this way.
        Map<String, String> blankTable = options();
        blankTable.put("schema", "analytics");
        blankTable.put("table", "\u2028");
        assertThatThrownBy(() -> source(SCHEMA, blankTable))
                .hasStackTraceContaining("table must be one non-blank GoogleSQL identifier");

        Map<String, String> blankKeyFile = options();
        blankKeyFile.put("service-account-key-file", "\u2028");
        assertThatThrownBy(() -> source(SCHEMA, blankKeyFile))
                .hasStackTraceContaining("service-account-key-file must not be blank");
    }

    @Test
    void mapsEveryWriterOptionAndTheEndpoint() {
        Map<String, String> options = options();
        options.put("sink.buffer-flush.max-cells", "100");
        options.put("sink.buffer-flush.max-mutations", "90");
        options.put("sink.buffer-flush.max-size", "2 mb");
        options.put("sink.buffer-flush.max-commit-delay", "25 ms");
        options.put("sink.rpc-priority", "low");
        options.put("sink.recovery.initial-backoff", "2 s");
        options.put("sink.recovery.max-backoff", "8 s");
        options.put("sink.recovery.max-attempts", "4");
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
        assertThat(sink.getConfig().getWriterOptions().getRecoveryInitialBackoff())
                .isEqualTo(Duration.ofSeconds(2));
        assertThat(sink.getConfig().getWriterOptions().getRecoveryMaxBackoff())
                .isEqualTo(Duration.ofSeconds(8));
        assertThat(sink.getConfig().getWriterOptions().getRecoveryMaxAttempts()).isEqualTo(4);
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

    /**
     * The bounded-source arm is the lookup arm: {@code createDynamicTableSource} is the only entry
     * point for a lookup table, because {@code SpannerDynamicSource} serves both {@code
     * ScanTableSource} and {@code LookupTableSource}. So this covers the case issue #1013 is about
     * — a table joined as a lookup dimension, which used to carry the value to a TaskManager and
     * parse it there.
     *
     * <p>The change-stream arm is driven because the call sits before the mode branch and must stay
     * there: pushing it into the bounded branch is what this arm fails on.
     *
     * <p>Two values, not a catalogue. {@code "localhost"} exercises the shape, and {@code ""} the
     * one thing that is this layer's rather than the parser's: whether an option written {@code ''}
     * arrives as present-and-empty rather than absent, so the check sees it at all. The rejection
     * set itself belongs to {@code EmulatorEndpointTest}.
     */
    @Test
    void rejectsAMalformedEmulatorEndpointOnEveryPath() {
        for (String malformed : new String[] {"localhost", ""}) {
            String message = "emulator-endpoint must be host:port, was '" + malformed + "'";

            Map<String, String> bounded = options();
            bounded.put("emulator-endpoint", malformed);
            assertThatThrownBy(() -> sink(SCHEMA, bounded))
                    .as("sink, '%s'", malformed)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(message);
            assertThatThrownBy(() -> source(SCHEMA, bounded))
                    .as("bounded scan and lookup, '%s'", malformed)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(message);

            Map<String, String> changeStream = changeStreamOptions("full");
            changeStream.put("emulator-endpoint", malformed);
            assertThatThrownBy(() -> source(SCHEMA, changeStream))
                    .as("change stream, '%s'", malformed)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(message);
        }
    }

    /**
     * Pins the endpoint parse behind every check that refuses an option outright. An emulator
     * endpoint is itself legal in every Spanner mode, so what would be pre-empted here is another
     * option's removal message — and a DDL told to remove {@code scan.index} is not helped by an
     * answer about the endpoint's shape.
     *
     * <p>Asserted on the root cause and on a phrase, and paired with the negative: with the parse
     * moved above {@code validateSourceMode} or {@code rejectChangeStreamOptions} the root cause
     * becomes the {@code IllegalArgumentException}, whose message these phrases do not appear in.
     *
     * <p>Green on {@code origin/main} by construction. It guards the ordering, not the fix.
     */
    @Test
    void refusesAnOptionOutrightBeforeReportingTheEndpointShape() {
        Map<String, String> changeStream = changeStreamOptions("full");
        changeStream.put("scan.index", "idx");
        changeStream.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> source(SCHEMA, changeStream))
                .as("a bounded-only option on a change-stream source")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("scan.index is incompatible with scan.mode=change-stream")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> written = options();
        written.put("scan.change-stream.name", "people_changes");
        written.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> sink(SCHEMA, written))
                .as("a change-stream option on a sink")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("is incompatible with a sink")
                .hasMessageNotContaining("must be host:port");
    }

    /**
     * A change-stream DDL declares the watched table's own columns, so nothing in the sink's schema
     * would object. Both changelog modes are driven because upsert additionally declares a primary
     * key, which makes it the shape an upsert sink is happiest with.
     *
     * <p>The bare case matters most: {@code changeStreamOptions} always adds {@code
     * scan.change-stream.name}, so without it a check keying off that key instead of the mode would
     * pass the other two.
     */
    @Test
    void rejectsAChangeStreamTableAsASink() {
        Map<String, String> bare = options();
        bare.put("scan.mode", "change-stream");
        assertThatThrownBy(() -> sink(SCHEMA, bare))
                .as("scan.mode alone")
                .hasStackTraceContaining(
                        "scan.mode=change-stream selects a source-only table and cannot be written"
                                + " to");

        assertThatThrownBy(() -> sink(SCHEMA, changeStreamOptions("full")))
                .as("full")
                .hasStackTraceContaining(
                        "scan.mode=change-stream selects a source-only table and cannot be written"
                                + " to");

        assertThatThrownBy(() -> sink(withPrimaryKey(), changeStreamOptions("upsert")))
                .as("upsert")
                .hasStackTraceContaining(
                        "scan.mode=change-stream selects a source-only table and cannot be written"
                                + " to");
    }

    @Test
    void rejectsChangeStreamOptionsOnASinkInsteadOfIgnoringThem() {
        for (Map.Entry<String, String> entry : CHANGE_STREAM_OWNED_OPTIONS.entrySet()) {
            Map<String, String> options = options();
            options.put(entry.getKey(), entry.getValue());
            assertThatThrownBy(() -> sink(SCHEMA, options))
                    .as(entry.getKey())
                    .hasStackTraceContaining(entry.getKey() + " is incompatible with a sink");
        }
    }

    /**
     * The boundary the source-only rule does not cross: one table is legitimately scanned, looked
     * up and written, so the bounded-scan and lookup options a sink cannot act on stay accepted
     * rather than being swept up with the change-stream ones.
     */
    @Test
    void aSinkStillAcceptsTheScanAndLookupOptionsOfATableThatIsAlsoRead() {
        Map<String, String> options = options();
        options.put("scan.index", "by_name");
        options.put("scan.partition.max-partitions", "12");
        options.put("scan.data-boost-enabled", "true");
        options.put("scan.timestamp-bound.exact-staleness", "15 s");
        options.put("lookup.async", "true");
        options.put("lookup.cache", "PARTIAL");
        options.put("lookup.partial-cache.max-rows", "100");

        SpannerMutationsSink<?> sink = built(SCHEMA, options);

        // Accepted, and left on the read side: none of the above reaches the write path.
        assertThat(sink.getConfig().getDatabase())
                .isEqualTo(DatabaseDestination.of("my-project", "my-instance", "my-database"));
        assertThat(sink.getConfig().getWriterOptions().getMaxBatchCells()).isEqualTo(5_000);
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
                .isEqualTo(DatabaseDestination.of("my-project", "my-instance", "my-database"));
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
        for (Map.Entry<String, String> entry : CHANGE_STREAM_OWNED_OPTIONS.entrySet()) {
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
                        Map.entry("scan.partition.size-bytes", "2 mb"),
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
        options.put("scan.partition.size-bytes", "2 mb");
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

    @Test
    void namesTheOptionKeyWhenASinkValueIsRejected() {
        // 'sink.buffer-flush.max-cells' maps onto maxBatchCells — a rename, so the builder's own
        // message is unreadable from a WITH clause without the option key in front of it.
        Map<String, String> options = options();
        options.put("sink.buffer-flush.max-cells", "0");

        assertThatThrownBy(() -> sink(SCHEMA, options))
                .hasStackTraceContaining("Option 'sink.buffer-flush.max-cells' is invalid")
                .hasStackTraceContaining("maxBatchCells must be positive");

        Map<String, String> recovery = options();
        recovery.put("sink.recovery.max-attempts", "0");
        assertThatThrownBy(() -> sink(SCHEMA, recovery))
                .hasStackTraceContaining("Option 'sink.recovery.max-attempts' is invalid")
                .hasStackTraceContaining("recoveryMaxAttempts must be positive");
    }

    @Test
    void namesTheOptionKeyWhenAScanValueIsRejected() {
        Map<String, String> options = options();
        options.put("scan.partition.max-partitions", "0");

        assertThatThrownBy(() -> builtSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'scan.partition.max-partitions' is invalid")
                .hasMessageContaining("maxPartitions must be positive");

        Map<String, String> partitionSize = options();
        partitionSize.put("scan.partition.size-bytes", "0 b");
        assertThatThrownBy(() -> builtSource(SCHEMA, partitionSize))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'scan.partition.size-bytes' is invalid")
                .hasMessageContaining("partitionSizeBytes must be positive");
    }

    @Test
    void namesTheOptionKeyWhenAChangeStreamValueIsRejected() {
        Map<String, String> options = changeStreamOptions("full");
        options.put("scan.max-concurrent-queries-per-subtask", "0");

        assertThatThrownBy(
                        () ->
                                ((ScanTableSource) source(SCHEMA, options))
                                        .getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'scan.max-concurrent-queries-per-subtask' is invalid")
                .hasMessageContaining("maxConcurrentQueriesPerSubtask must be positive");
    }
}
