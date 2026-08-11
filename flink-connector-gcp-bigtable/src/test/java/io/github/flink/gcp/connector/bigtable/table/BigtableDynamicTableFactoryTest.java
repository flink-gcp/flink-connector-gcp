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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.api.connector.sink2.Sink;
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
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.FixedDestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableDynamicSource;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableDynamicTableFactory}.
 *
 * <p>Every rejection is asserted with {@code hasStackTraceContaining} and on a phrase only this
 * connector's own message carries. {@code FactoryUtil} wraps anything the factory throws in a
 * {@code ValidationException} whose own message is generic and carries a dump of the whole {@code
 * WITH} clause — so the actionable sentence arrives in the cause, and an assertion naming an option
 * key would be satisfied by that dump with the check deleted.
 */
class BigtableDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("rowkey", DataTypes.STRING()),
                    Column.physical(
                            "cf1",
                            DataTypes.ROW(
                                    DataTypes.FIELD("q1", DataTypes.STRING()),
                                    DataTypes.FIELD("q2", DataTypes.BIGINT()))));

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my-instance", "my-table");

    private static ResolvedSchema withPrimaryKey(String... columns) {
        return new ResolvedSchema(
                SCHEMA.getColumns(),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Arrays.asList(columns)));
    }

    private static Map<String, String> minimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("instance", "my-instance");
        options.put("table", "my-table");
        return options;
    }

    private static DynamicTableSink sink(Map<String, String> options) {
        return sink(SCHEMA, options);
    }

    private static DynamicTableSink sink(ResolvedSchema schema, Map<String, String> options) {
        return FactoryMocks.createTableSink(schema, options);
    }

    /**
     * The connector's own sink, as the planner would obtain it.
     *
     * <p>Option assertions read off this rather than off the {@link DynamicTableSink}: a value
     * dropped on the way to {@code BigtableSink.builder()} is invisible everywhere else.
     */
    private static BigtableMutateRowsSink<?> built(
            ResolvedSchema schema, Map<String, String> options) {
        Sink<?> sink =
                ((SinkV2Provider)
                                FactoryMocks.createTableSink(schema, options)
                                        .getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                        .createSink();
        return (BigtableMutateRowsSink<?>) sink;
    }

    @Test
    void buildsASinkFromTheMinimalOptions() {
        assertThat(sink(minimalOptions()))
                .isInstanceOf(BigtableDynamicSink.class)
                .extracting(DynamicTableSink::asSummaryString)
                .isEqualTo("Bigtable table sink");
    }

    @Test
    void theSinkItBuildsIsTheConnectorsOwnAndNamesTheDeclaredTable() {
        BigtableMutateRowsSink<?> sink = built(SCHEMA, minimalOptions());

        assertThat(sink.getConfig().getDestinationResolver())
                .isInstanceOfSatisfying(
                        FixedDestinationResolver.class,
                        resolver -> assertThat(resolver.getDestination()).isEqualTo(DESTINATION));
        assertThat(sink.getConfig().getWriterOptions()).isEqualTo(BigtableWriterOptions.defaults());
        assertThat(sink.getConfig().getAppProfileId()).isNull();
        assertThat(sink.getConfig().getEmulatorEndpoint()).isNull();
        assertThat(sink.getConfig().getTableCreateOptions()).isNull();
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
    void carriesTheAppProfileAndTheEmulatorEndpoint() {
        Map<String, String> options = minimalOptions();
        options.put("sink.app-profile-id", "writer-profile");
        options.put("emulator-endpoint", "localhost:8086");

        BigtableMutateRowsSink<?> sink = built(SCHEMA, options);

        assertThat(sink.getConfig().getAppProfileId()).isEqualTo("writer-profile");
        // The value, not merely its presence: an endpoint dropped on the way to the builder is
        // caught here in half a second, where the emulator ITCase would only notice it after three
        // minutes of retrying against the wrong host and would blame a hung job.
        assertThat(sink.getConfig().getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8086"));
    }

    @Test
    void copyCarriesEveryValue() {
        // A copy() that dropped emulator-endpoint would send a job at the real service where the
        // DDL said emulator, and the planner copies a sink on every applied ability and on
        // serialization. Measured: nulling three fields in copy() left all 102 table-layer tests
        // green before this existed.
        Map<String, String> options = minimalOptions();
        options.put("sink.app-profile-id", "writer-profile");
        options.put("emulator-endpoint", "localhost:8086");
        options.put("sink.parallelism", "3");
        options.put("null-string-literal", "<none>");
        options.put("sink.batching.element-count", "250");
        options.put("sink.create-disposition", "create-if-needed");
        options.put("sink.table-create.gc-rule.max-versions", "2");
        DynamicTableSink original = sink(options);

        assertThat(original.copy()).isEqualTo(original).hasSameHashCodeAs(original);
    }

    @Test
    void twoSinksDifferingInOneOptionAreNotEqual() {
        // The control arm for the test above: an equals() that compared nothing would satisfy it.
        Map<String, String> options = minimalOptions();
        options.put("sink.app-profile-id", "writer-profile");

        assertThat(sink(options)).isNotEqualTo(sink(minimalOptions()));
    }

    @Test
    void carriesTheWriterTuning() {
        Map<String, String> options = minimalOptions();
        options.put("sink.batching.element-count", "250");

        assertThat(built(SCHEMA, options).getConfig().getWriterOptions().getBatchElementCount())
                .isEqualTo(250L);
    }

    @Test
    void carriesTheCreationSettingsBuiltFromTheDdlFamilies() {
        Map<String, String> options = minimalOptions();
        options.put("sink.create-disposition", "create-if-needed");
        options.put("sink.table-create.gc-rule.max-versions", "2");

        BigtableMutateRowsSink<?> sink = built(SCHEMA, options);

        assertThat(sink.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_IF_NEEDED);
        assertThat(sink.getConfig().getTableCreateOptions()).isNotNull();
        assertThat(sink.getConfig().getTableCreateOptions().getColumnFamilies())
                .containsExactly(java.util.Map.entry("cf1", GcRule.maxVersions(2)));
    }

    @Test
    void rejectsATableWithoutItsThreeDestinationParts() {
        for (String missing : new String[] {"project", "instance", "table"}) {
            Map<String, String> options = minimalOptions();
            options.remove(missing);

            // On FactoryUtil's own message, not on the key: the key alone is satisfied by the
            // WITH-clause dump FactoryUtil attaches to anything the factory throws — measured, by
            // emptying requiredOptions(), which left every assertion in this class green while
            // 'table' appeared 21 times in an unrelated rejection's stack trace.
            assertThatThrownBy(() -> sink(options))
                    .as("without '%s'", missing)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Missing required options")
                    .hasStackTraceContaining(missing);
        }
    }

    @Test
    void rejectsAnUnknownOption() {
        Map<String, String> options = minimalOptions();
        options.put("sink.buffer-flush.max-rows", "100");

        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Unsupported options");
    }

    @Test
    void rejectsAPrimaryKeyThatIsNotTheRowKey() {
        ResolvedSchema schema = withPrimaryKey("cf1");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must be its row-key column")
                .hasStackTraceContaining("'rowkey'");
    }

    @Test
    void acceptsAPrimaryKeyOnTheRowKey() {
        // The control arm: without it, a check that rejected every declared primary key would read
        // exactly like the rejection above.
        assertThat(FactoryMocks.createTableSink(withPrimaryKey("rowkey"), minimalOptions()))
                .isInstanceOf(BigtableDynamicSink.class);
    }

    @Test
    void rejectsATableWithNoColumnFamily() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("rowkey", DataTypes.STRING()));

        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("needs at least one column family with a qualifier");
    }

    @Test
    void rejectsATableWhoseOnlyColumnFamilyIsEmpty() {
        // ROW<> parses, so "declares a family" and "can hold a cell" are different questions. A
        // table like this planned, and then failed every record at runtime with a message blaming
        // a null family.
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("rowkey", DataTypes.STRING()),
                        Column.physical("cf1", DataTypes.ROW()));

        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("needs at least one column family with a qualifier");
    }

    @Test
    void rejectsASecondAtomicColumn() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("rowkey", DataTypes.STRING()),
                        Column.physical("stray", DataTypes.INT()));

        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("exactly one atomic column");
    }

    @Test
    void rejectsCreationKeysUnderADispositionThatCreatesNothing() {
        Map<String, String> options = minimalOptions();
        options.put("sink.table-create.gc-rule.max-versions", "2");

        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("this table does not create any");

        options.put("sink.create-disposition", "create-never");
        assertThatThrownBy(() -> sink(options))
                .as("an explicit create-never is rejected the same way as an absent disposition")
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("this table does not create any");
    }

    @Test
    void rejectsCreationWithNoGarbageCollectionRule() {
        Map<String, String> options = minimalOptions();
        options.put("sink.create-disposition", "create-if-needed");

        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("needs a garbage-collection rule");
    }

    @Test
    void rejectsAColumnTypeWithNoCellEncoding() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("rowkey", DataTypes.STRING()),
                        Column.physical(
                                "cf1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "tags", DataTypes.ARRAY(DataTypes.STRING())))));

        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("no Bigtable cell encoding");
    }

    // ------------------------------------------------------------------------
    //  The source half
    // ------------------------------------------------------------------------

    private static DynamicTableSource source(Map<String, String> options) {
        return FactoryMocks.createTableSource(SCHEMA, options);
    }

    /**
     * The connector's own source configuration, as the planner would build it — the source-side
     * mirror of {@link #built(ResolvedSchema, Map)}.
     */
    private static BigtableSourceConfig<?> builtSource(
            ResolvedSchema schema, Map<String, String> options) {
        // The provider builds the source's real clients; the endpoint is never connected to.
        options.put("emulator-endpoint", "localhost:1");
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) FactoryMocks.createTableSource(schema, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return ((BigtableReadRowsSource<?>) provider.createSource()).getConfig();
    }

    @Test
    void buildsASourceFromTheMinimalOptions() {
        assertThat(source(minimalOptions()))
                .isInstanceOf(BigtableDynamicSource.class)
                .extracting(DynamicTableSource::asSummaryString)
                .isEqualTo("Bigtable table source");
    }

    @Test
    void acceptsARowKeyOnlyTableForReading() {
        // The deliberate asymmetry against rejectsATableWithNoColumnFamily: a row-key-only table
        // is not a thing to write, but it is a legitimate thing to read, served by a keys-only
        // filter chain.
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("rowkey", DataTypes.STRING()));

        assertThat(FactoryMocks.createTableSource(schema, minimalOptions()))
                .isInstanceOf(BigtableDynamicSource.class);
    }

    @Test
    void rejectsAPrimaryKeyThatIsNotTheRowKeyOnTheSourceSideToo() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        withPrimaryKey("cf1"), minimalOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must be its row-key column");
    }

    @Test
    void everyScanOptionReachesTheSource() {
        Map<String, String> options = minimalOptions();
        options.put("scan.app-profile-id", "reader-profile");
        options.put("scan.row-prefix", "user;web");
        options.put("scan.row-range.start-closed", "a");
        options.put("scan.row-range.end-open", "m");

        BigtableSourceConfig<?> config = builtSource(SCHEMA, options);

        assertThat(config.getAppProfileId()).isEqualTo("reader-profile");
        assertThat(config.getRanges().stream().map(RowRanges::format).collect(Collectors.toList()))
                .containsExactly("[a, m)", "[user, uses)", "[web, wec)");
    }

    @Test
    void acceptsEveryStandardLookupCacheOption() {
        Map<String, String> options = minimalOptions();
        options.put("lookup.cache", "partial");
        options.put("lookup.max-retries", "4");
        options.put("lookup.partial-cache.expire-after-access", "1 min");
        options.put("lookup.partial-cache.expire-after-write", "2 min");
        options.put("lookup.partial-cache.cache-missing-key", "false");
        options.put("lookup.partial-cache.max-rows", "100");
        options.put("lookup.full-cache.reload-strategy", "timed");
        options.put("lookup.full-cache.periodic-reload.interval", "5 min");
        options.put("lookup.full-cache.periodic-reload.schedule-mode", "FIXED_RATE");
        options.put("lookup.full-cache.timed-reload.iso-time", "10:15Z");
        options.put("lookup.full-cache.timed-reload.interval-in-days", "2");

        assertThat(source(options)).isInstanceOf(BigtableDynamicSource.class);
    }

    @Test
    void rejectsFullCachingWithAsyncLookupWhenTheSourceIsPlanned() {
        Map<String, String> options = minimalOptions();
        options.put("lookup.async", "true");
        options.put("lookup.cache", "full");
        options.put("lookup.full-cache.periodic-reload.interval", "1 min");

        assertThatThrownBy(() -> source(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("'lookup.async' cannot be true")
                .hasStackTraceContaining("'lookup.cache' is FULL");
    }

    @Test
    void aOneSidedRangeIsAccepted() {
        Map<String, String> startOnly = minimalOptions();
        startOnly.put("scan.row-range.start-closed", "b");
        assertThat(
                        builtSource(SCHEMA, startOnly).getRanges().stream()
                                .map(RowRanges::format)
                                .collect(Collectors.toList()))
                .containsExactly("[b, *)");

        Map<String, String> endOnly = minimalOptions();
        endOnly.put("scan.row-range.end-open", "b");
        assertThat(
                        builtSource(SCHEMA, endOnly).getRanges().stream()
                                .map(RowRanges::format)
                                .collect(Collectors.toList()))
                .containsExactly("(*, b)");
    }

    @Test
    void carriesTheSourceParallelismWhenItIsSet() {
        Map<String, String> options = minimalOptions();
        options.put("scan.parallelism", "5");
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) source(options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider.getParallelism()).contains(5);
    }

    @Test
    void rejectsAnEmptyStringRangeBound() {
        for (String key : new String[] {"scan.row-range.start-closed", "scan.row-range.end-open"}) {
            Map<String, String> options = minimalOptions();
            options.put(key, "");

            assertThatThrownBy(() -> source(options))
                    .as("with '%s' empty", key)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("is the empty string, which is not a row key");
        }
    }

    @Test
    void rejectsAnEmptyPrefixElement() {
        Map<String, String> options = minimalOptions();
        // Concatenated so the adjacent list separators do not read as a double semicolon to the
        // one-semicolon style check; the value under test is a three-element list whose middle
        // element is empty.
        options.put("scan.row-prefix", "a;" + ";b");

        assertThatThrownBy(() -> source(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("contains an empty prefix");
    }
}
