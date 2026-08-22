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

package io.github.flink.gcp.connector.bigtable.table;

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
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.FixedDestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSource;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamDynamicSource;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamEnvelopeSchema;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableDynamicSource;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

    /**
     * Every option the Change Streams mode owns, one legal value each. Bounded mode rejects all of
     * them, and both directions are held to that: a sink can act on a Change Streams option no more
     * than a bounded source can. Adding one to {@code changeStreamSourceOptions()} without adding
     * it here leaves it untested rather than failing.
     */
    private static final List<Map.Entry<String, String>> CHANGE_STREAM_OWNED_OPTIONS =
            Arrays.asList(
                    Map.entry("scan.change-stream.changelog-mode", "envelope"),
                    Map.entry("scan.startup.mode", "latest"),
                    Map.entry("scan.startup.timestamp-millis", "1000"),
                    Map.entry("scan.resume-fallback.mode", "earliest"),
                    Map.entry("scan.resume-fallback.timestamp-millis", "1000"),
                    Map.entry("scan.end-timestamp-millis", "2000"),
                    Map.entry("scan.max-concurrent-streams-per-subtask", "1"),
                    Map.entry("scan.change-stream.selected-cell.family", "state"),
                    Map.entry("scan.change-stream.selected-cell.qualifier-base64", "cQ=="),
                    Map.entry("scan.change-stream.selected-cell.source-cluster-id", "cluster-1"),
                    Map.entry("value.format", "json"));

    private static final DataType CHANGE_STREAM_GENERIC_VALUE =
            DataTypes.ROW(
                    DataTypes.FIELD("value_type", DataTypes.STRING()),
                    DataTypes.FIELD("bytes_value", DataTypes.BYTES()),
                    DataTypes.FIELD("long_value", DataTypes.BIGINT()));

    private static final ResolvedSchema CHANGE_STREAM_SCHEMA =
            changeStreamSchema(CHANGE_STREAM_GENERIC_VALUE);

    private static ResolvedSchema changeStreamSchema(DataType qualifierType) {
        return ResolvedSchema.of(
                Column.physical("row_key", DataTypes.BYTES()),
                Column.physical(
                        "entries",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("entry_index", DataTypes.INT()),
                                        DataTypes.FIELD("kind", DataTypes.STRING()),
                                        DataTypes.FIELD("family", DataTypes.STRING()),
                                        DataTypes.FIELD("qualifier", qualifierType),
                                        DataTypes.FIELD("timestamp", CHANGE_STREAM_GENERIC_VALUE),
                                        DataTypes.FIELD("value", CHANGE_STREAM_GENERIC_VALUE),
                                        DataTypes.FIELD(
                                                "delete_range",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "start_bound", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "start_micros", DataTypes.BIGINT()),
                                                        DataTypes.FIELD(
                                                                "end_bound", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "end_micros",
                                                                DataTypes.BIGINT())))))));
    }

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
        return built(FactoryMocks.createTableSink(schema, options));
    }

    @SuppressWarnings("unchecked")
    private static BigtableMutateRowsSink<RowData> built(DynamicTableSink tableSink) {
        Sink<?> sink =
                ((SinkV2Provider)
                                tableSink.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        return (BigtableMutateRowsSink<RowData>) sink;
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
        assertThat(sink.getConfig().getServiceAccountKeyFile()).isNull();
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
                .isEqualTo(EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint"));
    }

    @Test
    void carriesTheServiceAccountKeyFileToTheSinkRuntime() {
        Map<String, String> options = minimalOptions();
        options.put("service-account-key-file", "/var/run/secrets/bigtable.json");

        assertThat(built(SCHEMA, options).getConfig().getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/bigtable.json");
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
        options.put("sink.cell-timestamp.truncate-to-millis", "true");
        DynamicTableSink original = sink(options);

        assertThat(original.copy()).isEqualTo(original).hasSameHashCodeAs(original);
    }

    @Test
    void cellTimestampTruncationOptionReachesTheSink() {
        Map<String, String> truncatingOptions = minimalOptions();
        truncatingOptions.put("sink.cell-timestamp.truncate-to-millis", "true");

        assertThat(sink(truncatingOptions)).isNotEqualTo(sink(minimalOptions()));
    }

    @Test
    void theDefaultTimestampOptionPreservesMicrosecondsInTheRuntimeSerializer() throws Exception {
        SupportsWritingMetadata metadataSink = (SupportsWritingMetadata) sink(minimalOptions());
        metadataSink.applyWritableMetadata(
                Collections.singletonList("timestamp"),
                DataTypes.ROW(
                        DataTypes.FIELD("rowkey", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "cf1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("q1", DataTypes.STRING()),
                                        DataTypes.FIELD("q2", DataTypes.BIGINT()))),
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(6))));
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("r1"),
                        GenericRowData.of(StringData.fromString("v"), 7L),
                        TimestampData.fromEpochMillis(1_700L, 123_456));

        BigtableMutateRowsSink<RowData> runtimeSink = built((DynamicTableSink) metadataSink);

        assertThat(
                        runtimeSink
                                .getConfig()
                                .getSerializer()
                                .serialize(row, null)
                                .toProto()
                                .getMutationsList())
                .extracting(mutation -> mutation.getSetCell().getTimestampMicros())
                .containsOnly(1_700_123L);
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

    /**
     * The full sentence is asserted, not the option key: {@code FactoryUtil} wraps a factory's
     * failure with a dump of the whole {@code WITH} clause, so the key alone appears in the message
     * whether or not the check fired.
     *
     * <p>The empty string is carried beside the whitespace one because it is the arm whose reach
     * depends on a Flink semantic rather than on this code: an option written {@code ''} in a
     * {@code WITH} clause arrives as a present, empty value rather than as an absent one, so {@code
     * optionalNonBlank} sees it. If that ever changed, this is what would notice.
     */
    @Test
    void rejectsABlankAppProfileIdOnEveryPathThatReadsIt() {
        for (String blank : new String[] {"", "  "}) {
            Map<String, String> blankScan = minimalOptions();
            blankScan.put("scan.app-profile-id", blank);
            assertThatThrownBy(() -> source(blankScan))
                    .as("bounded scan, %d characters", blank.length())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Option 'scan.app-profile-id' must not be blank.");

            Map<String, String> blankSink = minimalOptions();
            blankSink.put("sink.app-profile-id", blank);
            assertThatThrownBy(() -> sink(blankSink))
                    .as("sink, %d characters", blank.length())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Option 'sink.app-profile-id' must not be blank.");

            Map<String, String> blankChangeStream = minimalChangeStreamOptions();
            blankChangeStream.put("scan.app-profile-id", blank);
            assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, blankChangeStream))
                    .as("change stream, %d characters", blank.length())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Option 'scan.app-profile-id' must not be blank.");
        }
    }

    @Test
    void rejectsBlankOrEmulatorCombinedCredentialOptionsForBothDirections() {
        for (java.util.function.Function<Map<String, String>, ?> direction :
                Arrays.<java.util.function.Function<Map<String, String>, ?>>asList(
                        BigtableDynamicTableFactoryTest::sink,
                        BigtableDynamicTableFactoryTest::source)) {
            Map<String, String> blank = minimalOptions();
            blank.put("service-account-key-file", "  ");
            assertThatThrownBy(() -> direction.apply(blank))
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("must not be blank");

            Map<String, String> emulator = minimalOptions();
            emulator.put("service-account-key-file", "key.json");
            emulator.put("emulator-endpoint", "localhost:8086");
            assertThatThrownBy(() -> direction.apply(emulator))
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("cannot be combined");
        }
    }

    /**
     * The source arm is the lookup arm: {@code createDynamicTableSource} is the only entry point
     * for a lookup table, because {@code BigtableDynamicSource} serves both {@code ScanTableSource}
     * and {@code LookupTableSource}. So this covers the case issue #1009 is about — a table joined
     * as a lookup dimension, which used to carry the value to a TaskManager and parse it there —
     * and a separate assertion through {@code getLookupRuntimeProvider} would repeat this one
     * rather than reach further.
     *
     * <p>The full sentence is asserted rather than the option key, for the reason recorded on
     * {@link #rejectsABlankAppProfileIdOnEveryPathThatReadsIt()}: {@code FactoryUtil} dumps the
     * whole {@code WITH} clause into the message, so {@code emulator-endpoint} appears in it
     * whether or not the check fired.
     *
     * <p>Two values, not a catalogue. {@code "localhost"} exercises the shape, and {@code ""} the
     * one thing that is this layer's rather than the parser's: whether an option written {@code ''}
     * arrives as present-and-empty rather than absent, so the check sees it at all. The rejection
     * set itself belongs to {@code EmulatorEndpointTest}, and re-testing it here would redden this
     * class for a deliberate change to the parser.
     */
    @Test
    void rejectsAMalformedEmulatorEndpointForBothDirections() {
        Map<String, java.util.function.Function<Map<String, String>, ?>> directions =
                new java.util.LinkedHashMap<>();
        directions.put("sink", BigtableDynamicTableFactoryTest::sink);
        directions.put("bounded scan and lookup", BigtableDynamicTableFactoryTest::source);
        for (String malformed : new String[] {"localhost", ""}) {
            directions.forEach(
                    (name, direction) -> {
                        Map<String, String> options = minimalOptions();
                        options.put("emulator-endpoint", malformed);
                        assertThatThrownBy(() -> direction.apply(options))
                                .as("%s, '%s'", name, malformed)
                                .isInstanceOf(ValidationException.class)
                                .hasStackTraceContaining(
                                        "emulator-endpoint must be host:port, was '"
                                                + malformed
                                                + "'");
                    });
        }
    }

    /**
     * Pins the endpoint parse behind every check that refuses an option outright, in both
     * directions. A DDL told to remove an option is not helped by an answer about its shape, and
     * moving {@code validateEmulatorEndpoint} above either check is what this fails on.
     *
     * <p>Both arms assert on the root cause rather than the stack trace, and on a phrase rather
     * than the whole sentence: those messages join every offending key they found, so pinning them
     * verbatim would redden this test when an unrelated option joins {@code boundedSourceOptions()}
     * or {@code changeStreamSourceOptions()}. The negative is the half that discriminates — with
     * the parse moved first the root cause becomes the {@code IllegalArgumentException}, whose
     * message these phrases do not appear in.
     *
     * <p>Green on {@code origin/main} by construction, since there the parse does not run at
     * planning at all. It guards the ordering, not the fix; {@link
     * #rejectsAMalformedEmulatorEndpointForBothDirections()} guards the fix.
     */
    @Test
    void refusesAnOptionOutrightBeforeReportingTheEndpointShape() {
        Map<String, String> changeStreamSource = minimalChangeStreamOptions();
        changeStreamSource.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, changeStreamSource))
                .as("a bounded-only option on a change-stream source")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("are not valid when 'scan.mode' = 'change-stream'")
                .hasMessageNotContaining("must be host:port");

        // scan.mode is left at its bounded default, so checkNotAChangeStreamTable does not fire and
        // checkSinkHasNoChangeStreamOptions is the check that has to be reached.
        Map<String, String> sinkWithChangeStreamOption = minimalOptions();
        sinkWithChangeStreamOption.put("scan.change-stream.changelog-mode", "envelope");
        sinkWithChangeStreamOption.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> sink(sinkWithChangeStreamOption))
                .as("a change-stream option on a sink")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("are not valid on a 'bigtable' table that is written to")
                .hasMessageNotContaining("must be host:port");
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

    /**
     * Every case carries the ordinary sink schema, not a Change Streams one. That is the point: the
     * published Change Streams shapes are refused by {@code BigtableTableSchema.of} anyway, so only
     * a DDL a sink could otherwise serve shows that the scan mode itself is what refuses it.
     *
     * <p>The bare case matters most. Both option helpers add a second Change Streams key, so
     * without it a check keying off {@code scan.change-stream.changelog-mode} instead of the mode
     * would pass every other case here.
     */
    @Test
    void rejectsAChangeStreamTableAsASink() {
        Map<String, String> bare = minimalOptions();
        bare.put("scan.mode", "change-stream");
        assertThatThrownBy(() -> sink(bare))
                .as("scan.mode alone")
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("is source-only and cannot be written to");

        assertThatThrownBy(() -> sink(minimalChangeStreamOptions()))
                .as("envelope mode")
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("is source-only and cannot be written to");

        assertThatThrownBy(() -> sink(minimalSelectedCellOptions()))
                .as("selected-cell mode")
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("is source-only and cannot be written to");
    }

    /**
     * The scan mode is read before {@code helper.validate()}, so the reason the table cannot be
     * written to arrives ahead of the format sub-keys no sink discovers a format for. With the two
     * reversed this fails on {@code Unsupported options} instead.
     */
    @Test
    void theSourceOnlyRejectionOutrunsTheUnconsumedFormatKeys() {
        Map<String, String> options = minimalSelectedCellOptions();
        options.put("value.json.ignore-parse-errors", "true");

        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("is source-only and cannot be written to");
    }

    /**
     * The needle names the offending key <em>inside</em> this connector's own sentence. The key on
     * its own would be satisfied by {@code FactoryUtil}'s dump of the whole {@code WITH} clause,
     * and the sentence on its own would not catch a message that named the wrong key.
     */
    @Test
    void rejectsChangeStreamOptionsOnASinkInsteadOfIgnoringThem() {
        for (Map.Entry<String, String> incompatible : CHANGE_STREAM_OWNED_OPTIONS) {
            Map<String, String> options = minimalOptions();
            options.put(incompatible.getKey(), incompatible.getValue());
            assertThatThrownBy(() -> sink(options))
                    .as("with incompatible option '%s'", incompatible.getKey())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(
                            "Options '"
                                    + incompatible.getKey()
                                    + "' are not valid on a 'bigtable' table that is written to");
        }
    }

    /**
     * The boundary the source-only rule deliberately does not cross. One table legitimately scans
     * and writes — under different application profiles, because a Data Boost profile reads and
     * cannot write — so the scan and lookup options a sink cannot act on stay accepted rather than
     * being swept up with the Change Streams ones.
     */
    @Test
    void aSinkStillAcceptsTheScanAndLookupOptionsOfATableThatIsAlsoRead() {
        Map<String, String> options = minimalOptions();
        options.put("scan.app-profile-id", "reader-profile");
        options.put("sink.app-profile-id", "writer-profile");
        options.put("scan.row-key-encoding", "UTF8");
        options.put("scan.row-prefix", "user;web");
        options.put("scan.row-range.start-closed", "a");
        options.put("scan.row-range.end-open", "m");
        options.put("scan.row-ranges", "[q,s)");
        options.put("lookup.async", "true");
        options.put("lookup.cache", "PARTIAL");
        options.put("lookup.partial-cache.max-rows", "100");

        assertThat(built(SCHEMA, options).getConfig().getAppProfileId())
                .isEqualTo("writer-profile");
    }

    // ------------------------------------------------------------------------
    //  The source half
    // ------------------------------------------------------------------------

    private static DynamicTableSource source(Map<String, String> options) {
        return FactoryMocks.createTableSource(SCHEMA, options);
    }

    private static DynamicTableSource source(ResolvedSchema schema, Map<String, String> options) {
        return FactoryMocks.createTableSource(schema, options);
    }

    private static Map<String, String> minimalChangeStreamOptions() {
        Map<String, String> options = minimalOptions();
        options.put("scan.mode", "change-stream");
        options.put("scan.change-stream.changelog-mode", "envelope");
        options.put("scan.app-profile-id", "single-cluster-profile");
        return options;
    }

    /**
     * A builder carrying what every envelope expectation here shares, so each test names only what
     * its options are supposed to change.
     */
    private static BigtableChangeStreamDynamicSource.Builder expectedEnvelopeSource() {
        return BigtableChangeStreamDynamicSource.builder()
                .destination(DESTINATION)
                .appProfileId("single-cluster-profile")
                .physicalDataType(BigtableChangeStreamEnvelopeSchema.DATA_TYPE.notNull());
    }

    private static Map<String, String> minimalSelectedCellOptions() {
        Map<String, String> options = minimalChangeStreamOptions();
        options.put("scan.change-stream.changelog-mode", "selected-cell");
        options.put("scan.change-stream.selected-cell.family", "state");
        options.put("scan.change-stream.selected-cell.qualifier-base64", "");
        options.put("scan.change-stream.selected-cell.source-cluster-id", "cluster-1");
        options.put("value.format", "json");
        return options;
    }

    private static ResolvedSchema selectedCellSchema(String... primaryKeyColumns) {
        java.util.List<Column> columns =
                Arrays.asList(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("row_id", DataTypes.STRING().notNull()),
                        Column.physical("score", DataTypes.INT()));
        return new ResolvedSchema(
                columns,
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Arrays.asList(primaryKeyColumns)));
    }

    /**
     * The connector's own source configuration, as the planner would build it — the source-side
     * mirror of {@link #built(ResolvedSchema, Map)}.
     */
    private static BigtableSourceConfig<?> builtSource(
            ResolvedSchema schema, Map<String, String> options) {
        // The endpoint is never connected to. A credential path is mutually exclusive with it.
        if (!options.containsKey("service-account-key-file")) {
            options.put("emulator-endpoint", "localhost:1");
        }
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
    void buildsTheGenericChangeStreamEnvelopeSource() {
        DynamicTableSource source = source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions());

        assertThat(source)
                .isInstanceOf(BigtableChangeStreamDynamicSource.class)
                .extracting(DynamicTableSource::asSummaryString)
                .isEqualTo("Bigtable Change Streams");
        assertThat(((ScanTableSource) source).getChangelogMode().getContainedKinds())
                .containsExactly(org.apache.flink.types.RowKind.INSERT);
        assertThat(source.copy()).isEqualTo(source).hasSameHashCodeAs(source);
    }

    @Test
    void buildsASelectedCellUpsertSourceWithANonLeadingPrimaryKey() {
        DynamicTableSource source =
                source(selectedCellSchema("row_id"), minimalSelectedCellOptions());

        assertThat(source).isInstanceOf(BigtableChangeStreamDynamicSource.class);
        assertThat(((ScanTableSource) source).getChangelogMode().getContainedKinds())
                .containsExactlyInAnyOrder(
                        org.apache.flink.types.RowKind.INSERT,
                        org.apache.flink.types.RowKind.UPDATE_AFTER,
                        org.apache.flink.types.RowKind.DELETE);
        assertThat(source.copy()).isEqualTo(source).hasSameHashCodeAs(source);

        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) source)
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider.createSource())
                .extracting("config.deserializer")
                .hasFieldOrPropertyWithValue("primaryKeyIndex", 1);
    }

    @Test
    void validatesTheSelectedCellSchemaAndRequiredProtocolOptions() {
        Map<String, String> options = minimalSelectedCellOptions();
        options.remove("scan.change-stream.selected-cell.family");
        assertThatThrownBy(() -> source(selectedCellSchema("row_id"), options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Option 'scan.change-stream.selected-cell.family' is required");

        ResolvedSchema noPrimaryKey =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("row_id", DataTypes.STRING()),
                        Column.physical("score", DataTypes.INT()));
        assertThatThrownBy(() -> source(noPrimaryKey, minimalSelectedCellOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires exactly one physical PRIMARY KEY column");
        assertThatThrownBy(
                        () ->
                                source(
                                        selectedCellSchema("row_id", "name"),
                                        minimalSelectedCellOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires exactly one physical PRIMARY KEY column");

        ResolvedSchema keyOnly =
                new ResolvedSchema(
                        Collections.singletonList(
                                Column.physical("row_id", DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("row_id")));
        assertThatThrownBy(() -> source(keyOnly, minimalSelectedCellOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("needs at least one non-key physical column");
    }

    @Test
    void validatesSelectedCellQualifierAndRejectsItsOptionsInEnvelopeMode() {
        Map<String, String> malformed = minimalSelectedCellOptions();
        malformed.put("scan.change-stream.selected-cell.qualifier-base64", "YQ");
        assertThatThrownBy(() -> source(selectedCellSchema("row_id"), malformed))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("canonical padded RFC 4648 standard Base64");

        for (Map.Entry<String, String> incompatible :
                Arrays.asList(
                        Map.entry("scan.change-stream.selected-cell.family", "state"),
                        Map.entry("scan.change-stream.selected-cell.qualifier-base64", "cQ=="),
                        Map.entry(
                                "scan.change-stream.selected-cell.source-cluster-id", "cluster-1"),
                        Map.entry("value.format", "json"))) {
            Map<String, String> envelope = minimalChangeStreamOptions();
            envelope.put(incompatible.getKey(), incompatible.getValue());
            assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, envelope))
                    .as("with selected-cell option '%s'", incompatible.getKey())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("valid only when")
                    .hasStackTraceContaining("selected-cell");
        }
    }

    @Test
    void rejectsAChangelogValueFormatInSelectedCellMode() {
        Map<String, String> options = minimalSelectedCellOptions();
        options.put("value.format", "debezium-json");

        assertThatThrownBy(() -> source(selectedCellSchema("row_id"), options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must be insert-only in Bigtable selected-cell mode");
    }

    @Test
    void appliesReadableMetadataIdempotentlyAndCarriesItThroughCopies() {
        BigtableChangeStreamDynamicSource source =
                (BigtableChangeStreamDynamicSource)
                        source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions());
        DataType firstProducedType =
                DataTypes.ROW(
                        DataTypes.FIELD("row_key", DataTypes.BYTES()),
                        DataTypes.FIELD(
                                "entries", CHANGE_STREAM_SCHEMA.getColumnDataTypes().get(1)),
                        DataTypes.FIELD("kind", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("committed_at", DataTypes.TIMESTAMP_LTZ(9).notNull()));

        SupportsReadingMetadata metadataSource = source;
        assertThat(metadataSource.listReadableMetadata())
                .containsOnlyKeys(
                        "mutation-type",
                        "source-cluster-id",
                        "commit-timestamp",
                        "tie-breaker",
                        "estimated-low-watermark");
        metadataSource.applyReadableMetadata(
                Arrays.asList("mutation-type", "commit-timestamp"), firstProducedType);

        assertThat(source.copy()).isEqualTo(source).hasSameHashCodeAs(source);
        assertThat(source).isNotEqualTo(source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions()));

        DataType secondProducedType =
                DataTypes.ROW(
                        DataTypes.FIELD("row_key", DataTypes.BYTES()),
                        DataTypes.FIELD(
                                "entries", CHANGE_STREAM_SCHEMA.getColumnDataTypes().get(1)),
                        DataTypes.FIELD("tie", DataTypes.INT().notNull()));
        metadataSource.applyReadableMetadata(
                Collections.singletonList("tie-breaker"), secondProducedType);

        BigtableChangeStreamDynamicSource same =
                (BigtableChangeStreamDynamicSource)
                        source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions());
        same.applyReadableMetadata(Collections.singletonList("tie-breaker"), secondProducedType);
        assertThat(source.copy()).isEqualTo(source).isEqualTo(same);
    }

    @Test
    void selectedMetadataKeysArePartOfTheSourceIdentity() {
        DataType producedType =
                DataTypes.ROW(
                        DataTypes.FIELD("row_key", DataTypes.BYTES()),
                        DataTypes.FIELD(
                                "entries", CHANGE_STREAM_SCHEMA.getColumnDataTypes().get(1)),
                        DataTypes.FIELD("metadata", DataTypes.STRING()));
        BigtableChangeStreamDynamicSource mutationType =
                (BigtableChangeStreamDynamicSource)
                        source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions());
        mutationType.applyReadableMetadata(
                Collections.singletonList("mutation-type"), producedType);
        BigtableChangeStreamDynamicSource sourceCluster =
                (BigtableChangeStreamDynamicSource)
                        source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions());
        sourceCluster.applyReadableMetadata(
                Collections.singletonList("source-cluster-id"), producedType);

        assertThat(mutationType).isNotEqualTo(sourceCluster);
    }

    @Test
    void selectedMetadataReachesTheRuntimeDeserializerInPlannerOrder() {
        BigtableChangeStreamDynamicSource source =
                (BigtableChangeStreamDynamicSource)
                        source(CHANGE_STREAM_SCHEMA, minimalChangeStreamOptions());
        DataType producedType =
                DataTypes.ROW(
                        DataTypes.FIELD("row_key", DataTypes.BYTES()),
                        DataTypes.FIELD(
                                "entries", CHANGE_STREAM_SCHEMA.getColumnDataTypes().get(1)),
                        DataTypes.FIELD("low_watermark", DataTypes.TIMESTAMP_LTZ(9).notNull()),
                        DataTypes.FIELD("mutation_type", DataTypes.STRING().notNull()));
        source.applyReadableMetadata(
                Arrays.asList("estimated-low-watermark", "mutation-type"), producedType);

        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider.createSource())
                .extracting("config.deserializer.metadata")
                .satisfies(
                        selected ->
                                assertThat((Object[]) selected)
                                        .extracting(Object::toString)
                                        .containsExactly(
                                                "ESTIMATED_LOW_WATERMARK", "MUTATION_TYPE"));
    }

    @Test
    void mapsEveryChangeStreamBuilderOptionAndLeavesLatestAsTheBuilderDefault() {
        Map<String, String> configured = minimalChangeStreamOptions();
        configured.put("service-account-key-file", "/var/run/secrets/bigtable.json");
        configured.put("scan.startup.mode", "timestamp");
        configured.put("scan.startup.timestamp-millis", "1000");
        configured.put("scan.resume-fallback.mode", "earliest");
        configured.put("scan.end-timestamp-millis", "2000");
        configured.put("scan.max-concurrent-streams-per-subtask", "5");
        configured.put("scan.parallelism", "3");

        DynamicTableSource actual = source(CHANGE_STREAM_SCHEMA, configured);
        BigtableChangeStreamDynamicSource expected =
                expectedEnvelopeSource()
                        .serviceAccountKeyFile("/var/run/secrets/bigtable.json")
                        .startPosition(StartPosition.at(Instant.ofEpochMilli(1000L)))
                        .resumeFallback(StartPosition.earliest())
                        .endTime(Instant.ofEpochMilli(2000L))
                        .maxConcurrentStreamsPerSubtask(5)
                        .parallelism(3)
                        .build();

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.copy()).isEqualTo(actual).hasSameHashCodeAs(actual);

        Map<String, String> defaults = minimalChangeStreamOptions();
        assertThat(source(CHANGE_STREAM_SCHEMA, defaults))
                .isEqualTo(expectedEnvelopeSource().build());
        assertThat(actual).isNotEqualTo(source(CHANGE_STREAM_SCHEMA, defaults));
    }

    @Test
    void mapsEveryLegalChangeStreamStartAndFallbackMode() {
        Map<String, String> latest = minimalChangeStreamOptions();
        latest.put("scan.startup.mode", "latest");
        assertThat(source(CHANGE_STREAM_SCHEMA, latest))
                .isEqualTo(expectedEnvelopeSource().startPosition(StartPosition.latest()).build());

        Map<String, String> timestampFallback = minimalChangeStreamOptions();
        timestampFallback.put("scan.resume-fallback.mode", "timestamp");
        timestampFallback.put("scan.resume-fallback.timestamp-millis", "3000");
        assertThat(source(CHANGE_STREAM_SCHEMA, timestampFallback))
                .isEqualTo(
                        expectedEnvelopeSource()
                                .resumeFallback(StartPosition.at(Instant.ofEpochMilli(3000L)))
                                .build());
    }

    @Test
    void rejectsKafkaOffsetNamesForChangeStreamPositions() {
        for (String mode : Arrays.asList("earliest-offset", "latest-offset")) {
            Map<String, String> options = minimalChangeStreamOptions();
            options.put("scan.startup.mode", mode);

            assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, options))
                    .as("with Kafka offset mode '%s'", mode)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("scan.startup.mode");
        }
    }

    @Test
    void buildsTheExistingDataStreamSourceWithBoundednessAndParallelism() {
        Map<String, String> options = minimalChangeStreamOptions();
        options.put("scan.end-timestamp-millis", "2000");
        options.put("scan.parallelism", "4");
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) source(CHANGE_STREAM_SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        Source<?, ?, ?> runtime = provider.createSource();
        assertThat(runtime).isInstanceOf(BigtableChangeStreamSource.class);
        assertThat(runtime.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(provider.getParallelism()).contains(4);
    }

    @Test
    void mapsEveryConfiguredValueAcrossTheDataStreamBuilderBoundary() {
        Map<String, String> options = minimalChangeStreamOptions();
        options.put("service-account-key-file", "/var/run/secrets/bigtable.json");
        options.put("scan.startup.mode", "timestamp");
        options.put("scan.startup.timestamp-millis", "1000");
        options.put("scan.resume-fallback.mode", "earliest");
        options.put("scan.end-timestamp-millis", "2000");
        options.put("scan.max-concurrent-streams-per-subtask", "5");
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) source(CHANGE_STREAM_SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider.createSource())
                .extracting(
                        "config.table",
                        "config.appProfileId",
                        "config.serviceAccountKeyFile",
                        "config.startPosition",
                        "config.resumeFallback",
                        "config.endTime",
                        "config.maxConcurrentStreamsPerSubtask")
                .containsExactly(
                        DESTINATION,
                        "single-cluster-profile",
                        "/var/run/secrets/bigtable.json",
                        StartPosition.at(Instant.ofEpochMilli(1000L)),
                        StartPosition.earliest(),
                        Instant.ofEpochMilli(2000L),
                        5);
    }

    @Test
    void requiresAChangelogModeAndSingleClusterAppProfile() {
        Map<String, String> noChangelogMode = minimalChangeStreamOptions();
        noChangelogMode.remove(BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE.key());
        // Asserted as the one contiguous phrase, not as "both names appear somewhere". The stack
        // trace also carries FactoryUtil's dump of the whole WITH clause, and a looser assertion
        // survives dropping the lead-in or the separator while still finding both names.
        AbstractThrowableAssert<?, ?> missingMode =
                assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, noChangelogMode))
                        .isInstanceOf(ValidationException.class)
                        .hasStackTraceContaining("is required when 'scan.mode' = 'change-stream'")
                        .hasStackTraceContaining(
                                "Set it to one of: "
                                        + Arrays.stream(ChangeStreamChangelogMode.values())
                                                .map(mode -> "'" + mode + "'")
                                                .collect(Collectors.joining(", ")));
        // Built from values() rather than a literal, so a mode added later has to reach the message
        // or this fails. The assertion it replaced required the message to name the envelope alone,
        // which is how the message came to offer one of two valid modes.
        for (ChangeStreamChangelogMode mode : ChangeStreamChangelogMode.values()) {
            missingMode
                    .as("changelog mode '%s' is offered", mode)
                    .hasStackTraceContaining("'" + mode + "'");
        }

        Map<String, String> noProfile = minimalChangeStreamOptions();
        noProfile.remove("scan.app-profile-id");
        assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, noProfile))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("single-cluster application profile");
    }

    @Test
    void requiresTheExactEnvelopeSchemaAndNoPrimaryKey() {
        assertThatThrownBy(() -> source(SCHEMA, minimalChangeStreamOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires exactly this physical schema");

        DataType renamedDiscriminator =
                DataTypes.ROW(
                        DataTypes.FIELD("type", DataTypes.STRING()),
                        DataTypes.FIELD("bytes_value", DataTypes.BYTES()),
                        DataTypes.FIELD("long_value", DataTypes.BIGINT()));
        assertThatThrownBy(
                        () ->
                                source(
                                        changeStreamSchema(renamedDiscriminator),
                                        minimalChangeStreamOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires exactly this physical schema");

        DataType nonNullDiscriminator =
                DataTypes.ROW(
                        DataTypes.FIELD("value_type", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("bytes_value", DataTypes.BYTES()),
                        DataTypes.FIELD("long_value", DataTypes.BIGINT()));
        assertThatThrownBy(
                        () ->
                                source(
                                        changeStreamSchema(nonNullDiscriminator),
                                        minimalChangeStreamOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires exactly this physical schema");

        ResolvedSchema keyed =
                new ResolvedSchema(
                        CHANGE_STREAM_SCHEMA.getColumns(),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("row_key")));
        assertThatThrownBy(() -> source(keyed, minimalChangeStreamOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must not declare a primary key");
    }

    @Test
    void rejectsOptionsOwnedByTheOtherSourceMode() {
        for (Map.Entry<String, String> incompatible : CHANGE_STREAM_OWNED_OPTIONS) {
            Map<String, String> bounded = minimalOptions();
            bounded.put(incompatible.getKey(), incompatible.getValue());
            assertThatThrownBy(() -> source(bounded))
                    .as("with incompatible option '%s'", incompatible.getKey())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("not valid when 'scan.mode' = 'bounded'");
        }

        for (Map.Entry<String, String> incompatible :
                Arrays.asList(
                        Map.entry("emulator-endpoint", "localhost:8086"),
                        Map.entry("null-string-literal", "<null>"),
                        Map.entry("scan.row-key-encoding", "UTF8"),
                        Map.entry("scan.row-prefix", "a"),
                        Map.entry("scan.row-range.start-closed", "a"),
                        Map.entry("scan.row-range.end-open", "z"),
                        Map.entry("scan.row-ranges", "[a,z)"),
                        Map.entry("lookup.async", "true"),
                        Map.entry("lookup.cache", "none"),
                        Map.entry("lookup.max-retries", "4"),
                        Map.entry("lookup.partial-cache.expire-after-access", "1 min"),
                        Map.entry("lookup.partial-cache.expire-after-write", "2 min"),
                        Map.entry("lookup.partial-cache.cache-missing-key", "false"),
                        Map.entry("lookup.partial-cache.max-rows", "100"),
                        Map.entry("lookup.full-cache.reload-strategy", "TIMED"),
                        Map.entry("lookup.full-cache.periodic-reload.interval", "5 min"),
                        Map.entry("lookup.full-cache.periodic-reload.schedule-mode", "FIXED_RATE"),
                        Map.entry("lookup.full-cache.timed-reload.iso-time", "10:15Z"),
                        Map.entry("lookup.full-cache.timed-reload.interval-in-days", "2"))) {
            Map<String, String> changeStream = minimalChangeStreamOptions();
            changeStream.put(incompatible.getKey(), incompatible.getValue());
            assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, changeStream))
                    .as("with incompatible option '%s'", incompatible.getKey())
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("not valid when 'scan.mode' = 'change-stream'");
        }
    }

    @Test
    void validatesChangeStreamPositionPairsAndConcurrencyAtPlanningTime() {
        Map<String, String> missingTimestamp = minimalChangeStreamOptions();
        missingTimestamp.put("scan.startup.mode", "timestamp");
        assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, missingTimestamp))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("requires option 'scan.startup.timestamp-millis'");

        Map<String, String> unusedTimestamp = minimalChangeStreamOptions();
        unusedTimestamp.put("scan.resume-fallback.timestamp-millis", "1000");
        assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, unusedTimestamp))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("was set without 'scan.resume-fallback.mode'");

        Map<String, String> timestampForLatest = minimalChangeStreamOptions();
        timestampForLatest.put("scan.startup.mode", "latest");
        timestampForLatest.put("scan.startup.timestamp-millis", "1000");
        assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, timestampForLatest))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "'scan.startup.timestamp-millis' is only valid when"
                                + " 'scan.startup.mode' = 'timestamp'");

        Map<String, String> zeroConcurrency = minimalChangeStreamOptions();
        zeroConcurrency.put("scan.max-concurrent-streams-per-subtask", "0");
        assertThatThrownBy(() -> source(CHANGE_STREAM_SCHEMA, zeroConcurrency))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must be positive, but was 0");
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
        options.put("service-account-key-file", "/var/run/secrets/bigtable.json");
        options.put("scan.row-prefix", "user;web");
        options.put("scan.row-range.start-closed", "a");
        options.put("scan.row-range.end-open", "m");
        options.put("scan.row-ranges", "[q,s);[x,z)");

        BigtableSourceConfig<?> config = builtSource(SCHEMA, options);

        assertThat(config.getAppProfileId()).isEqualTo("reader-profile");
        assertThat(config.getServiceAccountKeyFile()).isEqualTo("/var/run/secrets/bigtable.json");
        assertThat(config.getRanges().stream().map(RowRanges::format).collect(Collectors.toList()))
                .containsExactly("[a, m)", "[q, s)", "[user, uses)", "[web, wec)", "[x, z)");
    }

    @Test
    void base64ScanBoundsReachTheSourceAsExactBytes() {
        Map<String, String> options = minimalOptions();
        options.put("scan.row-key-encoding", "BASE64");
        options.put("scan.row-prefix", "AA==;/g==");
        options.put("scan.row-range.start-closed", "gAA=");
        options.put("scan.row-range.end-open", "gP8=");
        options.put("scan.row-ranges", "[/wA=,/wE=)");

        BigtableSourceConfig<?> config = builtSource(SCHEMA, options);

        assertThat(config.getRanges())
                .containsExactly(
                        ByteStringRange.prefix(ByteString.copyFrom(new byte[] {0x00})),
                        ByteStringRange.unbounded()
                                .startClosed(ByteString.copyFrom(new byte[] {(byte) 0x80, 0x00}))
                                .endOpen(
                                        ByteString.copyFrom(new byte[] {(byte) 0x80, (byte) 0xff})),
                        ByteStringRange.prefix(ByteString.copyFrom(new byte[] {(byte) 0xfe})),
                        ByteStringRange.unbounded()
                                .startClosed(ByteString.copyFrom(new byte[] {(byte) 0xff, 0x00}))
                                .endOpen(ByteString.copyFrom(new byte[] {(byte) 0xff, 0x01})));
    }

    @Test
    void theDefaultRowKeyEncodingRemainsUtf8() {
        Map<String, String> options = minimalOptions();
        options.put("scan.row-range.start-closed", "\u00e9");

        assertThat(builtSource(SCHEMA, options).getRanges())
                .containsExactly(
                        ByteStringRange.unbounded().startClosed(ByteString.copyFromUtf8("\u00e9")));
    }

    @Test
    void malformedBase64IsRejectedWhenTheFactoryBuildsTheSource() {
        Map<String, String> options = minimalOptions();
        options.put("scan.row-key-encoding", "BASE64");
        options.put("scan.row-range.start-closed", "YQ");

        assertThatThrownBy(() -> source(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("canonical padded RFC 4648 standard Base64");
    }

    @Test
    void malformedMultipleRangeIsRejectedWhenTheFactoryBuildsTheSource() {
        Map<String, String> options = minimalOptions();
        options.put("scan.row-ranges", "[a,b);[z,a)");

        assertThatThrownBy(() -> source(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("'scan.row-ranges' entry 2")
                .hasStackTraceContaining("decoded start greater than its end");
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
                    .hasStackTraceContaining("decodes to an empty row key");
        }
    }

    @Test
    void rejectsEveryEmptyPrefixElement() {
        for (String value : new String[] {"", ";", ";a", "a;", "a;" + ";b"}) {
            Map<String, String> options = minimalOptions();
            options.put("scan.row-prefix", value);

            assertThatThrownBy(() -> source(options))
                    .as("with scan.row-prefix='%s'", value)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("decodes to an empty row key")
                    .hasStackTraceContaining("Remove the empty value");
        }
    }
}
