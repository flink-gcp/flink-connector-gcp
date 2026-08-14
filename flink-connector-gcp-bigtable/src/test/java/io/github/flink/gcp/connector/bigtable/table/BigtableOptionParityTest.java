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

import org.apache.flink.configuration.ConfigOption;

import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkBuilder;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceBuilder;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the DDL option surface and the DataStream builders' setters equal, in both directions.
 *
 * <p>The table layer is a mapping onto those builders, so a knob added to one of them without an
 * option is a gap in SQL and an option whose knob was deleted is a key that configures nothing.
 * Neither shows up anywhere else: the mapper tests read a {@code Configuration} directly and the
 * factory tests set a handful of keys.
 *
 * <p>The tables below are written out rather than derived, because the keys are grouped ({@code
 * sink.batching.*}, {@code sink.recovery.*}) and no naming rule turns a setter into a key. The
 * reflection is what makes them exhaustive.
 *
 * <p>This widens the Pub/Sub precedent, which reflects over its <em>options</em> builders only and
 * so leaves its own sink and source builders unguarded. Reflecting over a connector builder means
 * carrying an exemption set, and each entry states why the setter has no DDL form; because the
 * assertion is a set equality against the union, an exemption that stops being true fails too.
 *
 * <p>Five surfaces: the writer options, the sink builder, the table-creation options, the bounded
 * {@code BigtableSourceBuilder}, and the {@code BigtableChangeStreamSourceBuilder}. The two source
 * builders are alternative scan modes, so their shared destination/profile/credential keys are
 * pinned explicitly rather than treated as two setters in one runtime path.
 */
class BigtableOptionParityTest {

    /** {@code BigtableWriterOptions.Builder}: every knob has a key, and there are no exemptions. */
    private static final Map<String, ConfigOption<?>> WRITER_OPTIONS = writerOptions();

    private static Map<String, ConfigOption<?>> writerOptions() {
        Map<String, ConfigOption<?>> map = new LinkedHashMap<>();
        map.put("batchElementCount", BigtableConnectorOptions.SINK_BATCHING_ELEMENT_COUNT);
        map.put("batchByteSize", BigtableConnectorOptions.SINK_BATCHING_BYTE_SIZE);
        map.put("maxInFlightEntries", BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_ENTRIES);
        map.put("maxInFlightBytes", BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES);
        map.put(
                "maxConsecutiveRejections",
                BigtableConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS);
        map.put("recoveryInitialBackoff", BigtableConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF);
        map.put("recoveryMaxBackoff", BigtableConnectorOptions.SINK_RECOVERY_MAX_BACKOFF);
        map.put("recoveryMaxAttempts", BigtableConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS);
        map.put("destinationIdleTimeout", BigtableConnectorOptions.SINK_DESTINATION_IDLE_TIMEOUT);
        map.put("perDestinationMetrics", BigtableConnectorOptions.SINK_METRICS_PER_DESTINATION);
        return Collections.unmodifiableMap(map);
    }

    /** {@code BigtableSinkBuilder}: the setters a {@code WITH} clause can reach. */
    private static final Map<String, ConfigOption<?>> SINK_BUILDER = sinkBuilder();

    private static Map<String, ConfigOption<?>> sinkBuilder() {
        Map<String, ConfigOption<?>> map = new LinkedHashMap<>();
        // 'project' and 'instance' reach this same setter through TableDestination.of; the
        // accounting test below is what keeps them from going unnoticed.
        map.put("table", BigtableConnectorOptions.TABLE);
        map.put("appProfileId", BigtableConnectorOptions.SINK_APP_PROFILE_ID);
        map.put("serviceAccountKeyFile", BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE);
        map.put("emulatorEndpoint", BigtableConnectorOptions.EMULATOR_ENDPOINT);
        map.put("createDisposition", BigtableConnectorOptions.SINK_CREATE_DISPOSITION);
        return Collections.unmodifiableMap(map);
    }

    /** {@code BigtableSinkBuilder}: the setters no {@code WITH} clause can reach, and why. */
    private static final Map<String, String> SINK_BUILDER_NO_DDL = sinkBuilderExemptions();

    private static Map<String, String> sinkBuilderExemptions() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(
                "serializer",
                "the table layer supplies RowDataSerializationSchema, built from the DDL schema");
        map.put(
                "destinationResolver",
                "a DDL names one table; a STATEMENT SET of INSERTs is the SQL fan-out");
        map.put(
                "failedMutationHandler",
                "no table connector in this repository exposes a failure policy in a DDL");
        map.put("writerOptions", "takes WriterOptionsMapper's output, covered by its own table");
        map.put(
                "tableCreateOptions",
                "takes TableCreateOptionsMapper's output, whose families come from the DDL");
        return Collections.unmodifiableMap(map);
    }

    /** {@code BigtableSourceBuilder}: the setters a {@code WITH} clause can reach. */
    private static final Map<String, ConfigOption<?>> SOURCE_BUILDER = sourceBuilder();

    private static Map<String, ConfigOption<?>> sourceBuilder() {
        Map<String, ConfigOption<?>> map = new LinkedHashMap<>();
        // 'project' and 'instance' reach this same setter through TableDestination.of, as on the
        // sink side.
        map.put("table", BigtableConnectorOptions.TABLE);
        map.put("prefix", BigtableConnectorOptions.SCAN_ROW_PREFIX);
        map.put("appProfileId", BigtableConnectorOptions.SCAN_APP_PROFILE_ID);
        map.put("serviceAccountKeyFile", BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE);
        map.put("emulatorEndpoint", BigtableConnectorOptions.EMULATOR_ENDPOINT);
        return Collections.unmodifiableMap(map);
    }

    /** {@code BigtableSourceBuilder}: the setters no {@code WITH} clause can reach, and why. */
    private static final Map<String, String> SOURCE_BUILDER_NO_DDL = sourceBuilderExemptions();

    private static Map<String, String> sourceBuilderExemptions() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(
                "deserializer",
                "the table layer supplies RowDataDeserializationSchema, built from the DDL schema");
        map.put(
                "filter",
                "projection and filter pushdown compose the runtime filter; no WITH option maps"
                        + " directly to this setter");
        map.put(
                "rowRange",
                "fed by the two scan.row-range.* keys, which build the one ByteStringRange it"
                        + " takes");
        return Collections.unmodifiableMap(map);
    }

    /** {@code BigtableChangeStreamSourceBuilder}: setters a Change Streams DDL can reach. */
    private static final Map<String, ConfigOption<?>> CHANGE_STREAM_SOURCE_BUILDER =
            changeStreamSourceBuilder();

    private static Map<String, ConfigOption<?>> changeStreamSourceBuilder() {
        Map<String, ConfigOption<?>> map = new LinkedHashMap<>();
        map.put("table", BigtableConnectorOptions.TABLE);
        map.put("appProfileId", BigtableConnectorOptions.SCAN_APP_PROFILE_ID);
        map.put("serviceAccountKeyFile", BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE);
        map.put("startPosition", BigtableConnectorOptions.SCAN_STARTUP_MODE);
        map.put("resumeFallback", BigtableConnectorOptions.SCAN_RESUME_FALLBACK_MODE);
        map.put("endTime", BigtableConnectorOptions.SCAN_END_TIMESTAMP_MILLIS);
        map.put(
                "maxConcurrentStreamsPerSubtask",
                BigtableConnectorOptions.SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK);
        return Collections.unmodifiableMap(map);
    }

    /** Change Streams builder setters supplied structurally rather than by one option. */
    private static final Map<String, String> CHANGE_STREAM_SOURCE_BUILDER_NO_DDL =
            changeStreamSourceBuilderExemptions();

    private static Map<String, String> changeStreamSourceBuilderExemptions() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(
                "deserializer",
                "the table layer supplies the generic mutation-envelope RowData converter");
        map.put(
                "familyIncludeList",
                "entry projection belongs to the DataStream mutation API, not the Table changelog");
        map.put(
                "familyExcludeList",
                "entry projection belongs to the DataStream mutation API, not the Table changelog");
        map.put(
                "qualifierIncludeList",
                "entry projection belongs to the DataStream mutation API, not the Table changelog");
        map.put(
                "qualifierExcludeList",
                "entry projection belongs to the DataStream mutation API, not the Table changelog");
        map.put(
                "skipMessagesWithoutChange",
                "empty projected mutations belong to the DataStream mutation API, not the Table changelog");
        return Collections.unmodifiableMap(map);
    }

    /** {@code TableCreateOptions.Builder}: the one setter, which the DDL feeds structurally. */
    private static final Map<String, String> TABLE_CREATE_NO_DDL =
            Collections.singletonMap(
                    "columnFamily",
                    "the families are the DDL's ROW<...> columns, not a key; only their"
                            + " garbage-collection rule is configured, by two keys that build a"
                            + " GcRule rather than call a setter");

    /** Options that configure something other than one setter, each with what it does reach. */
    private static final Map<String, String> NOT_A_SETTER = notASetter();

    private static Map<String, String> notASetter() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(
                BigtableConnectorOptions.PROJECT.key(),
                "a component of the TableDestination that table(...) takes");
        map.put(
                BigtableConnectorOptions.INSTANCE.key(),
                "a component of the TableDestination that table(...) takes");
        map.put(
                BigtableConnectorOptions.NULL_STRING_LITERAL.key(),
                "the cell codec's null marker; the DataStream API has no serializer to configure"
                        + " here, since a DataStream user writes their own");
        map.put(
                BigtableConnectorOptions.LOOKUP_ASYNC.key(),
                "selects the table runtime provider shape; the DataStream scan source has no"
                        + " lookup concept");
        map.put(
                BigtableConnectorOptions.SCAN_ROW_KEY_ENCODING.key(),
                "selects how the table factory decodes scan bounds before it calls the source"
                        + " builder");
        map.put(
                BigtableConnectorOptions.SCAN_MODE.key(),
                "selects the bounded or Change Streams source builder");
        map.put(
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE.key(),
                "selects the envelope or selected-cell planner contract");
        map.put(
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_FAMILY.key(),
                "identifies the complete-value cell interpreted by the table layer");
        map.put(
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_QUALIFIER_BASE64.key(),
                "identifies the complete-value cell interpreted by the table layer");
        map.put(
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_SOURCE_CLUSTER_ID.key(),
                "bounds the selected-cell mutation protocol to one source cluster");
        map.put(
                BigtableConnectorOptions.VALUE_FORMAT.key(),
                "is discovered by the table factory and decodes selected-cell non-key columns");
        map.put(
                BigtableConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                "is the instant component of the StartPosition passed to startPosition(...)");
        map.put(
                BigtableConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS.key(),
                "is the instant component of the StartPosition passed to resumeFallback(...)");
        map.put(
                BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS.key(),
                "builds the GcRule every created family takes");
        map.put(
                BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE.key(),
                "builds the GcRule every created family takes");
        map.put(
                BigtableConnectorOptions.SINK_CELL_TIMESTAMP_TRUNCATE_TO_MILLIS.key(),
                "configures the table layer's timestamp metadata serializer");
        map.put(
                BigtableConnectorOptions.SINK_INSERT_ONLY_INPUT_MODE.key(),
                "configures the changelog mode the table sink advertises to the planner");
        map.put(
                BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED.key(),
                "builds the one ByteStringRange that rowRange(...) takes");
        map.put(
                BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN.key(),
                "builds the one ByteStringRange that rowRange(...) takes");
        map.put(
                BigtableConnectorOptions.SCAN_ROW_RANGES.key(),
                "builds every ByteStringRange that repeated rowRange(...) calls take");
        return Collections.unmodifiableMap(map);
    }

    /**
     * The public setters of a builder: declared, public, and returning the builder itself.
     *
     * <p>That one return-type filter is what drops {@code build()}, {@code equals}, {@code
     * hashCode} and {@code toString} without naming any of them. Deliberately not filtered on
     * arity, because {@code TableCreateOptions.Builder.columnFamily} already has two shapes and an
     * arity filter would let a third slip in unmapped.
     *
     * <p>Two things it would miss, neither of which exists today: a setter inherited from a
     * superclass ({@code getDeclaredMethods} does not see one) and a setter declaring a supertype
     * as its return type. Both become reachable the moment one of these builders grows a fluent
     * base class, which is the point at which this filter needs revisiting.
     */
    private static Set<String> publicSettersOf(Class<?> builder) {
        return Arrays.stream(builder.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(m -> m.getReturnType() == builder)
                .map(Method::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Test
    void everyWriterKnobHasAnOption() {
        assertThat(publicSettersOf(BigtableWriterOptions.Builder.class))
                .isEqualTo(WRITER_OPTIONS.keySet());
    }

    @Test
    void everySinkBuilderKnobIsMappedOrExempt() {
        Set<String> expected = new LinkedHashSet<>(SINK_BUILDER.keySet());
        expected.addAll(SINK_BUILDER_NO_DDL.keySet());

        assertThat(publicSettersOf(BigtableSinkBuilder.class)).isEqualTo(expected);
    }

    @Test
    void everyTableCreateKnobIsMappedOrExempt() {
        assertThat(publicSettersOf(TableCreateOptions.Builder.class))
                .isEqualTo(TABLE_CREATE_NO_DDL.keySet());
    }

    @Test
    void everySourceBuilderKnobIsMappedOrExempt() {
        Set<String> expected = new LinkedHashSet<>(SOURCE_BUILDER.keySet());
        expected.addAll(SOURCE_BUILDER_NO_DDL.keySet());

        assertThat(publicSettersOf(BigtableSourceBuilder.class)).isEqualTo(expected);
    }

    @Test
    void everyChangeStreamSourceBuilderKnobIsMappedOrExempt() {
        Set<String> expected = new LinkedHashSet<>(CHANGE_STREAM_SOURCE_BUILDER.keySet());
        expected.addAll(CHANGE_STREAM_SOURCE_BUILDER_NO_DDL.keySet());

        assertThat(publicSettersOf(BigtableChangeStreamSourceBuilder.class)).isEqualTo(expected);
    }

    @Test
    void noOptionFeedsTwoSettersOfOneDirection() {
        List<String> sinkKeys =
                java.util.stream.Stream.concat(
                                WRITER_OPTIONS.values().stream(), SINK_BUILDER.values().stream())
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        List<String> sourceKeys =
                SOURCE_BUILDER.values().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());

        assertThat(sinkKeys).doesNotHaveDuplicates();
        assertThat(sourceKeys).doesNotHaveDuplicates();
        assertThat(
                        CHANGE_STREAM_SOURCE_BUILDER.values().stream()
                                .map(ConfigOption::key)
                                .collect(Collectors.toList()))
                .doesNotHaveDuplicates();
    }

    @Test
    void sourceModesShareExactlyTableProfileAndCredentials() {
        Set<String> bounded =
                SOURCE_BUILDER.values().stream().map(ConfigOption::key).collect(Collectors.toSet());
        Set<String> shared =
                CHANGE_STREAM_SOURCE_BUILDER.values().stream()
                        .map(ConfigOption::key)
                        .filter(bounded::contains)
                        .collect(Collectors.toSet());

        assertThat(shared)
                .containsExactlyInAnyOrder(
                        BigtableConnectorOptions.TABLE.key(),
                        BigtableConnectorOptions.SCAN_APP_PROFILE_ID.key(),
                        BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key());
    }

    @Test
    void theDirectionsShareExactlyDestinationCredentialsAndEmulator() {
        // These three shared keys feed a setter on each direction's builder, and nothing else may:
        // a sink.* key reaching the source builder — or the reverse — is a wiring slip.
        Set<String> sinkKeys = new HashSet<>();
        WRITER_OPTIONS.values().forEach(o -> sinkKeys.add(o.key()));
        SINK_BUILDER.values().forEach(o -> sinkKeys.add(o.key()));
        Set<String> shared =
                SOURCE_BUILDER.values().stream()
                        .map(ConfigOption::key)
                        .filter(sinkKeys::contains)
                        .collect(Collectors.toSet());

        assertThat(shared)
                .containsExactlyInAnyOrder(
                        BigtableConnectorOptions.TABLE.key(),
                        BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(),
                        BigtableConnectorOptions.EMULATOR_ENDPOINT.key());
    }

    @Test
    void everyDeclaredOptionIsAccountedFor() {
        Set<String> mapped = new HashSet<>();
        WRITER_OPTIONS.values().forEach(o -> mapped.add(o.key()));
        SINK_BUILDER.values().forEach(o -> mapped.add(o.key()));
        SOURCE_BUILDER.values().forEach(o -> mapped.add(o.key()));
        CHANGE_STREAM_SOURCE_BUILDER.values().forEach(o -> mapped.add(o.key()));
        mapped.addAll(NOT_A_SETTER.keySet());

        Set<String> declared = declaredKeys();
        assertThat(declared).isNotEmpty();
        // Both directions: an option added with no home here, and an entry above naming a key that
        // no longer exists.
        assertThat(declared).isEqualTo(mapped);
    }

    private static Set<String> declaredKeys() {
        return DeclaredOptions.all().stream()
                .map(ConfigOption::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
