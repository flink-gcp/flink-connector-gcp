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
 * <p>{@code BigtableSourceBuilder} is not covered yet — the {@code scan.*} surface arrives with the
 * table source, and covering it here first would mean exempting every one of its setters with a
 * reason that is scheduled to stop being true.
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
                BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS.key(),
                "builds the GcRule every created family takes");
        map.put(
                BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE.key(),
                "builds the GcRule every created family takes");
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
    void noOptionFeedsTwoSetters() {
        List<String> keys =
                java.util.stream.Stream.concat(
                                WRITER_OPTIONS.values().stream(), SINK_BUILDER.values().stream())
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());

        assertThat(keys).doesNotHaveDuplicates();
    }

    @Test
    void everyDeclaredOptionIsAccountedFor() {
        Set<String> mapped = new HashSet<>();
        WRITER_OPTIONS.values().forEach(o -> mapped.add(o.key()));
        SINK_BUILDER.values().forEach(o -> mapped.add(o.key()));
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
