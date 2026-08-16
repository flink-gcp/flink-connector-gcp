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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The scan source against real Cloud Bigtable.
 *
 * <p>This is the only place three things are covered. Real {@code SampleRowKeys}, over a pre-split
 * table, which is the only way the split planner is exercised against tablets that actually exist —
 * the emulator models none. The production client-construction path, over application-default
 * credentials, which every emulator test bypasses. And the measurement the restore design rests on:
 * the service refuses a range whose start is exclusive at its own end key — the state a split
 * reaches after emitting its last row — which is what makes the reader's finish-without-a-stream
 * short-circuit load-bearing rather than tidy (#481).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableSourceRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final int ROWS = 60;

    /** Created on the ephemeral instance by the application-profile tests. */
    private static final String APP_PROFILE = "flink-it-source";

    private static String key(int index) {
        return String.format("row-%04d", index);
    }

    private static String[] keys() {
        return IntStream.range(0, ROWS)
                .mapToObj(BigtableSourceRealGcpITCase::key)
                .toArray(String[]::new);
    }

    private static List<String> read(
            TableDestination table,
            UnaryOperator<BigtableSourceBuilder<String>> customizer,
            int parallelism)
            throws Exception {
        Source<String, ?, ?> source =
                customizer
                        .apply(
                                BigtableSource.<String>builder()
                                        .table(table)
                                        .deserializer(new TestSources.RowKeyDeserializer()))
                        .build();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        env.setParallelism(parallelism);
        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "bigtable")
                        .executeAndCollect()) {
            records.forEachRemaining(collected::add);
        }
        collected.sort(String::compareTo);
        return collected;
    }

    @Test
    void readsEveryRowOfAPreSplitTableThroughAJob() throws Exception {
        TableDestination table = createTableWithSplits("source-presplit", key(20), key(40));
        seedRows(table, keys());

        assertThat(read(table, builder -> builder, 2))
                .containsExactlyElementsOf(
                        IntStream.range(0, ROWS)
                                .mapToObj(BigtableSourceRealGcpITCase::key)
                                .collect(Collectors.toList()));
    }

    @Test
    void samplesOneBoundaryPerTabletOfAPreSplitTable() {
        // The measurement the whole split-planning design rests on, and the one the emulator
        // cannot produce: it models no tablets, so it answers with next to no boundaries whatever
        // the table looks like.
        TableDestination table = createTableWithSplits("source-sample", key(20), key(40));
        seedRows(table, keys());

        List<KeyOffset> samples = sampleRowKeys(table);

        assertThat(samples)
                .extracting(sample -> sample.getKey().toStringUtf8())
                .contains(key(20), key(40));
        assertThat(samples)
                .extracting(KeyOffset::getOffsetBytes)
                .isSortedAccordingTo(Long::compareTo);
    }

    @Test
    void refusesARangeExclusiveAtItsOwnEndKey() {
        // What a split's range looks like after its last row was emitted. The service refuses it
        // outright — INVALID_ARGUMENT, "start_key must be less than end_key", measured 2026-08-10
        // (#481) — rather than answering it empty, which is what makes the reader finishing such a
        // split without opening a stream load-bearing rather than tidy. Asserted on the status;
        // the message is the service's prose to change.
        TableDestination table = createTable("source-empty-range");
        seedRows(table, key(0), key(1));

        assertThatThrownBy(
                        () ->
                                readRange(
                                        table,
                                        ByteStringRange.unbounded()
                                                .startOpen(ByteString.copyFromUtf8(key(1)))
                                                .endClosed(ByteString.copyFromUtf8(key(1)))))
                .isInstanceOf(InvalidArgumentException.class);
    }

    @Test
    void answersAClosedClosedRangeAtOneKeyWithThatRow() {
        // The refused shape's nearest legal neighbour, measured so ADR-0080's scope note carries
        // an answer rather than an unknown: [K, K] with both bounds closed is a single-row read,
        // so the refusal above is about a range empty by construction, not about start == end.
        // No connector path produces this shape either — truncation always yields an open start.
        TableDestination table = createTable("source-single-key-range");
        seedRows(table, key(0), key(1));

        assertThat(
                        readRange(
                                table,
                                ByteStringRange.unbounded()
                                        .startClosed(ByteString.copyFromUtf8(key(1)))
                                        .endClosed(ByteString.copyFromUtf8(key(1)))))
                .extracting(row -> row.getKey().toStringUtf8())
                .containsExactly(key(1));
    }

    @Test
    void readsThroughAnApplicationProfileTheInstanceDefines() throws Exception {
        // The one testable statement this project makes about Data Boost: a profile is named
        // through appProfileId like any other. Nothing here exercises Data Boost itself, which
        // needs an Enterprise-edition instance and SPU billing (#248). Gated because the emulator
        // ignores application profiles entirely.
        TableDestination table = createTable("source-app-profile");
        seedRows(table, key(0), key(1));
        createSingleClusterAppProfile(APP_PROFILE);

        assertThat(read(table, builder -> builder.appProfileId(APP_PROFILE), 1))
                .containsExactly(key(0), key(1));
    }

    @Test
    void failsWhenTheApplicationProfileDoesNotExist() throws Exception {
        // The load-bearing half: a source that dropped the setter would pass the test above by
        // reading through the instance's default profile, and fail only here — which is why the
        // assertion must not accept just any failure. NOT_FOUND, measured 2026-08-10 (#481):
        // asserted on the status name in the chain's messages, the SerializedThrowable rule the
        // filter test below explains.
        TableDestination table = createTable("source-app-profile-missing");
        seedRows(table, key(0));

        // The control: the same table reads fine through the default profile, so the only
        // NOT_FOUND left for the read below to earn is the profile's.
        assertThat(read(table, builder -> builder, 1)).containsExactly(key(0));

        assertThatThrownBy(() -> read(table, builder -> builder.appProfileId("no-such-profile"), 1))
                .satisfies(
                        thrown -> ExceptionUtils.assertThrowableWithMessage(thrown, "NOT_FOUND"));
    }

    @Test
    void appliesRangesAndPrefixesOnTheServer() throws Exception {
        TableDestination table = createTable("source-pushdown");
        seedRows(table, keys());

        assertThat(read(table, builder -> builder.rowRange(key(10), key(13)), 1))
                .containsExactly(key(10), key(11), key(12));
        assertThat(read(table, builder -> builder.prefix("row-000"), 1))
                .containsExactlyElementsOf(
                        IntStream.range(0, 10)
                                .mapToObj(BigtableSourceRealGcpITCase::key)
                                .collect(Collectors.toList()));
    }

    @Test
    void refusesAFilterNamingAColumnFamilyTheTableDoesNotHave() throws Exception {
        // NOT_FOUND, "Requested column family not found", measured 2026-08-10 (#481) — the read
        // fails rather than answering empty, so a misconfigured filter fails the job loudly. The
        // source deliberately does not pre-validate a filter's families against the table: that
        // would cost the scan a metadata read it does not otherwise need, to soften an error the
        // service already reports precisely. The refusal being the service's own answer is also
        // what tells the filter apart from one that never left the client.
        //
        // Asserted on the status name in the chain's messages, not on the gax exception class:
        // Flink transports a task-side failure as SerializedThrowable, which keeps the original
        // class only as a message prefix — so a class-based findThrowable can never match here
        // (measured against the MiniCluster, same day). assertThrowableWithMessage rethrows the
        // original chain on a miss, so a red run reports what was actually thrown.
        TableDestination table = createTable("source-filter-absent-family");
        seedRows(table, key(0));

        // The control: the same table reads fine unfiltered, so the only NOT_FOUND left for the
        // read below to earn is the filter's.
        assertThat(read(table, builder -> builder, 1)).containsExactly(key(0));

        assertThatThrownBy(
                        () ->
                                read(
                                        table,
                                        builder ->
                                                builder.filter(
                                                        Filters.FILTERS
                                                                .family()
                                                                .exactMatch("absent")),
                                        1))
                .satisfies(
                        thrown -> ExceptionUtils.assertThrowableWithMessage(thrown, "NOT_FOUND"));
    }
}
