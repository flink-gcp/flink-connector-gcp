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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests against the Bigtable emulator, driving the source exclusively
 * through the public {@code BigtableSource.builder()...emulatorEndpoint(...)} path — no test seams.
 * These are what covers the whole assembly: the builder's ranges surviving into a plan, the plan
 * into splits, the splits through their serializer, and the reader's queries reaching a service
 * that answers them.
 *
 * <p><b>What this suite deliberately does not prove.</b> The emulator's {@code SampleRowKeys}
 * returns the table's final key plus a scattering of random ones, so every plan built here is
 * effectively one split, and nothing about split planning or about reading in parallel is
 * exercised. That coverage is the gated real-GCP suite's, over a pre-split table; the planner's own
 * correctness is a unit test.
 */
class BigtableSourceEmulatorITCase extends AbstractBigtableEmulatorITCase {

    /** Reads a table through the source and returns the row keys it produced, sorted. */
    private static List<String> read(
            TableDestination table, UnaryOperator<BigtableSourceBuilder<String>> customizer)
            throws Exception {
        return read(table, customizer, RuntimeExecutionMode.STREAMING, 1);
    }

    private static List<String> read(
            TableDestination table,
            UnaryOperator<BigtableSourceBuilder<String>> customizer,
            RuntimeExecutionMode mode,
            int parallelism)
            throws Exception {
        Source<String, ?, ?> source =
                customizer
                        .apply(
                                BigtableSource.<String>builder()
                                        .table(table)
                                        .deserializer(new RowKeyDeserializer())
                                        .emulatorEndpoint(emulatorEndpoint()))
                        .build();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        env.setRuntimeMode(mode);
        env.setParallelism(parallelism);
        List<String> keys = new ArrayList<>();
        try (CloseableIterator<String> collected =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "bigtable")
                        .executeAndCollect()) {
            collected.forEachRemaining(keys::add);
        }
        keys.sort(String::compareTo);
        return keys;
    }

    @Test
    void readsEveryRowOfATable() throws Exception {
        TableDestination table = createTable("read-all");
        seedRows(table, "a", "b", "c", "d");

        assertThat(read(table, builder -> builder)).containsExactly("a", "b", "c", "d");
    }

    @Test
    void readsNothingFromAnEmptyTable() throws Exception {
        TableDestination table = createTable("read-empty");

        assertThat(read(table, builder -> builder)).isEmpty();
    }

    @Test
    void readsOnlyTheConfiguredRange() throws Exception {
        TableDestination table = createTable("read-range");
        seedRows(table, "a", "b", "c", "d", "e");

        assertThat(read(table, builder -> builder.rowRange("b", "d"))).containsExactly("b", "c");
    }

    @Test
    void readsOnlyTheConfiguredPrefix() throws Exception {
        TableDestination table = createTable("read-prefix");
        seedRows(table, "other#1", "user#1", "user#2", "zzz");

        assertThat(read(table, builder -> builder.prefix("user#")))
                .containsExactly("user#1", "user#2");
    }

    @Test
    void readsSeveralRangesAtOnce() throws Exception {
        TableDestination table = createTable("read-two-ranges");
        seedRows(table, "a", "b", "m", "n", "z");

        assertThat(read(table, builder -> builder.rowRange("a", "c").rowRange("z", "zz")))
                .containsExactly("a", "b", "z");
    }

    @Test
    void appliesTheFilterOnTheServer() throws Exception {
        TableDestination table = createTable("read-filter");
        seedRows(table, "a", "b", "c");

        // The family filter matches nothing, so the rows never leave the emulator. What this pins
        // is that the filter reaches the wire at all — how Bigtable itself applies one is the
        // service's behaviour, and the gated suite's business.
        assertThat(
                        read(
                                table,
                                builder ->
                                        builder.filter(
                                                Filters.FILTERS.family().exactMatch("absent"))))
                .isEmpty();
    }

    @Test
    void skipsTheRowsTheDeserializerEmitsNothingFor() throws Exception {
        TableDestination table = createTable("read-skip");
        seedRows(table, "keep-1", "drop-1", "keep-2", "drop-2");
        Source<String, ?, ?> source =
                BigtableSource.<String>builder()
                        .table(table)
                        .deserializer(new SkippingDeserializer())
                        .emulatorEndpoint(emulatorEndpoint())
                        .build();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        env.setParallelism(1);

        List<String> keys = new ArrayList<>();
        try (CloseableIterator<String> collected =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "bigtable")
                        .executeAndCollect()) {
            collected.forEachRemaining(keys::add);
        }

        assertThat(keys).containsExactlyInAnyOrder("keep-1", "keep-2");
    }

    @Test
    void finishesABatchJobAndAStreamingOneAlike() throws Exception {
        // A bounded source is not a batch-only one: it runs inside a streaming pipeline and simply
        // ends, which is what makes joining a Bigtable table against an unbounded stream work.
        TableDestination table = createTable("read-modes");
        seedRows(table, "a", "b", "c");

        assertThat(read(table, builder -> builder, RuntimeExecutionMode.BATCH, 1))
                .containsExactly("a", "b", "c");
        assertThat(read(table, builder -> builder, RuntimeExecutionMode.STREAMING, 1))
                .containsExactly("a", "b", "c");
    }

    @Test
    void finishesAJobWhoseSubtasksOutnumberItsSplits() throws Exception {
        // The emulator's sampling gives one split, so at parallelism two one subtask is told there
        // is nothing for it — and the job has to end rather than wait for work that never comes.
        TableDestination table = createTable("read-parallel");
        seedRows(table, "a", "b", "c");

        assertThat(read(table, builder -> builder, RuntimeExecutionMode.STREAMING, 2))
                .containsExactly("a", "b", "c");
    }

    /** Turns each row into its key. */
    private static final class RowKeyDeserializer
            implements BigtableRowDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Row row, Collector<String> out) {
            out.collect(row.getKey().toStringUtf8());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    /** Emits nothing for the rows whose key starts with {@code drop-}. */
    private static final class SkippingDeserializer
            implements BigtableRowDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Row row, Collector<String> out) {
            String key = row.getKey().toStringUtf8();
            if (!key.startsWith("drop-")) {
                out.collect(key);
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
