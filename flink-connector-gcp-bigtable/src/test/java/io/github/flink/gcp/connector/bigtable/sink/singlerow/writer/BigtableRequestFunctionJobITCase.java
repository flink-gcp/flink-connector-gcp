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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.CloseableIterator;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.ReadModifyWriteRowRequest.Rule;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration tests for {@link BigtableRequestFunction} on the MiniCluster against the
 * Bigtable emulator, through the production constructor — so the credential loading, the client
 * factory and the {@code BigtableRow} type information a job serializes results with are what these
 * exercise.
 *
 * <p>The streaming job checkpoints while records are still arriving, so the async operator's own
 * state — the inputs it has accepted and not yet emitted — is taken and the function's result
 * completion is what lets those checkpoints complete.
 */
class BigtableRequestFunctionJobITCase extends AbstractBigtableEmulatorITCase {

    private static final String COUNTERS = "counters";
    private static final long RECORD_COUNT = 20;
    private static final int ROW_COUNT = 4;
    private static final double RECORDS_PER_SECOND = 10;

    @Test
    void streamingJobEmitsEveryAnswerAndAppliesEveryIncrement() throws Exception {
        TableDestination table = createTable("job-increments", FAMILY, COUNTERS);
        StreamExecutionEnvironment env = environment();
        env.setParallelism(2);
        env.enableCheckpointing(500);
        DataStream<String> records =
                env.fromSource(
                        new DataGeneratorSource<>(
                                (GeneratorFunction<Long, String>) index -> "record-" + index,
                                RECORD_COUNT,
                                RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                                Types.STRING),
                        WatermarkStrategy.noWatermarks(),
                        "records");
        DataStream<BigtableRow> answers =
                AsyncDataStream.unorderedWait(
                        records,
                        new IncrementFunction(table, emulatorEndpoint()),
                        30,
                        TimeUnit.SECONDS,
                        100);

        List<BigtableRow> collected = new ArrayList<>();
        try (CloseableIterator<BigtableRow> iterator = answers.executeAndCollect()) {
            iterator.forEachRemaining(collected::add);
        }

        // One answer per record, each carrying the counter cell the increment touched — the
        // connector-owned row crossed a serialization boundary to get here.
        assertThat(collected).hasSize((int) RECORD_COUNT);
        assertThat(collected)
                .allSatisfy(
                        row -> {
                            assertThat(row.getKey().toStringUtf8()).startsWith("row-");
                            assertThat(row.getCells()).hasSize(1);
                            assertThat(row.getCells().get(0).getFamily()).isEqualTo(COUNTERS);
                            assertThat(row.getCells().get(0).getValue().size()).isEqualTo(8);
                        });
        // Every row saw exactly its share: the increments are the service's, applied once each.
        List<Row> rows = readRows(table);
        assertThat(rows).hasSize(ROW_COUNT);
        for (Row row : rows) {
            ByteString counter = row.getCells(COUNTERS, "hits").get(0).getValue();
            assertThat(ByteBuffer.wrap(counter.toByteArray()).getLong())
                    .isEqualTo(RECORD_COUNT / ROW_COUNT);
        }
    }

    @Test
    void aRequestTheServiceNeverAnswersFailsTheJobNamingBothDeadlines() {
        // The operator's timeout, with a client that never answers: what the user sees is the
        // connector's message and not the async operator's generic one — which also pins that
        // the operator hands timeout() the same ResultFuture instance it gave asyncInvoke(), the
        // identity the function's ledger is keyed by.
        StreamExecutionEnvironment env = environment();
        env.setParallelism(1);
        DataStream<String> records =
                env.fromSource(
                        new DataGeneratorSource<>(
                                (GeneratorFunction<Long, String>) index -> "record-" + index,
                                1,
                                Types.STRING),
                        WatermarkStrategy.noWatermarks(),
                        "records");
        AsyncDataStream.unorderedWait(
                        records,
                        new NeverAnsweredFunction(TableDestination.of("p", "i", "orders")),
                        1,
                        TimeUnit.SECONDS,
                        10)
                .sinkTo(new DiscardingSink<>());

        assertThatThrownBy(() -> env.execute("bigtable-request-function-timeout-it"))
                .hasStackTraceContaining(
                        "A ReadModifyWriteRow request to Bigtable table p.i.orders did not"
                                + " complete within the async operator's timeout and was cancelled")
                .hasStackTraceContaining("BigtableRequestOptions.requestTimeout (PT0.5S)");
    }

    private static StreamExecutionEnvironment environment() {
        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a permanently
        // failing request would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        return env;
    }

    /** Increments a per-row hit counter, spreading the records over a few rows. */
    private static final class IncrementFunction
            extends BigtableRequestFunction<String, BigtableRow, BigtableRow> {

        private static final long serialVersionUID = 1L;

        private final TableDestination table;

        IncrementFunction(TableDestination table, String emulatorEndpoint) {
            super(
                    null,
                    BigtableRequestOptions.builder().build(),
                    null,
                    EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint"));
            this.table = table;
        }

        @Override
        protected TableDestination destination(String input) {
            return table;
        }

        @Override
        protected RowRequest<BigtableRow> request(String input) {
            long index = Long.parseLong(input.substring("record-".length()));
            return new ReadModifyWriteRowRequest(
                    ByteString.copyFromUtf8("row-" + index % ROW_COUNT),
                    Collections.singletonList(
                            Rule.increment(COUNTERS, ByteString.copyFromUtf8("hits"), 1L)));
        }

        @Override
        protected BigtableRow result(String input, BigtableRow answer) {
            return answer;
        }
    }

    /** A function over a client that never answers, so only the operator's timeout can end it. */
    private static final class NeverAnsweredFunction
            extends BigtableRequestFunction<String, BigtableRow, BigtableRow> {

        private static final long serialVersionUID = 1L;

        private final TableDestination table;

        NeverAnsweredFunction(TableDestination table) {
            super(
                    new NeverAnsweringFactory(),
                    BigtableRequestOptions.builder()
                            .requestTimeout(Duration.ofMillis(500))
                            .build());
            this.table = table;
        }

        @Override
        protected TableDestination destination(String input) {
            return table;
        }

        @Override
        protected RowRequest<BigtableRow> request(String input) {
            return new ReadModifyWriteRowRequest(
                    ByteString.copyFromUtf8(input),
                    Collections.singletonList(
                            Rule.increment(COUNTERS, ByteString.copyFromUtf8("hits"), 1L)));
        }

        @Override
        protected BigtableRow result(String input, BigtableRow answer) {
            return answer;
        }
    }

    private static final class NeverAnsweringFactory implements SingleRowClientFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public SingleRowClient create(TableDestination destination) {
            return new SingleRowClient() {
                @Override
                public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation mutation) {
                    return SettableApiFuture.create();
                }

                @Override
                public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation) {
                    return SettableApiFuture.create();
                }
            };
        }

        @Override
        public void release(TableDestination destination) {}

        @Override
        public void close() {}
    }
}
