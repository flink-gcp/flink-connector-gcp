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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the sink inside a Flink MiniCluster, which is the only coverage of the path a real job
 * takes: the sink is serialized to the task, the writer is created through {@code
 * createWriter(WriterInitContext)}, and the flushes are driven by checkpoint barriers rather than
 * by a test calling them.
 *
 * <p>What these do <em>not</em> prove is that a mid-job flush wrote anything: they read the table
 * after the job finishes, by which point the end-of-input flush has run too. That a {@code
 * flush(false)} sends at all is pinned at unit level instead, by {@code
 * SpannerWriterTest.holdsMutationsUntilTheBarrier}.
 */
class SpannerSinkJobITCase extends AbstractSpannerEmulatorITCase {

    private static final int RECORDS = 40;

    @Test
    void writesEveryRecordOfACheckpointedStreamingJob() throws Exception {
        SpannerDatabase database = ordersDatabase();

        StreamExecutionEnvironment env = miniCluster();
        // Slow enough that the job spans several checkpoints. The batch limits stay at their
        // defaults, well above the record count, so no limit can fire mid-stream — every write
        // here leaves on a flush.
        env.enableCheckpointing(1_000);
        env.fromSource(
                        new DataGeneratorSource<>(
                                index -> index,
                                RECORDS,
                                RateLimiterStrategy.perSecond(10),
                                Types.LONG),
                        WatermarkStrategy.noWatermarks(),
                        "records")
                .sinkTo(
                        SpannerSink.<Long>builder()
                                .database(database)
                                .serializer((element, context) -> mutation(element))
                                .emulatorEndpoint(emulatorEndpoint())
                                .build());
        env.execute("spanner-sink-streaming");

        assertThat(ids(database)).containsExactlyElementsOf(expectedIds());
    }

    @Test
    void writesEveryRecordOfABoundedJobOnTheEndOfInputFlush() throws Exception {
        SpannerDatabase database = ordersDatabase();

        StreamExecutionEnvironment env = miniCluster();
        env.fromSequence(0, RECORDS - 1)
                .sinkTo(
                        SpannerSink.<Long>builder()
                                .database(database)
                                .serializer((element, context) -> mutation(element))
                                .emulatorEndpoint(emulatorEndpoint())
                                .build());
        env.execute("spanner-sink-bounded");

        assertThat(ids(database)).containsExactlyElementsOf(expectedIds());
    }

    @Test
    void skippedRecordsReachNoTable() throws Exception {
        SpannerDatabase database = ordersDatabase();

        StreamExecutionEnvironment env = miniCluster();
        env.fromSequence(0, RECORDS - 1)
                .sinkTo(
                        SpannerSink.<Long>builder()
                                .database(database)
                                .serializer(
                                        (element, context) ->
                                                element % 2 == 0 ? mutation(element) : null)
                                .emulatorEndpoint(emulatorEndpoint())
                                .build());
        env.execute("spanner-sink-skipping");

        assertThat(ids(database))
                .containsExactlyElementsOf(
                        LongStream.range(0, RECORDS)
                                .filter(id -> id % 2 == 0)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    // ---------------------------------------------------------------- helpers

    private static StreamExecutionEnvironment miniCluster() {
        Configuration configuration = new Configuration();
        // A retry would hide a sink failure behind a green job, which is the one outcome these
        // tests must not produce.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        return env;
    }

    private static SpannerDatabase ordersDatabase() throws Exception {
        return createDatabase(
                Dialect.GOOGLE_STANDARD_SQL,
                "CREATE TABLE orders (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)");
    }

    private static Mutation mutation(long element) {
        return Mutation.newInsertOrUpdateBuilder("orders")
                .set("id")
                .to(element)
                .set("name")
                .to("record-" + element)
                .build();
    }

    private static List<Long> ids(SpannerDatabase database) {
        List<Long> ids = new ArrayList<>();
        for (Struct row : query(database, "SELECT id FROM orders ORDER BY id")) {
            ids.add(row.getLong(0));
        }
        return ids;
    }

    private static List<Long> expectedIds() {
        return LongStream.range(0, RECORDS).boxed().collect(Collectors.toList());
    }
}
