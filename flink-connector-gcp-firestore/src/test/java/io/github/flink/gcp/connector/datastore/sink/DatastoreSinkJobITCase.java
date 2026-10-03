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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.datastore.Entity;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the sink inside a Flink MiniCluster, the only coverage of the path a real job takes: the
 * sink is serialized to the task, the writer is created through {@code
 * createWriter(WriterInitContext)}, and flushes are driven by checkpoint barriers rather than by a
 * test calling them.
 *
 * <p>These read the kind after the job finishes, by which point the end-of-input flush has run too,
 * so they do not prove that a mid-job flush wrote anything; the writer's unit tests pin what a
 * flush sends.
 */
class DatastoreSinkJobITCase extends AbstractDatastoreEmulatorITCase {

    private static final int RECORDS = 40;

    @Test
    void writesEveryRecordOfACheckpointedStreamingJob() throws Exception {
        String kind = uniqueKind();

        StreamExecutionEnvironment env = miniCluster();
        env.enableCheckpointing(1_000);
        env.fromSource(
                        new DataGeneratorSource<>(
                                index -> index,
                                RECORDS,
                                RateLimiterStrategy.perSecond(10),
                                Types.LONG),
                        WatermarkStrategy.noWatermarks(),
                        "records")
                .sinkTo(sink(kind, false));
        env.execute("datastore-sink-streaming");

        assertThat(ids(kind)).containsExactlyElementsOf(expected(id -> true));
    }

    @Test
    void writesEveryRecordOfABoundedJobOnTheEndOfInputFlush() throws Exception {
        String kind = uniqueKind();

        StreamExecutionEnvironment env = miniCluster();
        env.fromSequence(0, RECORDS - 1).sinkTo(sink(kind, false));
        env.execute("datastore-sink-bounded");

        assertThat(ids(kind)).containsExactlyElementsOf(expected(id -> true));
    }

    @Test
    void skippedRecordsReachNoKind() throws Exception {
        String kind = uniqueKind();

        StreamExecutionEnvironment env = miniCluster();
        env.fromSequence(0, RECORDS - 1).sinkTo(sink(kind, true));
        env.execute("datastore-sink-skipping");

        assertThat(ids(kind)).containsExactlyElementsOf(expected(id -> id % 2 == 0));
    }

    private static Sink<Long> sink(String kind, boolean skipOdd) {
        return DatastoreSink.<Long>builder()
                .database(database())
                .serializer(
                        (element, context) ->
                                skipOdd && element % 2 == 1
                                        ? null
                                        : DatastoreMutation.upsert(
                                                Entity.newBuilder(key(kind, "e" + element))
                                                        .set("id", element)
                                                        .build()))
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

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

    private static List<Long> ids(String kind) {
        return readAll(kind).stream()
                .map(entity -> entity.getLong("id"))
                .sorted()
                .collect(Collectors.toList());
    }

    private static List<Long> expected(LongPredicate keep) {
        return LongStream.range(0, RECORDS).filter(keep).boxed().collect(Collectors.toList());
    }
}
