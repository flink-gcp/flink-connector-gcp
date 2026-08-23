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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonParser;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Restarts a checkpointed Change Streams job against each emulator dialect. */
class SpannerChangeStreamSourceFailoverITCase extends AbstractSpannerEmulatorITCase {

    private static final int RECORDS = 40;
    private static final int FAIL_AFTER = 10;
    private static final AtomicBoolean FAILED = new AtomicBoolean();
    private static final AtomicBoolean CHECKPOINT_AFTER_THRESHOLD = new AtomicBoolean();
    private static final AtomicLong FAILURE_CHECKPOINT_HIGHEST = new AtomicLong(-1);

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void resumesInclusivelyWithoutLosingRecords(Dialect dialect) throws Exception {
        FAILED.set(false);
        CHECKPOINT_AFTER_THRESHOLD.set(false);
        FAILURE_CHECKPOINT_HIGHEST.set(-1);
        Seeded seeded = seed(dialect);

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(50);

        SpannerChangeStreamSource<Long> source =
                SpannerChangeStreamSource.<Long>builder()
                        .database(seeded.database)
                        .changeStreamName("changes")
                        .deserializer(new KeyDeserializer())
                        .startPosition(StartPosition.at(seeded.start))
                        .heartbeatInterval(Duration.ofSeconds(1))
                        .maxConcurrentQueriesPerSubtask(2)
                        .emulatorEndpoint(emulatorEndpoint())
                        .endTimestamp(seeded.end)
                        .build();

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<Long> collected =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "spanner-change-stream")
                        .map(new FailOnce())
                        .executeAndCollect()) {
            collected.forEachRemaining(ids::add);
        }

        assertThat(FAILED).isTrue();
        assertThat(ids).containsAll(expectedIds());
        long checkpointedHighest = FAILURE_CHECKPOINT_HIGHEST.get();
        assertThat(checkpointedHighest).isGreaterThanOrEqualTo(FAIL_AFTER);
        for (long id = 0; id < checkpointedHighest; id++) {
            long expectedId = id;
            assertThat(ids.stream().filter(seen -> seen == expectedId).count())
                    .as("records before the greatest checkpointed id are not reread")
                    .isOne();
        }
        assertThat(ids.size())
                .as("restoring the inclusive commit timestamp repeats the boundary record")
                .isGreaterThan(RECORDS);
    }

    private static Seeded seed(Dialect dialect) throws Exception {
        DatabaseDestination database =
                createDatabase(
                        dialect,
                        dialect == Dialect.POSTGRESQL
                                ? "CREATE TABLE singers (id bigint NOT NULL PRIMARY KEY)"
                                : "CREATE TABLE singers (id INT64 NOT NULL) PRIMARY KEY (id)",
                        "CREATE CHANGE STREAM changes FOR singers");
        Instant start = null;
        Instant end = null;
        for (long id = 0; id < RECORDS; id++) {
            Timestamp committed =
                    client(database)
                            .write(
                                    Collections.singletonList(
                                            Mutation.newInsertBuilder("singers")
                                                    .set("id")
                                                    .to(id)
                                                    .build()));
            Instant instant = instant(committed);
            if (start == null) {
                start = instant;
            }
            end = instant;
        }
        return new Seeded(database, start, end);
    }

    private static List<Long> expectedIds() {
        return LongStream.range(0, RECORDS).boxed().collect(Collectors.toList());
    }

    private static Instant instant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static final class KeyDeserializer
            implements SpannerChangeStreamDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<Long> out) {
            out.collect(
                    JsonParser.parseString(record.getMods().get(0).getKeysJson())
                            .getAsJsonObject()
                            .get("id")
                            .getAsLong());
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }

    private static final class FailOnce
            implements MapFunction<Long, Long>, CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private final Map<Long, Long> snapshots = new HashMap<>();
        private transient ListState<Long> highestSeenState;
        private long highestSeen = -1;

        @Override
        public Long map(Long value) throws InterruptedException {
            highestSeen = Math.max(highestSeen, value);
            if (!FAILED.get()) {
                Thread.sleep(25);
            }
            if (CHECKPOINT_AFTER_THRESHOLD.get() && FAILED.compareAndSet(false, true)) {
                throw new IllegalStateException("Injected failure at " + value);
            }
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            highestSeenState.update(Collections.singletonList(highestSeen));
            snapshots.put(context.getCheckpointId(), highestSeen);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            highestSeenState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "highest-seen", LongSerializer.INSTANCE));
            if (context.isRestored()) {
                for (Long restored : highestSeenState.get()) {
                    highestSeen = Math.max(highestSeen, restored);
                }
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            Long checkpointed = snapshots.remove(checkpointId);
            if (checkpointed != null && checkpointed >= FAIL_AFTER) {
                FAILURE_CHECKPOINT_HIGHEST.compareAndSet(-1, checkpointed);
                CHECKPOINT_AFTER_THRESHOLD.set(true);
            }
            snapshots.keySet().removeIf(id -> id < checkpointId);
        }
    }

    private static final class Seeded {
        private final DatabaseDestination database;
        private final Instant start;
        private final Instant end;

        private Seeded(DatabaseDestination database, Instant start, Instant end) {
            this.database = database;
            this.start = start;
            this.end = end;
        }
    }
}
