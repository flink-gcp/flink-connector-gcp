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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fails one subtask of a running read and asserts what the recovery delivers.
 *
 * <p>What this exercises, measured from the enumerator's own log rather than assumed: a task
 * failure does <em>not</em> rebuild the split enumerator. The coordinator survives, the splits the
 * failed reader held come back through {@code addSplitsBack}, and they are handed out again — so
 * the job reads on at the snapshot it already planned. Rebuilding an enumerator from a checkpoint
 * is a different path, and it is covered where it can be driven exactly, in {@code
 * SpannerBatchReadSplitEnumeratorTest}.
 *
 * <p>The two halves of the recovery contract are both asserted. Every row the read covers arrives —
 * that is the completeness half — and some rows arrive twice, because a partition is the unit of
 * re-reading and Spanner exposes no position inside one. The duplicate assertion is the control: a
 * source that resumed part-way through a partition would pass the first half and fail this one, and
 * a run that never actually failed would fail it too.
 */
class SpannerSourceFailoverITCase extends AbstractSpannerEmulatorITCase {

    /**
     * Enough rows that the source cannot have finished before the failure lands.
     *
     * <p>Sized against the reader's element queue rather than picked: the queue holds two fetches
     * and a fetch holds a thousand rows, so a table of a few thousand can be read whole into memory
     * while the map is still on its first hundred — and the duplicate assertion below, which is
     * this test's control, would then have nothing to observe.
     */
    private static final int ROWS = 10_000;

    /** How far into the stream the failure lands, well inside what the source is still reading. */
    private static final int FAIL_AFTER = 200;

    /** How long each of the first records costs, which is what holds the pipeline open. */
    private static final long RECORD_DELAY_MILLIS = 2;

    /**
     * Whether the failure has already been thrown.
     *
     * <p>Static because the map function is serialized into the job graph and the MiniCluster runs
     * it in this JVM; a field would be reset on the restore and the job would never finish.
     */
    private static final AtomicBoolean FAILED = new AtomicBoolean();

    @Test
    void aFailedSubtaskRecoversDeliveringEveryRowAndRepeatingSome() throws Exception {
        FAILED.set(false);
        DatabaseDestination database = seededDatabase();

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        // The reader's own state has to be checkpointable for the recovery to be the one a
        // real job takes; nothing here asserts on a particular checkpoint completing.
        env.enableCheckpointing(100);

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<Long> collected =
                env.fromSource(
                                SpannerSource.<Long>builder()
                                        .database(database)
                                        .readOperation(
                                                SpannerReadOperation.query(
                                                        Statement.of("SELECT id FROM singers")))
                                        .deserializer(new TestSources.IdDeserializer())
                                        .emulatorEndpoint(emulatorEndpoint())
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "spanner")
                        .map(new FailOnce())
                        .executeAndCollect()) {
            collected.forEachRemaining(ids::add);
        }

        assertThat(FAILED).isTrue();
        assertThat(ids).containsAll(expectedIds());
        assertThat(ids.size())
                .as("a returned partition is read again from its start, so rows repeat")
                .isGreaterThan(ROWS);
    }

    /**
     * Throws once, part way through the stream, on whichever subtask gets there first.
     *
     * <p>The records around the failure each cost a few milliseconds, which is what keeps the
     * source still reading when the failure lands. Without that the table is read into the reader's
     * element queue first, the splits are already finished, and nothing comes back through {@code
     * addSplitsBack} for the duplicate assertion to observe.
     */
    private static final class FailOnce implements MapFunction<Long, Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long map(Long value) throws InterruptedException {
            if (value < FAIL_AFTER * 2L) {
                // Only the records around the failure are slowed, which keeps the rest of the run
                // — and the whole of the re-read after it — at full speed.
                Thread.sleep(RECORD_DELAY_MILLIS);
            }
            if (value >= FAIL_AFTER && FAILED.compareAndSet(false, true)) {
                throw new IllegalStateException("Injected failure at " + value);
            }
            return value;
        }
    }

    private static DatabaseDestination seededDatabase() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (id)");
        List<Mutation> rows = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            rows.add(
                    Mutation.newInsertOrUpdateBuilder("singers")
                            .set("id")
                            .to(id)
                            .set("name")
                            .to("singer-" + id)
                            .build());
            // Written in batches, because one commit is bounded by Spanner's own mutation limit.
            if (rows.size() == 500) {
                client(database).write(rows);
                rows.clear();
            }
        }
        if (!rows.isEmpty()) {
            client(database).write(rows);
        }
        return database;
    }

    private static List<Long> expectedIds() {
        return LongStream.range(0, ROWS).boxed().collect(Collectors.toList());
    }
}
