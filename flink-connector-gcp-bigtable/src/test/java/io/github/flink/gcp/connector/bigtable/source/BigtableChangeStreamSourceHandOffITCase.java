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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceRegionFailoverITCase.KeepRestoredSplit;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceRegionFailoverITCase.RowKeys;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceRegionFailoverITCase.WholeTableClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.RestoreScriptedChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.RestoreScriptedChangeStreamOpener.Run;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.RestoreScriptedChangeStreamOpener.Step;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceRegionFailoverITCase.awaitRunning;
import static io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceRegionFailoverITCase.status;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a partition split through Flink's own source coordinator at parallelism 2 (#2194).
 *
 * <p>Partition P0 emits {@code P0#1} and splits into C1 and C2, which emit {@code C1#2} and {@code
 * C2#3}. An operator behind the source, in its own task, keeps {@code P0#1} until a successor's
 * record has reached the keyed operator, or for a few seconds after the test lets P0 close when
 * none does: that stands in for a parent's last record still in a network buffer when the partition
 * closes. The keyed operator puts every record under one key, so its arrival order is one key's
 * order, and it logs each arrival into the script's log beside the opener's {@code open} entries.
 * The emulator does not implement Change Streams, so a scripted stream plays the service's part.
 */
@Timeout(180)
class BigtableChangeStreamSourceHandOffITCase {

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final ByteStringRange WHOLE = ByteStringRange.unbounded();
    private static final ByteStringRange LEFT = ByteStringRange.unbounded().endOpen("m");
    private static final ByteStringRange RIGHT = ByteStringRange.unbounded().startClosed("m");

    private static final String PARENT_RECORD = "P0#1";
    private static final String HOLD_WARNING = "successors start at once";

    /** How long the delaying operator keeps the parent's record once the test lets P0 close. */
    private static final Duration PARENT_DELAY = Duration.ofSeconds(5);

    private static final Map<String, Run> SCRIPTS = new ConcurrentHashMap<>();
    private static final Map<String, List<String>> ARRIVALS = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, Long>> ARRIVAL_NANOS = new ConcurrentHashMap<>();
    private static final Map<String, Long> CLOSE_RELEASED_NANOS = new ConcurrentHashMap<>();

    @Test
    void anImmediateHandOffLetsASuccessorOvertakeItsParentsLastRecord(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        // Without checkpointing: an aligned barrier in flight blocks the successor's channel at the
        // keyed operator until the parent's record arrives, which would hide the race.
        Result result = run(cluster, PartitionHandoff.IMMEDIATE, false, Duration.ofMinutes(10));

        assertThat(result.arrivals).containsExactlyInAnyOrder(PARENT_RECORD, "C1#2", "C2#3");
        assertThat(result.successorsOpenedBeforeTheParentRecordArrived())
                .as(result.diagnosis)
                .isTrue();
        assertThat(result.arrivals.get(0)).as(result.diagnosis).isNotEqualTo(PARENT_RECORD);
    }

    @Test
    void aSuccessorWaitsForACheckpointAndCannotOvertakeItsParent(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        // Two seconds apart, so that no barrier is usually in flight while a successor's record
        // races the delayed one; the opens are compared too, which no barrier can reorder.
        Result result =
                run(cluster, PartitionHandoff.AFTER_CHECKPOINT, true, Duration.ofMinutes(10));

        assertThat(result.arrivals).containsExactlyInAnyOrder(PARENT_RECORD, "C1#2", "C2#3");
        // The checkpoint the successors wait for completes only after the parent's record has
        // passed the keyed operator, so neither successor opens before it arrives.
        assertThat(result.successorsOpenedBeforeTheParentRecordArrived())
                .as(result.diagnosis)
                .isFalse();
        assertThat(result.arrivals.get(0)).as(result.diagnosis).isEqualTo(PARENT_RECORD);
    }

    @Test
    void withoutCheckpointingTheSuccessorsStartAfterTheFirstCheckpointTimeout(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            Result result =
                    run(cluster, PartitionHandoff.AFTER_CHECKPOINT, false, Duration.ofSeconds(1));

            assertThat(result.arrivals).containsExactlyInAnyOrder(PARENT_RECORD, "C1#2", "C2#3");
            // Held for the timeout: no successor record arrives within a second of P0's release,
            // which the timeout counts from P0's close, later still.
            assertThat(result.millisFromReleaseTo("C1#2")).as(result.diagnosis).isGreaterThan(900);
            assertThat(result.millisFromReleaseTo("C2#3")).as(result.diagnosis).isGreaterThan(900);
            // The warning that names the successors is logged only while one is held, which also
            // shows that Flink ran the periodic check that released them.
            assertThat(capture.getMessages())
                    .filteredOn(m -> m.contains(HOLD_WARNING))
                    .singleElement()
                    .satisfies(m -> assertThat(m).contains("PT1S"));
        }
    }

    private static Result run(
            MiniCluster cluster,
            PartitionHandoff handoff,
            boolean checkpointing,
            Duration firstCheckpointTimeout)
            throws Exception {
        String run = UUID.randomUUID().toString();
        Run script =
                RestoreScriptedChangeStreamOpener.register(run)
                        .partition(
                                "P0",
                                WHOLE,
                                Step.data(1),
                                Step.await("close"),
                                Step.close("C1", "C2"))
                        .partition("C1", LEFT, Step.data(2), Step.close())
                        .partition("C2", RIGHT, Step.data(3), Step.close());
        SCRIPTS.put(run, script);
        List<String> arrivals = new CopyOnWriteArrayList<>();
        ARRIVALS.put(run, arrivals);
        ARRIVAL_NANOS.put(run, new ConcurrentHashMap<>());
        Supplier<String> diagnosis = () -> "log=" + script.log + " arrivals=" + arrivals;

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        if (checkpointing) {
            env.enableCheckpointing(2_000);
        }
        BigtableChangeStreamSource<String> source =
                BigtableChangeStreamSource.<String>builder()
                        .table(TableDestination.of("project", "instance", "table"))
                        .appProfileId("profile")
                        .deserializer(new RowKeys())
                        .startPosition(StartPosition.at(RestoreScriptedChangeStreamOpener.BASE))
                        .boundedTimestamp(Instant.now().plus(Duration.ofDays(365)))
                        .partitionHandoff(handoff)
                        .firstCheckpointTimeout(firstCheckpointTimeout)
                        .opener(new RestoreScriptedChangeStreamOpener(run))
                        .restoreResolver(new KeepRestoredSplit())
                        .coordinatorClientFactory(WholeTableClient::new)
                        .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "bigtable")
                .map(new DelayParentRecord(run))
                .disableChaining()
                .keyBy(value -> "one-key", Types.STRING)
                .map(new RecordArrival(run))
                .sinkTo(new DiscardingSink<>());

        JobClient job = env.executeAsync("hand-off");
        // Every reader has started, and so has told the coordinator its capacity, before P0
        // closes: the successors then go to both readers rather than wait for P0's reader alone.
        awaitRunning(cluster, job);
        Awaits.await(
                "P0 to emit its record",
                Duration.ofSeconds(30),
                () -> script.reached.contains("P0:close"),
                diagnosis);
        long released = System.nanoTime();
        CLOSE_RELEASED_NANOS.put(run, released);
        script.open("close");
        Awaits.await(
                "the job to finish",
                Duration.ofSeconds(120),
                () -> status(job) == JobStatus.FINISHED,
                diagnosis);
        return new Result(
                new ArrayList<>(arrivals),
                new ArrayList<>(script.log),
                ARRIVAL_NANOS.get(run),
                released,
                diagnosis.get());
    }

    /** What one run observed. */
    private static final class Result {

        private final List<String> arrivals;
        private final List<String> log;
        private final Map<String, Long> arrivalNanos;
        private final long releasedNanos;
        private final String diagnosis;

        private Result(
                List<String> arrivals,
                List<String> log,
                Map<String, Long> arrivalNanos,
                long releasedNanos,
                String diagnosis) {
            this.arrivals = arrivals;
            this.log = log;
            this.arrivalNanos = arrivalNanos;
            this.releasedNanos = releasedNanos;
            this.diagnosis = diagnosis;
        }

        private boolean successorsOpenedBeforeTheParentRecordArrived() {
            int parentArrival = log.indexOf("arrive " + PARENT_RECORD);
            int firstOpen = Math.min(index("open C1@0"), index("open C2@0"));
            return parentArrival >= 0 && firstOpen < parentArrival;
        }

        private int index(String entry) {
            int index = log.indexOf(entry);
            return index < 0 ? Integer.MAX_VALUE : index;
        }

        private long millisFromReleaseTo(String record) {
            return Duration.ofNanos(arrivalNanos.get(record) - releasedNanos).toMillis();
        }
    }

    /** Keeps the parent's record until a successor's record has arrived, or a few seconds. */
    private static final class DelayParentRecord implements MapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final String run;

        DelayParentRecord(String run) {
            this.run = run;
        }

        @Override
        public String map(String value) throws Exception {
            if (value.equals(PARENT_RECORD)) {
                // The delay counts from the test's release of P0's close, so a slow deployment
                // before it spends none of it.
                while (ARRIVALS.get(run).isEmpty()) {
                    Long released = CLOSE_RELEASED_NANOS.get(run);
                    if (released != null
                            && System.nanoTime() - released >= PARENT_DELAY.toNanos()) {
                        break;
                    }
                    Thread.sleep(10);
                }
            }
            return value;
        }
    }

    private static final class RecordArrival implements MapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final String run;

        RecordArrival(String run) {
            this.run = run;
        }

        @Override
        public String map(String value) {
            ARRIVAL_NANOS.get(run).put(value, System.nanoTime());
            SCRIPTS.get(run).log.add("arrive " + value);
            ARRIVALS.get(run).add(value);
            return value;
        }
    }
}
