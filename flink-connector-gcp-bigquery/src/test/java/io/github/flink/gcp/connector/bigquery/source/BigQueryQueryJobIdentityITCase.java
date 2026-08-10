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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the Flink runtime property the query source's deterministic job id rests on: a split
 * enumerator can read the job's name out of {@code
 * SplitEnumeratorContext#metricGroup()#getAllVariables()}, and a re-planned enumerator reads the
 * same name.
 *
 * <p>{@code SplitEnumeratorContext} carries no first-class job identity at all, so the metric
 * variables are the only route from an enumerator to the name the user gave {@code
 * StreamExecutionEnvironment#execute(String)} — issue #477 flagged relying on that as "a source
 * reading, not a measurement", and this class is the measurement. It runs against a real local job,
 * not a fake context, because the shared {@code FakeSplitEnumeratorContext} answers an empty
 * variables map and would prove nothing.
 *
 * <p>The recreation half matters as much as the presence half, and <em>which failures recreate an
 * enumerator at all</em> is this class's other finding (measured 2026-08-10, Flink 2.2.1): a task
 * failure — under the default {@code region} failover strategy and under {@code full} alike —
 * restarts tasks but keeps the operator coordinator, and so the enumerator and its query, alive;
 * only the global-restore path resets coordinators ({@code
 * OperatorCoordinatorRestoreBehavior.RESTORE_OR_RESET}), which with no completed checkpoint builds
 * a fresh enumerator from {@code Source#createEnumerator}. That path is reached by a JobManager
 * failover — the failure #477 is about — and by a coordinator-reported failure ({@code
 * SplitEnumeratorContext#failJob} escalates to {@code handleGlobalFailure}), which is how this test
 * drives it in one JVM: the probe's enumerator throws once from a coordinator action.
 *
 * <p>The probe deliberately checkpoints nothing, which is exactly the pre-first-checkpoint window
 * the deterministic job id exists for.
 *
 * <p>Kept as a permanent test rather than a one-off probe so the weekly Flink version matrix
 * re-verifies the assumption on every supported version.
 */
@Timeout(180)
class BigQueryQueryJobIdentityITCase {

    /** The variables each enumerator instance observed, keyed by the test run. */
    private static final Map<String, List<Map<String, String>>> OBSERVED =
            new ConcurrentHashMap<>();

    /** The runs whose one deliberate failure has already fired. */
    private static final Set<String> FAILED = ConcurrentHashMap.newKeySet();

    @Test
    void theJobNameIsReadableFromTheEnumeratorAndSurvivesAGlobalFailover() throws Exception {
        String runId = "named-" + System.nanoTime();
        String jobName = "query-job-identity-probe";

        execute(runId, jobName);

        List<Map<String, String>> observed = OBSERVED.get(runId);
        // Two enumerators: the initial one, and the one the global failover recreated. Fewer
        // means the deliberate failure never fired and the recreation half measured nothing.
        assertThat(observed)
                .as("one enumerator before the failure and one after the global failover")
                .hasSize(2);
        for (Map<String, String> variables : observed) {
            assertThat(variables)
                    .as("the enumerator's metric variables carry the job name")
                    .containsEntry("<job_name>", jobName);
        }
    }

    @Test
    void aJobLeftUnnamedStillPutsANameInTheVariables() throws Exception {
        String runId = "default-" + System.nanoTime();

        execute(runId, null);

        List<Map<String, String>> observed = OBSERVED.get(runId);
        assertThat(observed).hasSize(2);
        for (Map<String, String> variables : observed) {
            // The exact default is Flink's to choose; what the query source's id needs is only
            // that some stable name is always there.
            assertThat(variables.get("<job_name>"))
                    .as("a defaulted job name is still present and non-empty")
                    .isNotNull()
                    .isNotEmpty();
        }
        assertThat(observed.get(0).get("<job_name>"))
                .as("the defaulted name is the same after the failover")
                .isEqualTo(observed.get(1).get("<job_name>"));
    }

    private static void execute(String runId, @Nullable String jobName) throws Exception {
        Configuration configuration = new Configuration();
        // One restart: the injected global failure recovers, anything further fails the test.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        // No checkpointing, deliberately: the recovery then rebuilds the enumerator from
        // scratch, which is the pre-first-checkpoint window the deterministic job id exists for.

        env.fromSource(
                        new VariablesRecordingSource(runId, 0, 99),
                        WatermarkStrategy.noWatermarks(),
                        "probe")
                .sinkTo(new DiscardingSink<>());
        if (jobName == null) {
            env.execute();
        } else {
            env.execute(jobName);
        }
    }

    /**
     * A {@link NumberSequenceSource} whose enumerators record the metric variables they can see,
     * and whose first enumerator fails the job from a coordinator action.
     *
     * <p>Subclassing Flink's own bounded source keeps the probe honest: the enumerator context it
     * records from is the real coordinator's, built exactly as it would be for the BigQuery source,
     * with no harness in between. The failure is thrown from {@code handleSplitRequest} — inside
     * the coordinator — because that is what escalates to a global failover; a task-side failure
     * restarts tasks around a surviving enumerator and would leave the recreation half of this test
     * measuring nothing (see the class javadoc).
     */
    private static final class VariablesRecordingSource extends NumberSequenceSource {

        private static final long serialVersionUID = 1L;

        private final String runId;

        VariablesRecordingSource(String runId, long from, long to) {
            super(from, to);
            this.runId = runId;
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                createEnumerator(SplitEnumeratorContext<NumberSequenceSplit> context) {
            record(context);
            return new FailOnceEnumerator(runId, super.createEnumerator(context));
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                restoreEnumerator(
                        SplitEnumeratorContext<NumberSequenceSplit> context,
                        Collection<NumberSequenceSplit> checkpoint) {
            record(context);
            return new FailOnceEnumerator(runId, super.restoreEnumerator(context, checkpoint));
        }

        private void record(SplitEnumeratorContext<NumberSequenceSplit> context) {
            // Synchronized because the two recordings come from two coordinator instances, each
            // on its own thread.
            OBSERVED.computeIfAbsent(
                            runId, unused -> Collections.synchronizedList(new ArrayList<>()))
                    .add(new HashMap<>(context.metricGroup().getAllVariables()));
        }
    }

    /** Delegates to the real enumerator, throwing once per run from a coordinator action. */
    private static final class FailOnceEnumerator
            implements SplitEnumerator<
                    NumberSequenceSource.NumberSequenceSplit,
                    Collection<NumberSequenceSource.NumberSequenceSplit>> {

        private final String runId;

        private final SplitEnumerator<
                        NumberSequenceSource.NumberSequenceSplit,
                        Collection<NumberSequenceSource.NumberSequenceSplit>>
                delegate;

        FailOnceEnumerator(
                String runId,
                SplitEnumerator<
                                NumberSequenceSource.NumberSequenceSplit,
                                Collection<NumberSequenceSource.NumberSequenceSplit>>
                        delegate) {
            this.runId = runId;
            this.delegate = delegate;
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            if (FAILED.add(runId)) {
                throw new IllegalStateException("deliberate failure to force a global failover");
            }
            delegate.handleSplitRequest(subtaskId, requesterHostname);
        }

        @Override
        public void addSplitsBack(
                List<NumberSequenceSource.NumberSequenceSplit> splits, int subtaskId) {
            delegate.addSplitsBack(splits, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            delegate.addReader(subtaskId);
        }

        @Override
        public Collection<NumberSequenceSource.NumberSequenceSplit> snapshotState(long checkpointId)
                throws Exception {
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
