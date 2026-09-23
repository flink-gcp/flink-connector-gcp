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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import io.github.flink.gcp.connector.cloudtasks.CloudTasksMetricNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The probe against an in-process Cloud Tasks server: one cell, one JVM, no emulator.
 *
 * <p>What it is really asserting is that the gauges reach the reader, and that a run which did not
 * measure says so. The cluster harness reported an empty metric listing on every cell of a whole
 * run and called all ten observations; the two failure cases below are the shapes that produced.
 *
 * <p>The timeout is the outer bound, and it is one the probe cannot outrun: {@code deadline}
 * abandons a wedged cell of this size after a minute. JUnit evaluates a {@code SAME_THREAD} timeout
 * only once the method returns, so a longer probe deadline than this would make the annotation
 * documentation rather than a control.
 */
@Timeout(300)
@Execution(ExecutionMode.SAME_THREAD)
final class CloudTasksLeanProbeITCase {
    /**
     * The gauges the committer sets when a commit starts and clears in that commit's {@code
     * finally}. A commit of this cell's size may be shorter than the reader's poll, so even a
     * reader that folds every poll can see only the {@code -1} they report when idle. Missing one
     * of these is not a defect; missing any of the others is, because the rest stay live for most
     * of each checkpoint interval whatever that interval is.
     */
    private static final List<String> COMMIT_SCOPED =
            List.of(
                    CloudTasksMetricNames.CURRENT_COMMIT_OLDEST_TASK_AGE_MILLIS,
                    CloudTasksMetricNames.CURRENT_COMMIT_REPLAY_BUDGET_MILLIS);

    @TempDir Path temporary;

    private MeasurementOptions options(String cell, MeasurementOptions.Arm arm) throws Exception {
        Path evidence = Files.createDirectories(temporary.resolve(cell + "-evidence"));
        return MeasurementOptions.parse(
                MeasurementOptionsTest.windowArguments(
                        "--arm", arm.name(),
                        "--cell-id", cell,
                        "--parallelism", "1",
                        "--concurrency", "1",
                        "--offered-rate", "50",
                        "--warmup-seconds", "1",
                        "--observation-seconds", "1",
                        "--record-limit", "500",
                        "--attempt-limit", "750",
                        "--evidence-root", evidence.toUri().toString()));
    }

    @ParameterizedTest
    @EnumSource(MeasurementOptions.Arm.class)
    void probeRunsOneCellAndReadsTheSinksOwnGauges(MeasurementOptions.Arm arm) throws Exception {
        String cell = "probe-" + arm.name().toLowerCase(Locale.ROOT).replace('_', '-');
        var options = options(cell, arm);
        try (FakeCloudTasks server = new FakeCloudTasks()) {
            String line =
                    CloudTasksLeanProbe.run(temporary.resolve(cell), server.endpoint(), options);

            String[] fields = line.split(",", -1);
            assertThat(fields).as(line).hasSize(13);
            assertThat(fields[0]).isEqualTo(CloudTasksLeanProbe.PREFIX);
            assertThat(fields[1]).isEqualTo(options.runId);
            assertThat(fields[2]).isEqualTo(cell);
            assertThat(fields[3]).isEqualTo(arm.name());
            // The reason field carries why a run failed, so assert it before the status.
            assertThat(fields[5]).as("reason in %s", line).isEqualTo("-");
            assertThat(fields[4]).isEqualTo("OBSERVATION");
            assertThat(fields[12]).as("teardown in %s", line).isEqualTo("-");
            assertThat(Long.parseLong(fields[7])).as("samples in %s", line).isPositive();
            assertThat(Long.parseLong(fields[8])).as("heap peak in %s", line).isPositive();

            assertThat(CloudTasksLeanProbe.required(arm)).isNotEmpty();
            for (String gauge : CloudTasksLeanProbe.required(arm)) {
                boolean extreme = fields[10].contains(gauge + "=");
                boolean unobserved = List.of(fields[11].split(";")).contains(gauge);
                // Every required gauge is on exactly one side: a reading caught a real value and
                // it has an extreme, or none did and the line says which. Both, or neither, means
                // a name went missing between the reader and the summary.
                assertThat(extreme)
                        .as("%s in extremes xor unobserved, in %s", gauge, line)
                        .isNotEqualTo(unobserved);
                // A commit-scoped gauge is live only inside a commit, which a reader polling
                // ten times a second still legitimately misses. The ones that stay live across a
                // checkpoint interval have no such excuse.
                if (!COMMIT_SCOPED.contains(gauge)) {
                    assertThat(extreme).as("%s has an extreme in %s", gauge, line).isTrue();
                }
            }
            List<String> samples =
                    Files.readAllLines(temporary.resolve(cell).resolve("samples.jsonl"));
            assertThat(samples).hasSizeGreaterThanOrEqualTo(2);
            // Every line carries the process figures, which have no lifetime of their own. The
            // gauges are asserted across the file rather than on the last line: a line written
            // between the job finishing and the loop noticing is taken after the operators have
            // closed and unregistered, and carries an empty `gauges` object through no fault of
            // the reader.
            // Matched as the rendered key, not as a bare substring: a future metric whose name
            // contains this one would otherwise satisfy it.
            String named = "\"" + CloudTasksLeanProbe.required(arm).get(0) + "\":";
            assertThat(samples).allMatch(sample -> sample.contains("\"heapUsed\":"));
            // `anyMatch` rather than a share of the lines, deliberately: the first line is written
            // before the operators are deployed and one or more trailing lines after they close,
            // so neither end is reliably gauge-bearing and any threshold would be a new race. That
            // a real value was caught is asserted far more strictly above, where each required
            // gauge must hold an extreme.
            //
            // Holding an extreme no longer implies appearing on a line, now that the fold runs ten
            // times per line. This gauge is safe from that: each arm's first required name is a
            // count -- staged or in-flight tasks -- which is never the idle sentinel the reader
            // drops, so it is in every reading taken while the writer is registered, and the run
            // is several lines long.
            assertThat(samples).anyMatch(sample -> sample.contains(named));
            // The cell runs for several one-second checkpoint intervals; each line is a completion
            // the gauge reported, in increasing id order.
            List<String> checkpoints =
                    Files.readAllLines(temporary.resolve(cell).resolve("checkpoints.jsonl"));
            assertThat(checkpoints).isNotEmpty();
            long previous = 0;
            for (String checkpoint : checkpoints) {
                assertThat(checkpoint).matches("\\{\"id\":[0-9]+,\"completedMillis\":[0-9]+}");
                long id = Long.parseLong(checkpoint.replaceAll("\\{\"id\":([0-9]+),.*", "$1"));
                assertThat(id).isGreaterThan(previous);
                previous = id;
            }
            assertThat(server.accepted()).isNotEmpty();
        }
    }

    @Test
    void aGaugeThatNeverReachedTheReaderFailsTheRunAndNamesIt() throws Exception {
        // The defect the whole change exists for: run cal1246c-221-1 got no staged gauges on any
        // of its ten cells and reported every one as an observation. Here the same absence has to
        // reach the line an operator reads, on an otherwise healthy job.
        String cell = "probe-absent";
        var options = options(cell, MeasurementOptions.Arm.STAGED_HASH);
        try (FakeCloudTasks server = new FakeCloudTasks()) {
            String line =
                    CloudTasksLeanProbe.run(
                            temporary.resolve(cell),
                            server.endpoint(),
                            options,
                            Set.of(CloudTasksMetricNames.STAGED_BYTES));

            String[] fields = line.split(",", -1);
            assertThat(fields).as(line).hasSize(13);
            assertThat(fields[5])
                    .as("reason in %s", line)
                    .isEqualTo("gauges absent: " + CloudTasksMetricNames.STAGED_BYTES);
            assertThat(fields[4]).isEqualTo("FAILED");
            // The job itself was healthy: it ran, sampled, and the server took its tasks.
            assertThat(Long.parseLong(fields[7])).as("samples in %s", line).isPositive();
            assertThat(server.accepted()).isNotEmpty();
            // A gauge that did reach the reader is still reported, so the line locates the hole.
            assertThat(fields[10]).contains(CloudTasksMetricNames.STAGED_TASKS + "=");
            // Never registered is the reason's fact, not this field's; naming it in both would
            // leave a reader unable to tell which of the two this column is asserting.
            assertThat(fields[11])
                    .as("unobserved in %s", line)
                    .doesNotContain(CloudTasksMetricNames.STAGED_BYTES);
        }
    }

    @Test
    void aJobThatDiesIsReportedFailedWithTheCauseRatherThanAsAnObservation() throws Exception {
        // Nothing here is deaf and every gauge registers, so only the job's own failure can make
        // this FAILED. Without the final result.get() the sampling loop simply stops and the run
        // reports a full set of samples as an observation — a dead cell counted as a good one,
        // which is the cal1246c-221-1 shape arriving by the other route.
        String cell = "probe-dead-server";
        var options = options(cell, MeasurementOptions.Arm.STAGED_HASH);
        // A port nothing is listening on: every CreateTask fails and the sink gives up.
        String unreachable;
        try (FakeCloudTasks server = new FakeCloudTasks()) {
            unreachable = server.endpoint();
        }

        String line = CloudTasksLeanProbe.run(temporary.resolve(cell), unreachable, options);

        String[] fields = line.split(",", -1);
        assertThat(fields).as(line).hasSize(13);
        assertThat(fields).as(line).hasSize(13);
        assertThat(fields[4]).as(line).isEqualTo("FAILED");
        assertThat(fields[5])
                .as("reason in %s", line)
                .doesNotStartWith("gauges absent")
                // The innermost cause. Every Flink job failure wraps as "Job execution failed.",
                // so without the root-cause walk two unrelated dead cells read identically.
                .doesNotContain("Job execution failed")
                .contains("UNAVAILABLE");
        // The connector's message carries commas; a summary that kept them would move every
        // field after this one, and the size assertion above is what notices.
        assertThat(fields[5]).doesNotContain(";");
        // A run that failed after measuring still reports what it measured, rather than
        // contradicting the samples.jsonl sitting beside it.
        assertThat(Long.parseLong(fields[7])).as("samples in %s", line).isPositive();
        assertThat(Files.readAllLines(temporary.resolve(cell).resolve("samples.jsonl")))
                .hasSize((int) Long.parseLong(fields[7]));
    }
}
