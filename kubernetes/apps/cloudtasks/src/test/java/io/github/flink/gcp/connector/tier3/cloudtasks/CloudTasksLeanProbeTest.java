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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** The probe's pure decisions, which the integration case exercises but does not pin. */
class CloudTasksLeanProbeTest {

    private static final long SAMPLE_NANOS = 1_000_000_000L;

    private static Map<String, SinkGaugeReporter.Reading> reading(String name, long value) {
        return Map.of(name, SinkGaugeReporter.Reading.of(List.of(value)));
    }

    @Test
    void onlyTheCheckpointsTheGaugeReportedAreWrittenEachOnce() throws IOException {
        StringWriter file = new StringWriter();

        long none = CloudTasksLeanProbe.completions(file, 0, null, 1_000);
        long first =
                CloudTasksLeanProbe.completions(
                        file, none, SinkGaugeReporter.Reading.of(List.of(1L)), 2_000);
        long same =
                CloudTasksLeanProbe.completions(
                        file, first, SinkGaugeReporter.Reading.of(List.of(1L)), 3_000);
        // A jump past id 2: it may have failed or expired, so only the id reported is written.
        long jump =
                CloudTasksLeanProbe.completions(
                        file, same, SinkGaugeReporter.Reading.of(List.of(3L)), 4_000);

        assertThat(List.of(none, first, same, jump)).containsExactly(0L, 1L, 1L, 3L);
        assertThat(file.toString())
                .isEqualTo(
                        "{\"id\":1,\"completedMillis\":2000}\n"
                                + "{\"id\":3,\"completedMillis\":4000}\n");
    }

    @Test
    void aPollThatWritesNoLineStillReachesTheExtremes() throws IOException {
        // The defect this pins: the gauges a staged arm publishes are live for part of each
        // checkpoint interval, and a reader that folded only when it wrote a line held one phase
        // against the drain for a whole run. Here the live reading arrives on a poll that is not
        // a line's turn, which is exactly the poll the old loop threw away.
        Map<String, Long> extremes = new LinkedHashMap<>();
        var cost = new CloudTasksLeanProbe.Cost();
        StringWriter file = new StringWriter();
        long next = 1_000_000_000L;

        long after =
                CloudTasksLeanProbe.tick(
                        extremes,
                        cost,
                        file,
                        reading(CloudTasksMetricNames.STAGED_TASKS, 47),
                        0,
                        next - 1,
                        next);

        assertThat(extremes).containsEntry(CloudTasksMetricNames.STAGED_TASKS, 47L);
        // The line's turn has not come, so nothing was written and nothing was counted.
        assertThat(after).isEqualTo(next);
        assertThat(file.toString()).isEmpty();
        assertThat(cost.samples).isZero();
    }

    @Test
    void aPollThatWritesALineFoldsTheSameReadingItRenders() throws IOException {
        Map<String, Long> extremes = new LinkedHashMap<>();
        var cost = new CloudTasksLeanProbe.Cost();
        StringWriter file = new StringWriter();

        long after =
                CloudTasksLeanProbe.tick(
                        extremes,
                        cost,
                        file,
                        reading(CloudTasksMetricNames.STAGED_TASKS, 12),
                        5,
                        1_000L,
                        0L);

        assertThat(extremes).containsEntry(CloudTasksMetricNames.STAGED_TASKS, 12L);
        assertThat(cost.samples).isOne();
        assertThat(file.toString())
                .contains("\"" + CloudTasksMetricNames.STAGED_TASKS + "\":")
                .contains("\"sum\":12")
                .contains("\"elapsedNanos\":5")
                .endsWith("\n");
        // Advanced from the schedule, so a slow poll does not stretch the period.
        assertThat(after).isEqualTo(SAMPLE_NANOS);
    }

    @Test
    void aLineDueLongAgoIsResynchronisedRatherThanEmittedAsABacklog() throws IOException {
        Map<String, Long> extremes = new LinkedHashMap<>();
        var cost = new CloudTasksLeanProbe.Cost();
        StringWriter file = new StringWriter();
        // A pause of several periods: the schedule is behind by more than one line.
        long now = 10 * SAMPLE_NANOS;

        long after =
                CloudTasksLeanProbe.tick(
                        extremes,
                        cost,
                        file,
                        reading(CloudTasksMetricNames.STAGED_TASKS, 1),
                        now,
                        now,
                        SAMPLE_NANOS);

        assertThat(cost.samples).isOne();
        assertThat(after).isEqualTo(now);
    }

    @ParameterizedTest
    @EnumSource(MeasurementOptions.Arm.class)
    void anArmOnlyRequiresTheGaugesItsOwnWriterRegisters(MeasurementOptions.Arm arm) {
        boolean staged =
                arm == MeasurementOptions.Arm.STAGED_HASH
                        || arm == MeasurementOptions.Arm.STAGED_RANDOM;
        assertThat(CloudTasksLeanProbe.required(arm))
                .containsExactlyElementsOf(
                        staged
                                ? List.of(
                                        "stagedTasks",
                                        "stagedBytes",
                                        "oldestStagedTaskAgeMillis",
                                        "stagedReplayBudgetMillis",
                                        "currentCommitOldestTaskAgeMillis",
                                        "currentCommitReplayBudgetMillis")
                                : List.of("inFlightTasks", "parkedTasks"));
    }

    @Test
    void everyRequiredGaugeIsTrackedInTheDirectionItsMeaningRuns() {
        // track() classifies by suffix, so it is asserted against the metric constants: renaming
        // one out of its suffix would silently report a budget at its largest, which is the most
        // reassuring number available and the wrong one.
        assertThat(CloudTasksLeanProbe.track(CloudTasksMetricNames.STAGED_REPLAY_BUDGET_MILLIS))
                .isEqualTo(CloudTasksLeanProbe.Track.LOW);
        assertThat(
                        CloudTasksLeanProbe.track(
                                CloudTasksMetricNames.CURRENT_COMMIT_REPLAY_BUDGET_MILLIS))
                .isEqualTo(CloudTasksLeanProbe.Track.LOW);
        assertThat(CloudTasksLeanProbe.track(CloudTasksMetricNames.OLDEST_STAGED_TASK_AGE_MILLIS))
                .isEqualTo(CloudTasksLeanProbe.Track.HIGH);
        assertThat(
                        CloudTasksLeanProbe.track(
                                CloudTasksMetricNames.CURRENT_COMMIT_OLDEST_TASK_AGE_MILLIS))
                .isEqualTo(CloudTasksLeanProbe.Track.HIGH);
        for (String total :
                List.of(
                        CloudTasksMetricNames.STAGED_TASKS,
                        CloudTasksMetricNames.STAGED_BYTES,
                        CloudTasksMetricNames.IN_FLIGHT_TASKS,
                        CloudTasksMetricNames.PARKED_TASKS)) {
            assertThat(CloudTasksLeanProbe.track(total))
                    .as(total)
                    .isEqualTo(CloudTasksLeanProbe.Track.TOTAL);
        }
    }

    @Test
    void anExtremeKeepsTheWorstMomentForItsOwnDirection() {
        Map<String, Long> extremes = new LinkedHashMap<>();
        for (long value : List.of(900L, 300L, 700L)) {
            CloudTasksLeanProbe.keep(
                    extremes,
                    CloudTasksMetricNames.STAGED_REPLAY_BUDGET_MILLIS,
                    new SinkGaugeReporter.Reading(value, value, value));
            CloudTasksLeanProbe.keep(
                    extremes,
                    CloudTasksMetricNames.OLDEST_STAGED_TASK_AGE_MILLIS,
                    new SinkGaugeReporter.Reading(value, value, value));
            CloudTasksLeanProbe.keep(
                    extremes,
                    CloudTasksMetricNames.STAGED_TASKS,
                    new SinkGaugeReporter.Reading(value, 0, value));
        }
        assertThat(extremes)
                .containsEntry(CloudTasksMetricNames.STAGED_REPLAY_BUDGET_MILLIS, 300L)
                .containsEntry(CloudTasksMetricNames.OLDEST_STAGED_TASK_AGE_MILLIS, 900L)
                .containsEntry(CloudTasksMetricNames.STAGED_TASKS, 900L);
    }

    @Test
    void aBudgetExtremeIsTheSmallestAcrossSubtasksAndAnAgeTheLargest() {
        Map<String, Long> extremes = new LinkedHashMap<>();
        // One reading, four subtasks: the budget's worst is the lowest of them, the age's the
        // highest, and the count's the total rather than any one subtask's.
        CloudTasksLeanProbe.keep(
                extremes,
                CloudTasksMetricNames.STAGED_REPLAY_BUDGET_MILLIS,
                new SinkGaugeReporter.Reading(2000, 200, 900));
        CloudTasksLeanProbe.keep(
                extremes,
                CloudTasksMetricNames.OLDEST_STAGED_TASK_AGE_MILLIS,
                new SinkGaugeReporter.Reading(2000, 200, 900));
        CloudTasksLeanProbe.keep(
                extremes,
                CloudTasksMetricNames.STAGED_TASKS,
                new SinkGaugeReporter.Reading(2000, 200, 900));
        assertThat(extremes)
                .containsEntry(CloudTasksMetricNames.STAGED_REPLAY_BUDGET_MILLIS, 200L)
                .containsEntry(CloudTasksMetricNames.OLDEST_STAGED_TASK_AGE_MILLIS, 900L)
                .containsEntry(CloudTasksMetricNames.STAGED_TASKS, 2000L);
    }

    @Test
    void theDeadlineScalesWithTheCellAndNeverDropsBelowAMinute() {
        var brief =
                MeasurementOptions.parse(
                        MeasurementOptionsTest.windowArguments(
                                "--warmup-seconds", "1",
                                "--observation-seconds", "1",
                                "--record-limit", "500",
                                "--attempt-limit", "750"));
        assertThat(CloudTasksLeanProbe.deadline(brief)).isEqualTo(java.time.Duration.ofSeconds(60));

        var wide =
                MeasurementOptions.parse(
                        MeasurementOptionsTest.windowArguments(
                                "--warmup-seconds", "120", "--observation-seconds", "600"));
        // Four times the window, so a cluster-sized cell keeps a ceiling proportionate to it.
        assertThat(CloudTasksLeanProbe.deadline(wide))
                .isEqualTo(java.time.Duration.ofSeconds(2880));
    }

    @Test
    void aRecordCountCellWithATinyRateIsClampedRatherThanWaitingForYears() {
        // `--offered-rate` has no lower bound beyond being positive, so records/rate is
        // unbounded above. Without the six-hour clamp the deadline stops being a control: at this
        // rate the raw span is over a day, and a wedged cell would wait it out.
        var crawling =
                MeasurementOptions.parse(
                        MeasurementOptionsTest.arguments(
                                "--records", "100", "--offered-rate", "0.001"));

        assertThat(CloudTasksLeanProbe.deadline(crawling))
                .isEqualTo(java.time.Duration.ofHours(24));
    }

    @Test
    void aFailureReportsItsInnermostCauseWithNothingThatWouldAddAField() {
        // Every Flink job failure arrives wrapped; the outermost message is the same sentence
        // whatever went wrong, so the summary would not distinguish two failed cells.
        Exception wrapped =
                new ExecutionException(
                        "Job execution failed.",
                        new IllegalStateException("staging cap exceeded: 10,000; see docs\r\n"));
        assertThat(CloudTasksLeanProbe.safe(wrapped))
                // Comma, semicolon and newline all become spaces: each would otherwise add a
                // field to a line every consumer reads by position.
                .isEqualTo("IllegalStateException: staging cap exceeded: 10 000  see docs  ")
                .doesNotContain(",")
                .doesNotContain(";")
                .doesNotContain("\n");
    }

    @Test
    void aFailureWithNoMessageStillSaysWhatItWas() {
        assertThat(CloudTasksLeanProbe.safe(new IOException()))
                .isEqualTo("IOException: no message");
    }

    @Test
    void aCyclicCauseChainTerminates() {
        Exception outer = new IllegalStateException("outer");
        Exception inner = new IllegalStateException("inner", outer);
        outer.initCause(inner);
        assertThat(CloudTasksLeanProbe.safe(outer)).contains("IllegalStateException");
    }

    @Test
    void anAbsentGaugeReasonNamesEveryOneOfThem() {
        assertThat(CloudTasksLeanProbe.absent(List.of("stagedBytes", "stagedTasks")))
                .isEqualTo("gauges absent: stagedBytes stagedTasks");
    }
}
