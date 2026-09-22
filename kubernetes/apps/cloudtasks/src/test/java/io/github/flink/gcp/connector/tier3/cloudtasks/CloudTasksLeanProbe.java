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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.cloudtasks.CloudTasksMetricNames;

import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Runs one measurement cell in this JVM and records what it cost.
 *
 * <p>It reads the sink's own gauges through {@link SinkGaugeReporter} rather than asking Flink's
 * REST API what a vertex has. A gauge an arm needs and the sink never registered makes the run
 * {@code FAILED}; one the sink registered but no reading ever caught is reported in its own field,
 * because those are different facts and only the first is a defect.
 *
 * <p>Two artefacts come out. {@code samples.jsonl} carries one line per second with each gauge's
 * sum, minimum and maximum across subtasks beside the heap and the collector. The summary line
 * carries thirteen comma-separated fields, in order:
 *
 * <ol>
 *   <li>{@link #PREFIX}
 *   <li>run id
 *   <li>cell id
 *   <li>arm
 *   <li>{@code OBSERVATION} or {@code FAILED}
 *   <li>the failure's innermost cause, or {@code -}
 *   <li>the job's elapsed time in nanoseconds, excluding cluster start-up and teardown
 *   <li>how many samples were written
 *   <li>the peak heap in bytes any sample saw
 *   <li>milliseconds this cell spent collecting
 *   <li>the extremes, as {@code name=value} joined by {@code ;}
 *   <li>the registered gauges no reading caught, joined by {@code ;}
 *   <li>a teardown failure, or {@code -}
 * </ol>
 *
 * <p>What it cannot show. The probe passes an emulator endpoint, which switches off the committer's
 * queue-retention readback — a cluster run performs that {@code GetQueue} on every {@code
 * createCommitter} before its first commit, and a failure there leaves the committer's gauges
 * unregistered, which is exactly the shape this probe reports as absent. Checkpoint statistics are
 * not sampled either. Which quantities this rig owes, and how it takes them, is settled by the
 * preregistration revision rather than here.
 */
final class CloudTasksLeanProbe {
    /**
     * The line every run prints. It shares no prefix with the {@code CT1246} rows, so a consumer
     * scanning a mixed stream tells them apart with {@code startsWith} rather than a field count.
     */
    static final String PREFIX = "PROBE1246";

    private static final Duration SAMPLE = Duration.ofSeconds(1);
    private static final Duration POLL = Duration.ofMillis(100);
    private static final Duration SUBMIT = Duration.ofSeconds(30);

    /** Nothing in the summary may carry these, or its fields stop being countable. */
    private static final String UNSAFE = "[,;\\r\\n]";

    /** What an eager arm's writer registers. */
    private static final List<String> EAGER =
            List.of(CloudTasksMetricNames.IN_FLIGHT_TASKS, CloudTasksMetricNames.PARKED_TASKS);

    /** What a staged arm's writer and committer register between them. */
    private static final List<String> STAGED =
            List.of(
                    CloudTasksMetricNames.STAGED_TASKS,
                    CloudTasksMetricNames.STAGED_BYTES,
                    CloudTasksMetricNames.OLDEST_STAGED_TASK_AGE_MILLIS,
                    CloudTasksMetricNames.STAGED_REPLAY_BUDGET_MILLIS,
                    CloudTasksMetricNames.CURRENT_COMMIT_OLDEST_TASK_AGE_MILLIS,
                    CloudTasksMetricNames.CURRENT_COMMIT_REPLAY_BUDGET_MILLIS);

    /** How a gauge's extreme is taken, because the wrong direction hides what it is for. */
    enum Track {
        /** A remaining budget: the run's worst moment is its smallest value. */
        LOW,
        /** An age: the run's worst moment is its largest. */
        HIGH,
        /** A count or a size: what matters is the largest total across subtasks. */
        TOTAL
    }

    private CloudTasksLeanProbe() {}

    public static void main(String... args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                    "Usage: <directory> <emulator endpoint or -> --name value ...");
        }
        String endpoint = args[1].equals("-") ? null : args[1];
        MeasurementOptions options =
                MeasurementOptions.parse(Arrays.copyOfRange(args, 2, args.length));
        System.out.println(run(Path.of(args[0]), endpoint, options));
    }

    /** The gauges the arm's sink must register for the run to have measured anything. */
    static List<String> required(MeasurementOptions.Arm arm) {
        return arm == MeasurementOptions.Arm.STAGED_HASH
                        || arm == MeasurementOptions.Arm.STAGED_RANDOM
                ? STAGED
                : EAGER;
    }

    /** The reason a run with an unregistered gauge reports, rather than a silent empty list. */
    static String absent(List<String> missing) {
        return "gauges absent: " + String.join(" ", missing);
    }

    /**
     * How long the run may take before it is abandoned: four times the cell's own nominal span and
     * never under a minute. Long enough that a slow host is not called a failure, short enough that
     * a wedged job does not wait for whoever started it to notice — and, for a cell whose span is
     * seconds, short enough that a surrounding test timeout is the outer bound rather than this.
     */
    static Duration deadline(MeasurementOptions options) {
        long span =
                options.windowed()
                        ? options.warmupSeconds + options.observationSeconds
                        : (long) Math.ceil(options.records / options.offeredRate);
        // The clamp, not the options, is what bounds this. `--offered-rate` is bounded above but
        // has no lower bound beyond being positive, so in record-count mode `records /
        // offeredRate` is unbounded above and can even saturate the cast at Long.MAX_VALUE; the
        // six-hour ceiling is applied before the multiplication, which is what keeps it sane.
        return Duration.ofSeconds(Math.max(60, Math.min(span, TimeUnit.HOURS.toSeconds(6)) * 4));
    }

    /**
     * Runs the cell and returns its summary line, printed by {@code main} and asserted by tests.
     */
    static String run(Path directory, String endpoint, MeasurementOptions options)
            throws Exception {
        return run(directory, endpoint, options, Set.of());
    }

    /**
     * The same run with the reader deaf to the named gauges, so a test can produce the absence the
     * summary exists to report.
     */
    static String run(Path directory, String endpoint, MeasurementOptions options, Set<String> deaf)
            throws Exception {
        Files.createDirectories(directory);
        SinkGaugeReporter.reset();
        // Both arms' names, not just this one's, so a sink registering a gauge the arm does not
        // require still shows up rather than being invisible to the reader.
        SinkGaugeReporter.watch(
                Stream.concat(EAGER.stream(), STAGED.stream()).collect(Collectors.toSet()));
        SinkGaugeReporter.deafTo(deaf);
        Map<String, Long> extremes = new LinkedHashMap<>();
        Cost cost = new Cost();
        String reason = "-";
        String teardown = "-";
        JobClient job = null;
        long started = System.nanoTime();
        long finished;
        try {
            job = submit(directory, endpoint, options);
            // The cell's cost is the job's, not the cluster's start-up.
            started = System.nanoTime();
            cost.gcAtStart = gcMillis();
            observe(directory, job, extremes, cost, started, deadline(options));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            reason = safe(interrupted);
        } catch (Exception failure) {
            reason = safe(failure);
        } finally {
            // Stamped before the cluster comes down, so teardown is not charged to the cell.
            finished = System.nanoTime();
            if (job != null) {
                try {
                    // A job that reached an end state has already taken its cluster down with it:
                    // the local executor shuts the MiniCluster down when the result completes.
                    // Only an abandoned one — the deadline, an interrupt — still needs cancelling.
                    if (!job.getJobExecutionResult().isDone()) {
                        job.cancel().get(SUBMIT.toSeconds(), TimeUnit.SECONDS);
                    }
                } catch (Exception failure) {
                    // A measurement that completed is not undone by a slow shutdown, so this gets
                    // its own field instead of rewriting the outcome.
                    teardown = safe(failure);
                }
            }
        }
        Set<String> registered = SinkGaugeReporter.registered();
        List<String> missing =
                required(options.arm).stream().filter(name -> !registered.contains(name)).toList();
        if (reason.equals("-") && !missing.isEmpty()) {
            reason = absent(missing);
        }
        Set<String> observed = SinkGaugeReporter.observed();
        // Registered and never caught. A gauge that was never registered is the reason field's,
        // and naming it here too would leave an operator unable to tell which fact this is.
        List<String> unobserved =
                required(options.arm).stream()
                        .filter(registered::contains)
                        .filter(name -> !observed.contains(name))
                        .toList();
        return summary(options, reason, finished - started, cost, extremes, unobserved, teardown);
    }

    /** Samples once a second until the job ends, recording what it saw as it goes. */
    private static void observe(
            Path directory,
            JobClient job,
            Map<String, Long> extremes,
            Cost cost,
            long started,
            Duration deadline)
            throws Exception {
        long expiry = started + deadline.toNanos();
        var result = job.getJobExecutionResult();
        try (Writer file = Files.newBufferedWriter(directory.resolve("samples.jsonl"))) {
            long next = started;
            while (!result.isDone()) {
                long now = System.nanoTime();
                if (now >= expiry) {
                    throw new TimeoutException("Cell exceeded " + deadline.toSeconds() + "s");
                }
                if (now >= next) {
                    file.write(sample(extremes, cost, now - started) + "\n");
                    // Advanced from the schedule so a slow poll does not stretch the period, but
                    // resynchronised after a long pause rather than emitting the backlog at once.
                    next = Math.max(next + SAMPLE.toNanos(), now);
                }
                try {
                    result.get(POLL.toMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException waiting) {
                    // This loop owns the deadline; the job decides when it is done.
                }
            }
        }
        // The polling get above already throws when the job has failed, which is how a dead cell
        // normally reaches the caller. This closes the one gap that leaves: a job that fails after
        // a poll times out and before the next `isDone` check would otherwise end the loop with
        // nothing thrown, and the run would report a full set of samples as an observation.
        result.get();
    }

    private static JobClient submit(Path directory, String endpoint, MeasurementOptions options)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, options.parallelism);
        // The cluster's cells run under the same strategy, so a restart costs the same here. A
        // restarted task's metrics are unregistered, which is why the reporter drops them.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                directory.resolve("checkpoints").toUri().toString());
        configuration.set(MetricOptions.REPORTERS_LIST, "sink-gauges");
        Configuration reporter = MetricOptions.forReporter(configuration, "sink-gauges");
        reporter.set(MetricOptions.REPORTER_FACTORY_CLASS, SinkGaugeReporter.class.getName());
        // The rig reads the gauge objects itself; this only has to be often enough that Flink
        // keeps the reporter alive.
        reporter.set(MetricOptions.REPORTER_INTERVAL, Duration.ofSeconds(10));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        CloudTasksMeasurementJob.configure(env, options, endpoint);
        return env.executeAsync("Cloud Tasks probe " + options.runId + "/" + options.cellId);
    }

    private static String sample(Map<String, Long> extremes, Cost cost, long elapsed) {
        Map<String, SinkGaugeReporter.Reading> readings = SinkGaugeReporter.read();
        readings.forEach((name, reading) -> keep(extremes, name, reading));
        var heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        cost.samples++;
        cost.heapPeak = Math.max(cost.heapPeak, heap.getUsed());
        StringJoiner values = new StringJoiner(",", "{", "}");
        readings.forEach(
                (name, reading) ->
                        values.add(
                                "\""
                                        + name
                                        + "\":{\"sum\":"
                                        + reading.sum()
                                        + ",\"min\":"
                                        + reading.min()
                                        + ",\"max\":"
                                        + reading.max()
                                        + "}"));
        return "{\"elapsedNanos\":"
                + elapsed
                + ",\"gauges\":"
                + values
                + ",\"heapUsed\":"
                + heap.getUsed()
                + ",\"heapMax\":"
                + heap.getMax()
                + ",\"gcMillis\":"
                + (gcMillis() - cost.gcAtStart)
                + "}";
    }

    /**
     * Keeps the extreme that says what the gauge is for, which is not always the largest.
     *
     * <p>A reading only exists for a gauge some registration reported a real value for — {@link
     * SinkGaugeReporter#read()} drops the idle sentinel per registration — so there is no sentinel
     * left to guard against here.
     */
    static void keep(Map<String, Long> extremes, String name, SinkGaugeReporter.Reading reading) {
        switch (track(name)) {
            case LOW -> extremes.merge(name, reading.min(), Math::min);
            case HIGH -> extremes.merge(name, reading.max(), Math::max);
            case TOTAL -> extremes.merge(name, reading.sum(), Math::max);
        }
    }

    /**
     * Which direction a gauge's extreme runs, by the suffix its name carries.
     *
     * <p>A name that matches neither suffix is a total. That is the right default for a count or a
     * size and the wrong one for a budget, so a renamed budget would be reported at its largest —
     * the most reassuring number available — which is why a test asserts this against the metric
     * constants rather than against literals.
     */
    static Track track(String name) {
        if (name.endsWith("ReplayBudgetMillis")) {
            return Track.LOW;
        }
        return name.endsWith("AgeMillis") ? Track.HIGH : Track.TOTAL;
    }

    private static long gcMillis() {
        long total = 0;
        for (var collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += Math.max(0, collector.getCollectionTime());
        }
        return total;
    }

    /**
     * A failure's text, with everything that would add a field to the line taken out.
     *
     * <p>It reports the innermost cause, not the one thrown here. A job that dies reaches this
     * method as {@code ExecutionException: Job execution failed.} on every arm and every defect
     * alike, and the summary line is all a caller keeps — no stack trace is written anywhere.
     */
    static String safe(Throwable failure) {
        Throwable root = failure;
        // A self-referential or cyclic chain is possible; bound the walk rather than trust it.
        for (int depth = 0; depth < 32 && root.getCause() != null && root.getCause() != root; ) {
            root = root.getCause();
            depth++;
        }
        String message = root.getMessage();
        String text =
                root.getClass().getSimpleName() + ": " + (message == null ? "no message" : message);
        return text.replaceAll(UNSAFE, " ");
    }

    private static String summary(
            MeasurementOptions options,
            String reason,
            long elapsed,
            Cost cost,
            Map<String, Long> extremes,
            List<String> unobserved,
            String teardown) {
        StringJoiner gauges = new StringJoiner(";");
        extremes.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> gauges.add(entry.getKey() + "=" + entry.getValue()));
        return String.join(
                ",",
                PREFIX,
                options.runId,
                options.cellId,
                options.arm.name(),
                reason.equals("-") ? "OBSERVATION" : "FAILED",
                reason,
                Long.toString(elapsed),
                Long.toString(cost.samples),
                Long.toString(cost.heapPeak),
                Long.toString(gcMillis() - cost.gcAtStart),
                gauges.toString(),
                String.join(";", unobserved),
                teardown);
    }

    /**
     * What the cell cost, accumulated as it runs.
     *
     * <p>Mutable and passed down so that a run which fails halfway still reports the samples it
     * took: a summary claiming zero samples beside a {@code samples.jsonl} holding two hundred
     * lines is the kind of contradiction this rig exists to stop producing.
     */
    private static final class Cost {
        private long samples;
        private long heapPeak;
        private long gcAtStart;
    }
}
