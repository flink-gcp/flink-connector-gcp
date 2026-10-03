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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ParameterTool;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Fixed-target inputs for one finite BigQuery recovery trial. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";
    static final String DATASET = "flink_gcp_tier3_bigquery";
    // The state bucket's run prefix: inside the run-state IAM condition and the cleanup prefix.
    static final String RUNS = "gs://flink-gcp-tier3-bigquery/runs";
    static final long MAX_BYTES_PER_SECOND = 1024 * 1024;
    static final long MAX_BYTES = 2L * 1024 * 1024 * 1024;
    static final Target TIER3 = new Target(PROJECT, DATASET, RUNS, null);

    enum Mode {
        ALO(64 * 1024, Duration.ofSeconds(30)),
        EO(1024, Duration.ofSeconds(30)),
        // FILE_LOADS refuses an interval below its minCheckpointInterval, two minutes by default.
        FILE_LOADS(1024, Duration.ofMinutes(2));

        final int rowBytes;
        final Duration checkpointInterval;

        Mode(int rowBytes, Duration checkpointInterval) {
            this.rowBytes = rowBytes;
            this.checkpointInterval = checkpointInterval;
        }
    }

    /**
     * Where a trial writes. The deployed application always uses {@link #TIER3}; only the gated
     * integration test substitutes its own project, dataset and bucket, and a checkpoint interval
     * short enough for a test, which then also becomes the FILE_LOADS minimum.
     */
    static final class Target {
        final String project;
        final String dataset;
        final String runs;
        @Nullable final Duration checkpointInterval;

        Target(String project, String dataset, String runs, @Nullable Duration checkpointInterval) {
            this.project = project;
            this.dataset = dataset;
            this.runs = runs;
            this.checkpointInterval = checkpointInterval;
        }
    }

    private static final Set<String> FILE_LOADS_ARGUMENTS =
            Set.of(
                    "staging-format",
                    "max-concurrent-checkpoint-finalizations",
                    "max-concurrent-destinations",
                    "max-staging-file-bytes",
                    "max-open-destinations");
    private static final Set<String> ARGUMENTS =
            Stream.concat(
                            Stream.of(
                                    "run-id",
                                    "mode",
                                    "destinations",
                                    "records",
                                    "bytes-per-second",
                                    "phase",
                                    "require-restored"),
                            FILE_LOADS_ARGUMENTS.stream())
                    .collect(Collectors.toUnmodifiableSet());

    final String runId;
    final Mode mode;
    final int destinations;
    final long records;
    final long bytesPerSecond;
    final String phase;
    final boolean requireRestored;
    final Duration checkpointInterval;
    // FILE_LOADS only; the connector's builder checks the arguments' ranges as it is parsed.
    @Nullable final FileLoadsOptions fileLoads;
    private final TableDestination[] tables;

    private RecoveryOptions(Target target, ParameterTool args) {
        runId = args.getRequired("run-id");
        if (!runId.matches("[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?")) {
            throw new IllegalArgumentException("--run-id must match the Tier-3 run label grammar");
        }
        mode = Mode.valueOf(args.getRequired("mode"));
        destinations = args.getInt("destinations");
        if (destinations != 10 && destinations != 50) {
            throw new IllegalArgumentException("--destinations must be 10 or 50");
        }
        records = args.getLong("records", 1800 * MAX_BYTES_PER_SECOND / mode.rowBytes);
        if (records < 2L * destinations || records > MAX_BYTES / mode.rowBytes) {
            throw new IllegalArgumentException(
                    "--records must cover all destinations on both writers and fit 2 GiB");
        }
        bytesPerSecond = args.getLong("bytes-per-second", MAX_BYTES_PER_SECOND);
        if (bytesPerSecond < 1 || bytesPerSecond > MAX_BYTES_PER_SECOND) {
            throw new IllegalArgumentException("--bytes-per-second must be between 1 and 1048576");
        }
        phase = args.get("phase", "initial");
        if (!Set.of("initial", "upgrade").contains(phase)) {
            throw new IllegalArgumentException("--phase must be initial or upgrade");
        }
        String restored = args.get("require-restored", "false");
        if (!Set.of("true", "false").contains(restored)) {
            throw new IllegalArgumentException("--require-restored must be true or false");
        }
        requireRestored = Boolean.parseBoolean(restored);
        if (phase.equals("upgrade") && !requireRestored) {
            throw new IllegalArgumentException("--phase upgrade requires --require-restored true");
        }
        if (mode != Mode.FILE_LOADS) {
            for (String name : FILE_LOADS_ARGUMENTS) {
                if (args.has(name)) {
                    throw new IllegalArgumentException(
                            "--" + name + " applies only to --mode FILE_LOADS");
                }
            }
        }
        checkpointInterval =
                target.checkpointInterval == null
                        ? mode.checkpointInterval
                        : target.checkpointInterval;
        fileLoads = mode == Mode.FILE_LOADS ? fileLoads(target, args) : null;
        tables = new TableDestination[destinations];
        String prefix = "bq_" + runId.replace('-', '_') + "_d";
        for (int destination = 0; destination < destinations; destination++) {
            tables[destination] =
                    TableDestination.of(target.project, target.dataset, prefix + destination);
        }
    }

    private FileLoadsOptions fileLoads(Target target, ParameterTool args) {
        var builder =
                FileLoadsOptions.builder()
                        .stagingPath(target.runs + "/" + runId + "/staging")
                        .writeDisposition(WriteDisposition.WRITE_APPEND)
                        .stagingFormat(
                                StagingFormat.valueOf(
                                        args.get(
                                                "staging-format",
                                                FileLoadsOptions.DEFAULT_STAGING_FORMAT.name())))
                        .maxConcurrentCheckpointFinalizations(
                                args.getInt(
                                        "max-concurrent-checkpoint-finalizations",
                                        FileLoadsOptions
                                                .DEFAULT_MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS))
                        .maxConcurrentDestinations(
                                args.getInt(
                                        "max-concurrent-destinations",
                                        FileLoadsOptions.DEFAULT_MAX_CONCURRENT_DESTINATIONS))
                        .maxStagingFileBytes(
                                args.getLong(
                                        "max-staging-file-bytes",
                                        FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES))
                        .maxOpenDestinations(
                                args.getInt(
                                        "max-open-destinations",
                                        FileLoadsOptions.DEFAULT_MAX_OPEN_DESTINATIONS));
        if (target.checkpointInterval != null) {
            // Only the gated test shortens the interval; the deployed trial keeps the default.
            builder.minCheckpointInterval(target.checkpointInterval);
        }
        return builder.build();
    }

    static RecoveryOptions parse(String... args) {
        return parse(TIER3, args);
    }

    static RecoveryOptions parse(Target target, String... args) {
        Set<String> seen = new HashSet<>();
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --name value pairs");
        }
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--")
                    || !ARGUMENTS.contains(args[i].substring(2))
                    || !seen.add(args[i])) {
                throw new IllegalArgumentException(
                        "Unknown or duplicate BigQuery argument: " + args[i]);
            }
        }
        return new RecoveryOptions(target, ParameterTool.fromArgs(args));
    }

    int destination(long sequence) {
        if (sequence < 0 || sequence >= records) {
            throw new IllegalArgumentException("Sequence is outside this run's input");
        }
        return (int) (sequence % destinations);
    }

    TableDestination table(long sequence) {
        return tables[destination(sequence)];
    }

    String identity() {
        // Phase changes during upgrade; the input and sink contract must not change.
        String identity =
                "v1/"
                        + runId
                        + "/"
                        + mode
                        + "/"
                        + destinations
                        + "/"
                        + records
                        + "/"
                        + bytesPerSecond;
        if (fileLoads == null) {
            return identity;
        }
        return identity
                + "/"
                + fileLoads.getStagingFormat().name()
                + "/"
                + fileLoads.getMaxConcurrentCheckpointFinalizations()
                + "/"
                + fileLoads.getMaxConcurrentDestinations()
                + "/"
                + fileLoads.getMaxStagingFileBytes()
                + "/"
                + fileLoads.getMaxOpenDestinations();
    }
}
