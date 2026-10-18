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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Inputs and the fixed table layout of one finite Bigtable recovery trial. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";

    // The layout the rig's runner creates in the run's instance and its oracle reads back.
    static final String EXACTLY_ONCE_TABLE = "eo";
    static final String AT_LEAST_ONCE_TABLE = "alo";
    static final String RELAY_TABLE = "relay";
    // Single-cluster routing with single-row transactions, which the staged sink requires and
    // under which the change-stream source's coordinator reads its partitions.
    static final String APP_PROFILE = "recovery";
    // INT64 SUM in both sink tables; the staged sink's markers sit in a raw family without GC.
    static final String SUM_FAMILY = "agg";
    static final String SUM_QUALIFIER = "count";
    static final long SUM_TIMESTAMP_MICROS = 1000;
    static final String MARKER_FAMILY = "flink_commit";
    static final String RELAY_FAMILY = "r";

    // The staged committers apply each checkpoint's input in one burst, as in the Cloud Tasks
    // trial; at this rate a 30-second checkpoint is 3,000 envelopes, one row each.
    private static final long MAX_RECORDS_PER_SECOND = 100;
    // One hour of input at that rate.
    private static final long MAX_RECORDS = 3600 * MAX_RECORDS_PER_SECOND;
    // The rig runs one subtask of each parallel vertex on each of at most two single-slot
    // TaskManagers, and a rescale trial moves between one and two.
    private static final int MAX_TRIAL_PARALLELISM = 2;
    // Bigtable instance IDs have at most 33 characters.
    private static final int MAX_INSTANCE_LENGTH = 33;

    private static final String LABEL = "[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?";
    private static final Set<String> ARGUMENTS =
            Set.of(
                    "run-id",
                    "instance",
                    "records",
                    "records-per-second",
                    "change-stream-end",
                    "phase",
                    "require-restored",
                    "parallelism");

    final String runId;
    // The instance in PROJECT that the rig creates for the run and owns by name.
    final String instance;
    final long records;
    final long recordsPerSecond;
    // The source stops reading the eo table's change stream here, so the job can finish.
    final Instant changeStreamEnd;
    final String phase;
    final boolean requireRestored;
    // Every vertex but the single input; a savepoint upgrade may change it, which rescales.
    final int parallelism;

    private RecoveryOptions(Map<String, String> args) {
        runId = required(args, "run-id");
        if (!runId.matches(LABEL)) {
            throw new IllegalArgumentException("--run-id must match the Tier-3 run label grammar");
        }
        instance = required(args, "instance");
        if (!instance.matches("bt1316-" + LABEL) || instance.length() > MAX_INSTANCE_LENGTH) {
            throw new IllegalArgumentException(
                    "--instance must be a bt1316- instance ID of at most "
                            + MAX_INSTANCE_LENGTH
                            + " characters");
        }
        records = number(args, "records");
        if (records < 2 || records > MAX_RECORDS) {
            throw new IllegalArgumentException(
                    "--records must cover two subtasks and be at most " + MAX_RECORDS);
        }
        recordsPerSecond = number(args, "records-per-second");
        if (recordsPerSecond < 1 || recordsPerSecond > MAX_RECORDS_PER_SECOND) {
            throw new IllegalArgumentException(
                    "--records-per-second must be between 1 and " + MAX_RECORDS_PER_SECOND);
        }
        changeStreamEnd = instant(args, "change-stream-end");
        phase = args.getOrDefault("phase", "initial");
        if (!Set.of("initial", "upgrade").contains(phase)) {
            throw new IllegalArgumentException("--phase must be initial or upgrade");
        }
        String restored = args.getOrDefault("require-restored", "false");
        if (!Set.of("true", "false").contains(restored)) {
            throw new IllegalArgumentException("--require-restored must be true or false");
        }
        requireRestored = Boolean.parseBoolean(restored);
        if (phase.equals("upgrade") && !requireRestored) {
            throw new IllegalArgumentException("--phase upgrade requires --require-restored true");
        }
        long value = args.containsKey("parallelism") ? number(args, "parallelism") : 2;
        if (value < 1 || value > MAX_TRIAL_PARALLELISM) {
            throw new IllegalArgumentException(
                    "--parallelism must be between 1 and " + MAX_TRIAL_PARALLELISM);
        }
        parallelism = (int) value;
    }

    static RecoveryOptions parse(String... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --name value pairs");
        }
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--")
                    || !ARGUMENTS.contains(args[i].substring(2))
                    || values.putIfAbsent(args[i].substring(2), args[i + 1]) != null) {
                throw new IllegalArgumentException(
                        "Unknown or duplicate Bigtable argument: " + args[i]);
            }
        }
        return new RecoveryOptions(values);
    }

    TableDestination table(String name) {
        return TableDestination.of(PROJECT, instance, name);
    }

    /**
     * The row of one input record. The leading hex digit spreads consecutive records over the
     * runner's initial splits, so every change-stream partition carries input; the rest names the
     * sequence, so a row read back maps to exactly one record of this run.
     */
    String rowKey(long sequence) {
        if (sequence < 0 || sequence >= records) {
            throw new IllegalArgumentException("Sequence is outside this run's input");
        }
        return String.format("%x/%07d", sequence % 16, sequence);
    }

    /** One SUM contribution of one record, which both sink modes write to their own table. */
    RowMutationEntry contribution(long sequence) {
        return RowMutationEntry.create(rowKey(sequence))
                .addToCell(SUM_FAMILY, SUM_QUALIFIER, SUM_TIMESTAMP_MICROS, 1L);
    }

    String identity() {
        // Phase and parallelism change in an upgrade; the input and its instance must not.
        return "v1/"
                + runId
                + "/"
                + instance
                + "/"
                + records
                + "/"
                + recordsPerSecond
                + "/"
                + changeStreamEnd;
    }

    private static String required(Map<String, String> args, String name) {
        String value = args.get(name);
        if (value == null) {
            throw new IllegalArgumentException("Missing --" + name);
        }
        return value;
    }

    private static long number(Map<String, String> args, String name) {
        try {
            return Long.parseLong(required(args, name));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " must be an integer", e);
        }
    }

    private static Instant instant(Map<String, String> args, String name) {
        try {
            return Instant.parse(required(args, name));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("--" + name + " must be an ISO-8601 instant", e);
        }
    }
}
