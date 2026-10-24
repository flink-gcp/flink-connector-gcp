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

package io.github.flink.gcp.connector.tier3.spanner;

import org.apache.flink.annotation.Internal;

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Inputs and the fixed database layout of one finite Spanner recovery trial. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";

    // The layout the rig's runner creates in the run's instance and its oracle reads back. One
    // GoogleSQL database holds every table; the change stream watches only the input table.
    static final String DATABASE = "recovery";
    static final String INPUT_TABLE = "input";
    static final String CHANGE_STREAM = "input_changes";
    static final String RELAY_TABLE = "relay";
    static final String SEED_TABLE = "seed";
    static final String BATCH_TABLE = "batch_out";
    static final String KEY_COLUMN = "k";
    static final String SEQUENCE_COLUMN = "n";
    static final String DELIVERY_COLUMN = "delivery";
    static final String COMMIT_COLUMN = "commit_micros";

    // The input sink holds a checkpoint's input in one batch, as the README explains; at this
    // rate a 30-second checkpoint is 3,000 mutations.
    private static final long MAX_RECORDS_PER_SECOND = 100;
    // One hour of input at that rate.
    private static final long MAX_RECORDS = 3600 * MAX_RECORDS_PER_SECOND;
    // The seed copy is paced, per reading subtask, so that its partitions outlast both
    // recoveries of a trial.
    private static final long MAX_SEED_ROWS_PER_SECOND = 1000;
    // The rig runs one subtask of each parallel vertex on each of at most two single-slot
    // TaskManagers, and a rescale trial moves between one and two.
    private static final int MAX_TRIAL_PARALLELISM = 2;

    private static final String LABEL = "[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?";
    private static final Set<String> ARGUMENTS =
            Set.of(
                    "run-id",
                    "instance",
                    "records",
                    "records-per-second",
                    "change-stream-end",
                    "seed-rows-per-second",
                    "phase",
                    "require-restored",
                    "parallelism");

    final String runId;
    // The instance in PROJECT that the rig creates for the run and owns by name.
    final String instance;
    final long records;
    final long recordsPerSecond;
    // The source stops reading the input table's change stream here, so the job can finish.
    final Instant changeStreamEnd;
    // The pace of the seed copy, which keeps the batch read's partitions unfinished.
    final long seedRowsPerSecond;
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
        // A label keeps the ID within Spanner's 64 characters.
        if (!instance.matches("sp1360-" + LABEL)) {
            throw new IllegalArgumentException("--instance must be an sp1360- instance ID");
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
        seedRowsPerSecond = number(args, "seed-rows-per-second");
        if (seedRowsPerSecond < 1 || seedRowsPerSecond > MAX_SEED_ROWS_PER_SECOND) {
            throw new IllegalArgumentException(
                    "--seed-rows-per-second must be between 1 and " + MAX_SEED_ROWS_PER_SECOND);
        }
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
                        "Unknown or duplicate Spanner argument: " + args[i]);
            }
        }
        return new RecoveryOptions(values);
    }

    DatabaseDestination database() {
        return DatabaseDestination.of(PROJECT, instance, DATABASE);
    }

    /**
     * The key of one input record. The leading hex digit spreads consecutive records over the key
     * space, so a split of the input table carries input; the rest names the sequence, so a row
     * read back maps to exactly one record of this run.
     */
    String rowKey(long sequence) {
        if (sequence < 0 || sequence >= records) {
            throw new IllegalArgumentException("Sequence is outside this run's input");
        }
        return String.format("%x/%07d", sequence % 16, sequence);
    }

    /**
     * The input row of one record. A replay writes the same values again, so the final table holds
     * one row per record however often the sink executed the write.
     */
    Mutation input(long sequence) {
        return Mutation.newInsertOrUpdateBuilder(INPUT_TABLE)
                .set(KEY_COLUMN)
                .to(rowKey(sequence))
                .set(SEQUENCE_COLUMN)
                .to(sequence)
                .build();
    }

    String identity() {
        // Phase, parallelism and the seed pace change in an upgrade; the input and its instance
        // must not.
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
