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

package io.github.flink.gcp.connector.tier3.firestore;

import org.apache.flink.annotation.Internal;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Inputs and the fixed database layout of one finite Firestore recovery trial. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";

    // The layout the rig's runner creates in the run's two databases and its oracle reads back.
    // Both databases use the same names: collections in the Native-mode database, kinds in the
    // Datastore-mode one.
    static final String INPUT = "input";
    static final String SEED = "seed";
    static final String BATCH = "batch_out";
    static final String KEY_FIELD = "k";
    static final String SEQUENCE_FIELD = "n";
    static final String DELIVERY_FIELD = "delivery";

    // Both sinks keep up with this from their first request: the Native-mode sink's BulkWriter
    // starts at 500 operations per second, and the Datastore-mode sink commits up to 500
    // mutations at a time.
    private static final long MAX_RECORDS_PER_SECOND = 100;
    // One hour of input at that rate.
    private static final long MAX_RECORDS = 3600 * MAX_RECORDS_PER_SECOND;
    // The seed copy is paced, per reading subtask, so that its splits outlast both recoveries of
    // a trial while the read still ends inside the service's one-hour read-time window. The pace
    // stays below what the copy sinks' ramp-up throttles pass from their first write, which a
    // restore starts again: the Datastore-mode sink's 500 operations per second shared by its
    // two subtasks, and the Native-mode BulkWriter's 500 per subtask. A faster pace would be cut
    // to the throttle's, and a read sized from the pace would run longer than planned.
    private static final long MAX_SEED_RECORDS_PER_SECOND = 200;
    // The rig runs one subtask of each parallel vertex on each of at most two single-slot
    // TaskManagers, and a rescale trial moves between one and two.
    private static final int MAX_TRIAL_PARALLELISM = 2;

    private static final String LABEL = "[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?";
    // Firestore's database ID grammar within its 63 characters: a lowercase letter first, then
    // lowercase letters, digits and hyphens, ending in a letter or digit. The fs1879- prefix
    // supplies the letter and is what the trial's IAM grants match.
    private static final String DATABASE = "fs1879-[a-z0-9]([a-z0-9-]{0,54}[a-z0-9])?";
    private static final Set<String> ARGUMENTS =
            Set.of(
                    "run-id",
                    "native-database",
                    "datastore-database",
                    "records",
                    "records-per-second",
                    "seed-records-per-second",
                    "phase",
                    "require-restored",
                    "parallelism");

    final String runId;
    // The databases in PROJECT that the rig creates for the run and owns by name, one per mode.
    final String nativeDatabase;
    final String datastoreDatabase;
    final long records;
    final long recordsPerSecond;
    // The pace of each seed copy, which keeps the batch reads' splits unfinished.
    final long seedRecordsPerSecond;
    final String phase;
    final boolean requireRestored;
    // Every vertex but the single input; a savepoint upgrade may change it, which rescales.
    final int parallelism;

    private RecoveryOptions(Map<String, String> args) {
        runId = required(args, "run-id");
        if (!runId.matches(LABEL)) {
            throw new IllegalArgumentException("--run-id must match the Tier-3 run label grammar");
        }
        nativeDatabase = database(args, "native-database");
        datastoreDatabase = database(args, "datastore-database");
        if (nativeDatabase.equals(datastoreDatabase)) {
            throw new IllegalArgumentException(
                    "--native-database and --datastore-database must name two databases");
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
        seedRecordsPerSecond = number(args, "seed-records-per-second");
        if (seedRecordsPerSecond < 1 || seedRecordsPerSecond > MAX_SEED_RECORDS_PER_SECOND) {
            throw new IllegalArgumentException(
                    "--seed-records-per-second must be between 1 and "
                            + MAX_SEED_RECORDS_PER_SECOND);
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
                        "Unknown or duplicate Firestore argument: " + args[i]);
            }
        }
        return new RecoveryOptions(values);
    }

    io.github.flink.gcp.connector.firestore.DatabaseDestination nativeDestination() {
        return io.github.flink.gcp.connector.firestore.DatabaseDestination.of(
                PROJECT, nativeDatabase);
    }

    io.github.flink.gcp.connector.datastore.DatabaseDestination datastoreDestination() {
        return io.github.flink.gcp.connector.datastore.DatabaseDestination.of(
                PROJECT, datastoreDatabase);
    }

    /**
     * The key of one input record: its document id in the Native-mode database and its key name in
     * the Datastore-mode one. The leading hex digit spreads consecutive records over the key space;
     * the rest names the sequence, so a record read back maps to exactly one record of this run.
     */
    String key(long sequence) {
        if (sequence < 0 || sequence >= records) {
            throw new IllegalArgumentException("Sequence is outside this run's input");
        }
        return String.format("%x-%07d", sequence % 16, sequence);
    }

    /**
     * The Native-mode input document of one record. A replay writes the same fields again, so the
     * final collection holds one document per record however often the sink executed the write.
     */
    FirestoreWrite nativeInput(long sequence) {
        return FirestoreWrite.set(INPUT + "/" + key(sequence), Map.of(SEQUENCE_FIELD, sequence));
    }

    /**
     * The Datastore-mode input entity of one record, replay-safe for the same reason as {@link
     * #nativeInput(long)}.
     */
    DatastoreMutation datastoreInput(long sequence) {
        return DatastoreMutation.upsert(
                Entity.newBuilder(datastoreKey(INPUT, key(sequence)))
                        .set(SEQUENCE_FIELD, sequence)
                        .build());
    }

    /** A complete key in this run's Datastore-mode database, as the sink requires. */
    Key datastoreKey(String kind, String name) {
        return Key.newBuilder(PROJECT, kind, name, datastoreDatabase).build();
    }

    String identity() {
        // Phase, parallelism and the seed pace change in an upgrade; the input and its databases
        // must not.
        return "v1/"
                + runId
                + "/"
                + nativeDatabase
                + "/"
                + datastoreDatabase
                + "/"
                + records
                + "/"
                + recordsPerSecond;
    }

    private static String database(Map<String, String> args, String name) {
        String value = required(args, name);
        if (!value.matches(DATABASE)) {
            throw new IllegalArgumentException("--" + name + " must be an fs1879- database ID");
        }
        return value;
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
}
