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

import org.apache.flink.annotation.Internal;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Fixed-target inputs for one finite Cloud Tasks recovery trial. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";
    static final String LOCATION = "us-central1";
    // RFC 6761 reserves .invalid; the paused queue never dispatches a task.
    static final String TARGET = "https://ct1362.invalid/task";
    // The committers create each checkpoint's tasks in one burst, not at the source rate. #1246
    // measured staged creation keeping up at 100 records/s on four subtasks. A 30-second checkpoint
    // of it is 3,000 creates, which two committers on the default single channel, about 210
    // creates/s per subtask in #937, send in seconds and below the 500 per second Google's 500/50/5
    // guidance starts a new queue at.
    private static final long MAX_RECORDS_PER_SECOND = 100;
    // One hour of input at that rate.
    private static final long MAX_RECORDS = 3600 * MAX_RECORDS_PER_SECOND;
    // The rig runs one sink subtask on each of at most two single-slot TaskManagers, and a rescale
    // trial moves between one and two.
    private static final int MAX_SINK_PARALLELISM = 2;

    private static final String LABEL = "[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?";
    private static final Set<String> ARGUMENTS =
            Set.of(
                    "run-id",
                    "queue",
                    "records",
                    "records-per-second",
                    "phase",
                    "require-restored",
                    "sink-parallelism");

    final String runId;
    // The queue ID in PROJECT/LOCATION; the rig creates it paused and owns its name.
    final String queue;
    final long records;
    final long recordsPerSecond;
    final String phase;
    final boolean requireRestored;
    // The writer's and committer's subtasks; a savepoint upgrade may change it, which rescales.
    final int sinkParallelism;

    private RecoveryOptions(Map<String, String> args) {
        runId = required(args, "run-id");
        if (!runId.matches(LABEL)) {
            throw new IllegalArgumentException("--run-id must match the Tier-3 run label grammar");
        }
        queue = required(args, "queue");
        if (!queue.matches("ct1362-" + LABEL)) {
            throw new IllegalArgumentException(
                    "--queue must be a ct1362- queue ID followed by a Tier-3 run label");
        }
        records = number(args, "records");
        if (records < 2 || records > MAX_RECORDS) {
            throw new IllegalArgumentException(
                    "--records must cover both sink subtasks and be at most " + MAX_RECORDS);
        }
        recordsPerSecond = number(args, "records-per-second");
        if (recordsPerSecond < 1 || recordsPerSecond > MAX_RECORDS_PER_SECOND) {
            throw new IllegalArgumentException(
                    "--records-per-second must be between 1 and " + MAX_RECORDS_PER_SECOND);
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
        long parallelism =
                args.containsKey("sink-parallelism") ? number(args, "sink-parallelism") : 2;
        if (parallelism < 1 || parallelism > MAX_SINK_PARALLELISM) {
            throw new IllegalArgumentException(
                    "--sink-parallelism must be between 1 and " + MAX_SINK_PARALLELISM);
        }
        sinkParallelism = (int) parallelism;
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
                        "Unknown or duplicate Cloud Tasks argument: " + args[i]);
            }
        }
        return new RecoveryOptions(values);
    }

    /**
     * The deduplication key of one input record. The sink names its task with the key's SHA-256 hex
     * digest, so a listed task name maps back to exactly one sequence of this run.
     */
    String taskKey(long sequence) {
        if (sequence < 0 || sequence >= records) {
            throw new IllegalArgumentException("Sequence is outside this run's input");
        }
        return runId + "/" + sequence;
    }

    /** The task for one record; its body repeats the key, so a full read can check the name. */
    Task task(long sequence) {
        return Task.newBuilder()
                .setHttpRequest(
                        HttpRequest.newBuilder()
                                .setHttpMethod(HttpMethod.POST)
                                .setUrl(TARGET)
                                .setBody(ByteString.copyFromUtf8(taskKey(sequence))))
                .build();
    }

    String identity() {
        // Phase and sink parallelism change in an upgrade; the input and sink contract must not.
        return "v1/" + runId + "/" + queue + "/" + records + "/" + recordsPerSecond;
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
