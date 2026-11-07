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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.annotation.Internal;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/** Offline completeness and duplicate accounting over independently collected output messages. */
@Internal
public final class PubSubRecoveryReport {
    private final RecoveryOptions options;
    private final Map<String, String> outputMessages = new HashMap<>();
    private final Map<String, String> observations = new HashMap<>();
    private final Map<String, String> attempts = new HashMap<>();
    private final Map<String, String> inputMessages = new HashMap<>();
    private final Set<String> logicalInputs = new HashSet<>();
    // Each input message's distinct observations as {origin, epoch, subtask, attempt number}.
    private final Map<String, List<long[]>> processing = new HashMap<>();
    private int lines;

    PubSubRecoveryReport(RecoveryOptions options) {
        this.options = options;
    }

    void accept(String outputMessageId, String payload) {
        if (++lines > 200000
                || outputMessageId.isEmpty()
                || outputMessageId.length() > 256
                || payload.length() > 2048) {
            throw new IllegalArgumentException(
                    "Evidence exceeds its bound or lacks an output message ID");
        }
        String[] fields = payload.split("\\|", -1);
        if (fields.length != 13
                || !Set.of("initial", "upgrade").contains(fields[7])
                || !Set.of("true", "false").contains(fields[8])
                || (fields[7].equals("upgrade") && !fields[8].equals("true"))) {
            throw new IllegalArgumentException("Invalid output observation");
        }
        long origin = RecoveryPayload.count(fields[9]);
        long epoch = RecoveryPayload.count(fields[10]);
        long subtask = RecoveryPayload.count(fields[11]);
        long number = RecoveryPayload.count(fields[12]);
        if ((origin == 0) == fields[8].equals("true") || epoch < origin) {
            throw new IllegalArgumentException("Invalid output observation");
        }
        String logical = String.join("|", fields[0], fields[1], fields[2], fields[3]);
        int[] id = RecoveryPayload.parseInput(options, logical);
        if (RecoveryPayload.decode(fields[4]).isEmpty()) {
            throw new IllegalArgumentException("Input message ID is missing");
        }
        for (int field : new int[] {5, 6}) {
            if (!UUID.fromString(fields[field]).toString().equals(fields[field])) {
                throw new IllegalArgumentException("Attempt and observation IDs must be UUIDs");
            }
        }
        consistent(outputMessages, outputMessageId, payload);
        if (observations.putIfAbsent(fields[6], payload) == null) {
            processing
                    .computeIfAbsent(id[0] + "|" + fields[4], key -> new ArrayList<>())
                    .add(new long[] {origin, epoch, subtask, number});
        } else {
            consistent(observations, fields[6], payload);
        }
        consistent(
                attempts,
                fields[5],
                String.join("|", fields[7], fields[8], fields[9], fields[11], fields[12]));
        consistent(inputMessages, id[0] + "|" + fields[4], logical);
        logicalInputs.add(logical);
    }

    private static void consistent(Map<String, String> values, String key, String value) {
        String previous = values.putIfAbsent(key, value);
        if (previous != null && !previous.equals(value)) {
            throw new IllegalArgumentException("One evidence identity names conflicting records");
        }
    }

    /**
     * Input messages a restored attempt processed again although its restored checkpoint covered
     * them, so their acknowledgement was lost rather than never due.
     *
     * <p>An earlier processing is covered when its attempt's origin is below the later attempt's
     * and its epoch is below the end of its own run. A run ends at the first restart after it: the
     * next origin above its own, since restores only move forward and a reactive restart restarts
     * every subtask, or the same subtask's next attempt, since Flink numbers a subtask's attempts
     * upwards across restarts and so two restores of one checkpoint are two runs. A subtask a
     * scale-in removed and a later restore brought back is bounded by the origin above, not by its
     * own next attempt; when the restore that removed it restored the same checkpoint again,
     * neither bound sees it, and the count is an upper bound. A region failover that leaves some
     * subtasks running reads their runs as ending at it, so the count can miss coverage there, and
     * attempts that wrote no output are unseen, so a run they ended reads as longer.
     */
    int coveredRedeliveries() {
        TreeSet<Long> origins = new TreeSet<>();
        Map<Long, TreeSet<List<Long>>> runs = new HashMap<>();
        for (String state : attempts.values()) {
            String[] parts = state.split("\\|", -1);
            long origin = Long.parseLong(parts[2]);
            origins.add(origin);
            runs.computeIfAbsent(Long.parseLong(parts[3]), key -> new TreeSet<>(RUN_ORDER))
                    .add(List.of(origin, Long.parseLong(parts[4])));
        }
        int covered = 0;
        for (List<long[]> seen : processing.values()) {
            boolean found = false;
            for (long[] later : seen) {
                for (long[] earlier : seen) {
                    if (earlier[0] < later[0]) {
                        List<Long> next =
                                runs.get(earlier[2]).higher(List.of(earlier[0], earlier[3]));
                        Long above = origins.higher(earlier[0]);
                        long end = later[0];
                        if (next != null) {
                            end = Math.min(end, next.get(0));
                        }
                        if (above != null) {
                            end = Math.min(end, above);
                        }
                        found |= earlier[1] < end;
                    }
                }
            }
            covered += found ? 1 : 0;
        }
        return covered;
    }

    private static final Comparator<List<Long>> RUN_ORDER =
            Comparator.<List<Long>, Long>comparing(run -> run.get(0))
                    .thenComparing(run -> run.get(1));

    int distinctInputs() {
        return logicalInputs.size();
    }

    String complete() {
        if (logicalInputs.size() != 2 * options.records) {
            throw new IllegalStateException(
                    "Missing logical inputs: expected="
                            + (2 * options.records)
                            + " observed="
                            + logicalInputs.size());
        }
        return "logical_inputs="
                + logicalInputs.size()
                + " input_publication_duplicates="
                + (inputMessages.size() - logicalInputs.size())
                + " repeated_input_processing="
                + (observations.size() - inputMessages.size())
                + " output_publication_duplicates="
                + (outputMessages.size() - observations.size())
                + " repeated_output_delivery="
                + (lines - outputMessages.size())
                + " covered_redeliveries="
                + coveredRedeliveries();
    }

    /** Reads bounded UTF-8 lines containing base64url output message ID, tab, base64url payload. */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Usage: <evidence-file> --run-id=... --records-per-subscription=...");
        }
        Path input = Path.of(args[0]);
        if (Files.size(input) > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("Evidence file exceeds 64 MiB");
        }
        PubSubRecoveryReport report =
                new PubSubRecoveryReport(
                        RecoveryOptions.parse(Arrays.copyOfRange(args, 1, args.length)));
        try (BufferedReader reader = Files.newBufferedReader(input)) {
            for (String line; (line = reader.readLine()) != null; ) {
                if (line.length() > 4096) {
                    throw new IllegalArgumentException("Evidence line exceeds 4096 characters");
                }
                String[] parts = line.split("\t", -1);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(
                            "Expected bounded base64url ID and payload separated by a tab");
                }
                report.accept(RecoveryPayload.decode(parts[0]), RecoveryPayload.decode(parts[1]));
            }
        }
        System.out.println(report.complete());
    }
}
