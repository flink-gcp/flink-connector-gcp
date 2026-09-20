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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
        if (fields.length != 9
                || !Set.of("initial", "upgrade").contains(fields[7])
                || !Set.of("true", "false").contains(fields[8])
                || (fields[7].equals("upgrade") && !fields[8].equals("true"))) {
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
        consistent(observations, fields[6], payload);
        consistent(attempts, fields[5], fields[7] + "|" + fields[8]);
        consistent(inputMessages, id[0] + "|" + fields[4], logical);
        logicalInputs.add(logical);
    }

    private static void consistent(Map<String, String> values, String key, String value) {
        String previous = values.putIfAbsent(key, value);
        if (previous != null && !previous.equals(value)) {
            throw new IllegalArgumentException("One evidence identity names conflicting records");
        }
    }

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
                + (lines - outputMessages.size());
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
