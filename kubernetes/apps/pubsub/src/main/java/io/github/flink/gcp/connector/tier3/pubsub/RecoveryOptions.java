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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Fixed run identity and small logical input domain for the recovery relay. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";
    final String runId;
    final int records;
    final int parallelism;
    final String phase;
    final boolean requireRestored;
    final String entryPoint;
    final boolean restoreDeduplication;
    final int replayDelayMillis;
    final long maxOutstandingMessages;
    final int holdAfterCheckpointMillis;

    RecoveryOptions(
            String runId,
            int records,
            int parallelism,
            String phase,
            boolean requireRestored,
            String entryPoint) {
        this(runId, records, parallelism, phase, requireRestored, entryPoint, false, 0, 1000, 0);
    }

    RecoveryOptions(
            String runId,
            int records,
            int parallelism,
            String phase,
            boolean requireRestored,
            String entryPoint,
            boolean restoreDeduplication,
            int replayDelayMillis,
            long maxOutstandingMessages,
            int holdAfterCheckpointMillis) {
        if (runId == null || !runId.matches("[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?")) {
            throw new IllegalArgumentException(
                    "--run-id must be a Tier-3 label of at most 40 characters");
        }
        if (records < 1 || records > 10000) {
            throw new IllegalArgumentException("--records-per-subscription must be in [1, 10000]");
        }
        if (parallelism < 1 || parallelism > 3) {
            throw new IllegalArgumentException("--parallelism must be in [1, 3]");
        }
        if (!Set.of("initial", "upgrade").contains(phase)
                || (phase.equals("upgrade") && !requireRestored)) {
            throw new IllegalArgumentException(
                    "--phase must be initial or upgrade; upgrade requires --require-restored=true");
        }
        if (!Set.of("datastream", "table").contains(entryPoint)) {
            throw new IllegalArgumentException("--entry-point must be datastream or table");
        }
        if (replayDelayMillis < 0 || replayDelayMillis > 1000) {
            throw new IllegalArgumentException("--replay-delay-millis must be in [0, 1000]");
        }
        if (maxOutstandingMessages < 1 || maxOutstandingMessages > 100000) {
            throw new IllegalArgumentException("--max-outstanding-messages must be in [1, 100000]");
        }
        if (holdAfterCheckpointMillis < 0 || holdAfterCheckpointMillis > 600000) {
            throw new IllegalArgumentException(
                    "--hold-after-checkpoint-millis must be in [0, 600000]");
        }
        this.runId = runId;
        this.records = records;
        this.parallelism = parallelism;
        this.phase = phase;
        this.requireRestored = requireRestored;
        this.entryPoint = entryPoint;
        this.restoreDeduplication = restoreDeduplication;
        this.replayDelayMillis = replayDelayMillis;
        this.maxOutstandingMessages = maxOutstandingMessages;
        this.holdAfterCheckpointMillis = holdAfterCheckpointMillis;
    }

    static RecoveryOptions parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        Set<String> keys =
                Set.of(
                        "run-id",
                        "records-per-subscription",
                        "parallelism",
                        "phase",
                        "require-restored",
                        "entry-point",
                        "restore-deduplication",
                        "replay-delay-millis",
                        "max-outstanding-messages",
                        "hold-after-checkpoint-millis");
        for (String arg : args) {
            int split = arg.indexOf('=');
            if (!arg.startsWith("--")
                    || split < 3
                    || !keys.contains(arg.substring(2, split))
                    || values.putIfAbsent(arg.substring(2, split), arg.substring(split + 1))
                            != null) {
                throw new IllegalArgumentException(
                        "Expected unique --name=value arguments: " + arg);
            }
        }
        String restored = values.getOrDefault("require-restored", "false");
        if (!restored.equals("true") && !restored.equals("false")) {
            throw new IllegalArgumentException("--require-restored must be true or false");
        }
        String deduplication = values.getOrDefault("restore-deduplication", "false");
        if (!deduplication.equals("true") && !deduplication.equals("false")) {
            throw new IllegalArgumentException("--restore-deduplication must be true or false");
        }
        return new RecoveryOptions(
                values.get("run-id"),
                Integer.parseInt(values.getOrDefault("records-per-subscription", "1000")),
                Integer.parseInt(values.getOrDefault("parallelism", "1")),
                values.getOrDefault("phase", "initial"),
                Boolean.parseBoolean(restored),
                values.getOrDefault("entry-point", "datastream"),
                Boolean.parseBoolean(deduplication),
                Integer.parseInt(values.getOrDefault("replay-delay-millis", "0")),
                Long.parseLong(values.getOrDefault("max-outstanding-messages", "1000")),
                Integer.parseInt(values.getOrDefault("hold-after-checkpoint-millis", "0")));
    }

    String input(int index) {
        if (index != 0 && index != 1) {
            throw new IllegalArgumentException("Input index must be 0 or 1");
        }
        return "t3-" + runId + "-in-" + index;
    }

    String output() {
        return "t3-" + runId + "-out";
    }

    /**
     * Whether a tagged input belongs to the replay cohort, the middle third of each input's
     * sequence domain, split as the supervisor splits it: at {@code floor(records / 3)} and {@code
     * floor(2 * records / 3)}.
     */
    boolean inReplayCohort(String taggedInput) {
        String[] fields = taggedInput.split("\\|", -1);
        int sequence = Integer.parseInt(fields[3]);
        return records / 3 <= sequence && sequence < 2 * records / 3;
    }

    String identity() {
        return "v1|" + PROJECT + "|" + runId + "|" + records;
    }
}
