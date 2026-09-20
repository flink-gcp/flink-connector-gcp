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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

/** Versioned input and output wire records; every processing call keeps its own observation ID. */
@Internal
final class RecoveryPayload {
    private RecoveryPayload() {}

    static String input(RecoveryOptions options, int subscription, int sequence) {
        check(options, subscription, sequence);
        return "v1|" + options.runId + "|" + subscription + "|" + sequence;
    }

    static int[] parseInput(RecoveryOptions options, String text) {
        String[] fields = text.split("\\|", -1);
        if (fields.length != 4 || !fields[0].equals("v1") || !fields[1].equals(options.runId)) {
            throw new IllegalArgumentException("Input must belong to this version and run");
        }
        int subscription = Integer.parseInt(fields[2]);
        int sequence = Integer.parseInt(fields[3]);
        if (!text.equals(input(options, subscription, sequence))) {
            throw new IllegalArgumentException("Input identity must use canonical decimal fields");
        }
        return new int[] {subscription, sequence};
    }

    static void check(RecoveryOptions options, int subscription, int sequence) {
        options.input(subscription);
        if (sequence < 0 || sequence >= options.records) {
            throw new IllegalArgumentException(
                    "Sequence is outside this run's logical input domain");
        }
    }

    static String encode(String value) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    static String decode(String value) {
        String decoded = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
        if (!encode(decoded).equals(value)) {
            throw new IllegalArgumentException("Noncanonical UTF-8/base64 field");
        }
        return decoded;
    }

    static String observation(
            RecoveryOptions options, String taggedInput, String attempt, boolean restored) {
        String[] fields = taggedInput.split("\\|", -1);
        if (fields.length != 5 || decode(fields[4]).isEmpty()) {
            throw new IllegalArgumentException("Input message ID is required");
        }
        parseInput(options, String.join("|", fields[0], fields[1], fields[2], fields[3]));
        return taggedInput
                + "|"
                + attempt
                + "|"
                + UUID.randomUUID()
                + "|"
                + options.phase
                + "|"
                + restored;
    }
}
