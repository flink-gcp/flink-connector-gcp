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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PubSubRecoveryReportTest {
    final RecoveryOptions options = new RecoveryOptions("oracle", 1, 1, "initial", false);

    String observation(int input, String message) {
        return RecoveryPayload.observation(
                options,
                RecoveryPayload.input(options, input, 0) + "|" + RecoveryPayload.encode(message),
                UUID.randomUUID().toString(),
                false);
    }

    @Test
    void distinguishesFourDuplicatePopulations() {
        PubSubRecoveryReport report = new PubSubRecoveryReport(options);
        String first = observation(0, "input-a");
        report.accept("output-a", first);
        report.accept("output-a", first);
        report.accept("output-b", first);
        report.accept("output-c", observation(0, "input-a"));
        report.accept("output-d", observation(0, "input-b"));
        // Message IDs are scoped by input topic; the same opaque value on the other input is
        // distinct.
        report.accept("output-e", observation(1, "input-a"));
        assertThat(report.complete())
                .isEqualTo(
                        "logical_inputs=2 input_publication_duplicates=1 repeated_input_processing=1 output_publication_duplicates=1 repeated_output_delivery=1");
    }

    @Test
    void missingWrongAndConflictingEvidenceCannotPass() {
        PubSubRecoveryReport report = new PubSubRecoveryReport(options);
        String first = observation(0, "a");
        report.accept("a", first);
        assertThatThrownBy(report::complete).hasMessageContaining("Missing logical");
        assertThatThrownBy(() -> report.accept("a", observation(1, "b")))
                .hasMessageContaining("conflicting");
        assertThatThrownBy(
                        () ->
                                new PubSubRecoveryReport(options)
                                        .accept("id", first.replace("|oracle|", "|foreign|")))
                .hasMessageContaining("run");
        String[] fields = first.split("\\|", -1);
        fields[8] = "true";
        assertThatThrownBy(() -> report.accept("b", String.join("|", fields)))
                .hasMessageContaining("conflicting");
        assertThatThrownBy(() -> new PubSubRecoveryReport(options).accept("", first))
                .hasMessageContaining("message ID");
    }

    @Test
    void rejectsImpossibleAttemptStateAndOversizedEvidence() {
        String first = observation(0, "a");
        assertThatThrownBy(
                        () ->
                                new PubSubRecoveryReport(options)
                                        .accept(
                                                "id",
                                                first.replace("|initial|false", "|upgrade|false")))
                .hasMessageContaining("Invalid");
        PubSubRecoveryReport report = new PubSubRecoveryReport(options);
        report.accept("id", first);
        String[] changed = observation(1, "b").split("\\|", -1);
        changed[5] = first.split("\\|", -1)[5];
        changed[8] = "true";
        assertThatThrownBy(() -> report.accept("other", String.join("|", changed)))
                .hasMessageContaining("conflicting");
        assertThatThrownBy(() -> new PubSubRecoveryReport(options).accept("id", "x".repeat(2049)))
                .hasMessageContaining("bound");
        PubSubRecoveryReport bounded = new PubSubRecoveryReport(options);
        for (int i = 0; i < 200000; i++) {
            bounded.accept("id", first);
        }
        assertThatThrownBy(() -> bounded.accept("id", first)).hasMessageContaining("bound");
    }

    @Test
    void cliReadsCollectedRecordsAndRejectsMalformedInput(@TempDir Path temporary)
            throws Exception {
        Path file = temporary.resolve("evidence.tsv");
        Files.write(
                file,
                List.of(
                        RecoveryPayload.encode("id-0")
                                + "\t"
                                + RecoveryPayload.encode(observation(0, "a")),
                        RecoveryPayload.encode("id-1")
                                + "\t"
                                + RecoveryPayload.encode(observation(1, "b"))));
        PubSubRecoveryReport.main(
                new String[] {file.toString(), "--run-id=oracle", "--records-per-subscription=1"});
        Files.writeString(file, "malformed\n");
        assertThatThrownBy(
                        () ->
                                PubSubRecoveryReport.main(
                                        new String[] {file.toString(), "--run-id=oracle"}))
                .hasMessageContaining("base64url");
        Files.writeString(file, "\t".repeat(4097));
        assertThatThrownBy(
                        () ->
                                PubSubRecoveryReport.main(
                                        new String[] {file.toString(), "--run-id=oracle"}))
                .hasMessageContaining("4096 characters");
    }
}
