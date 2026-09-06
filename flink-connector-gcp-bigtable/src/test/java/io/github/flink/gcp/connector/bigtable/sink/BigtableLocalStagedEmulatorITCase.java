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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SDK transport against the owned emulator, including restored request reconstruction. */
class BigtableLocalStagedEmulatorITCase extends AbstractBigtableEmulatorITCase {
    @TempDir Path directory;

    @Test
    void sameRowMarkersSurviveStopAndRestoreThroughTheProductionClientFactory() throws Exception {
        TableDestination table =
                createTable("staged-local-markers", FAMILY, StagedMutationTestSink.MARKER_FAMILY);
        try (LocalStagedHarness run =
                new LocalStagedHarness(table, localEndpoint(), 1024, true, false, 4)) {
            String savepoint;
            try (LocalStagedJob initial =
                    new LocalStagedJob(
                            run, directory, true, true, 2, 40, 3_600_000, true, null, false)) {
                initial.awaitAdmissions(40);
                assertThat(readRows(table)).isEmpty();
                savepoint = initial.savepoint(directory, true);
            }
            Set<ByteString> markers =
                    run.attempts.stream()
                            .map(
                                    request ->
                                            request.getFalseMutations(
                                                            request.getFalseMutationsCount() - 1)
                                                    .getSetCell()
                                                    .getColumnQualifier())
                            .collect(Collectors.toSet());
            assertThat(markers).hasSize(40);
            assertMarkersAndValues(table, run, markers);
            try (LocalStagedJob restored =
                    new LocalStagedJob(
                            run, directory, true, true, 3, 40, 100, true, savepoint, false)) {
                restored.finish();
            }
            assertThat(run.attempts).hasSize(80);
            assertMarkersAndValues(table, run, markers);
            assertThat(run.staged.get()).isEqualTo(40);
        }
    }

    @Test
    void existingBulkPathUsesTheSamePayloadsAndReportsEveryAcknowledgement() throws Exception {
        TableDestination table = createTable("bulk-local-observation");
        try (LocalStagedHarness run =
                        new LocalStagedHarness(table, localEndpoint(), 65536, false, false, 4);
                LocalStagedJob job =
                        new LocalStagedJob(
                                run, directory, false, true, 2, 40, 100, false, null, false)) {
            job.finish();
            assertThat(run.acknowledgements).hasSize(40);
            assertThat(readRows(table))
                    .hasSize(40)
                    .allSatisfy(
                            row -> {
                                long sequence =
                                        row.getCells(FAMILY, "q")
                                                .get(0)
                                                .getValue()
                                                .asReadOnlyByteBuffer()
                                                .getLong();
                                assertThat(row.getCells(FAMILY, "q").get(0).getValue())
                                        .isEqualTo(
                                                run.input(sequence)
                                                        .mutations()
                                                        .get(0)
                                                        .getSetCell()
                                                        .getValue());
                            });
        }
    }

    private static String localEndpoint() {
        assertThat(EMULATOR.getHost()).isIn("localhost", "127.0.0.1");
        return "127.0.0.1:" + EMULATOR.getEmulatorPort();
    }

    private static void assertMarkersAndValues(
            TableDestination table, LocalStagedHarness run, Set<ByteString> expected) {
        Set<ByteString> actual = new HashSet<>();
        assertThat(readRows(table))
                .hasSize(5)
                .allSatisfy(
                        row -> {
                            row.getCells(StagedMutationTestSink.MARKER_FAMILY)
                                    .forEach(
                                            cell -> {
                                                assertThat(cell.getTimestamp()).isZero();
                                                assertThat(cell.getValue())
                                                        .isEqualTo(ByteString.copyFromUtf8("1"));
                                                assertThat(actual.add(cell.getQualifier()))
                                                        .isTrue();
                                            });
                            long sequence =
                                    row.getCells(FAMILY, "q")
                                            .get(0)
                                            .getValue()
                                            .asReadOnlyByteBuffer()
                                            .getLong();
                            assertThat(sequence).isBetween(0L, 39L);
                            assertThat(row.getKey()).isEqualTo(run.input(sequence).row());
                            assertThat(row.getCells(FAMILY, "q").get(0).getValue())
                                    .isEqualTo(
                                            run.input(sequence)
                                                    .mutations()
                                                    .get(0)
                                                    .getSetCell()
                                                    .getValue());
                        });
        assertThat(actual).isEqualTo(expected);
    }
}
