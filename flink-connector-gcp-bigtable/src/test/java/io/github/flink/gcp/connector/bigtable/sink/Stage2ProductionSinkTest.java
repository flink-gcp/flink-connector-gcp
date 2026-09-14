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

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2ProductionSinkTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void expiredOrForeignLeaseStopsMetadataAndSubmissionBeforeAnySdkCall(boolean expired)
            throws Exception {
        Stage2Lease lease = Stage2Lease.plan(directory.resolve("lease.properties"));
        lease.update(
                properties -> {
                    properties.setProperty("phase", "ACTIVE");
                    properties.setProperty("startedAt", Long.toString(System.currentTimeMillis()));
                });
        TableDestination table =
                expired
                        ? lease.table("staged-r1")
                        : TableDestination.of("flink-gcp", "not-owned", "staged-r1");
        AtomicInteger metadataReads = new AtomicInteger();
        try (Stage2Harness run =
                        new Stage2Harness(
                                table,
                                "127.0.0.1:1",
                                directory.resolve("inventory"),
                                8,
                                1024,
                                false,
                                false,
                                1,
                                false,
                                lease);
                var committer =
                        new Stage2ProductionSink(run, true)
                                .createCommitter(
                                        context(),
                                        (destination, profile, marker, families) -> {
                                            metadataReads.incrementAndGet();
                                            throw new AssertionError(
                                                    "Metadata delegate must not be reached");
                                        })) {
            if (expired) {
                lease.update(properties -> properties.setProperty("startedAt", "1"));
            }
            var value =
                    BigtableCommittable.stage(
                            table,
                            LocalStagedHarness.PROFILE,
                            StagedMutationTestSink.MARKER_FAMILY,
                            RowMutationEntry.create("r").setCell("cf", "q", 1000, "v").toProto(),
                            new SecureRandom());
            String rejection = expired ? "execution deadline expired" : "not owned";
            assertThatThrownBy(() -> committer.commit(List.of(new Request(value))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(rejection);
            assertThatThrownBy(() -> run.beforeProductionSend(1024))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(rejection);
            assertThat(metadataReads).hasValue(0);
            assertThat(run.attemptsCount).hasValue(0);
            assertThat(run.wireBytes).hasValue(0);
            assertThat(run.active).hasValue(0);
        }
    }

    private static CommitterInitContext context() {
        return new CommitterInitContext() {
            private final SinkCommitterMetricGroup metrics = TestSinkCommitterMetricGroup.create();

            @Override
            public SinkCommitterMetricGroup metricGroup() {
                return metrics;
            }

            @Override
            public OptionalLong getRestoredCheckpointId() {
                return OptionalLong.empty();
            }

            @Override
            public JobInfo getJobInfo() {
                throw new AssertionError("Unused job info");
            }

            @Override
            public TaskInfo getTaskInfo() {
                throw new AssertionError("Unused task info");
            }
        };
    }

    private static final class Request implements CommitRequest<BigtableCommittable> {
        private final BigtableCommittable value;

        Request(BigtableCommittable value) {
            this.value = value;
        }

        @Override
        public BigtableCommittable getCommittable() {
            return value;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalAlreadyCommitted() {
            throw new AssertionError("Must reject before a send");
        }

        @Override
        public void retryLater() {
            throw new AssertionError("Must fail the commit");
        }

        @Override
        public void updateAndRetryLater(BigtableCommittable value) {
            throw new AssertionError("Must retain identity");
        }

        @Override
        public void signalFailedWithKnownReason(Throwable failure) {
            throw new AssertionError("Must fail the commit");
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable failure) {
            throw new AssertionError("Must fail the commit");
        }
    }
}
