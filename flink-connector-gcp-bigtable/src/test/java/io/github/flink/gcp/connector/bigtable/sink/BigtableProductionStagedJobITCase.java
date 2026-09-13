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

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(120)
class BigtableProductionStagedJobITCase {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void stopSavepointRescalesProductionEnvelopesWithoutSerializingThemAgain(int parallelism)
            throws Exception {
        try (var service = new StagedRpcTestService();
                var run =
                        new LocalStagedHarness(
                                TableDestination.of("p", "i", "t"),
                                "127.0.0.1:1",
                                32,
                                true,
                                true,
                                2)) {
            String savepoint;
            try (var job = job(run, service, 2, 60000, null, false)) {
                job.awaitAdmissions(24);
                assertThat(applied(service)).isZero();
                savepoint = job.savepoint(directory.resolve("stop"), true);
                assertThat(applied(service)).isEqualTo(24);
            }
            int serializations = run.staged.get();
            try (var restored = job(run, service, parallelism, 1000, savepoint, false)) {
                restored.finish();
                assertThat(run.staged.get()).isEqualTo(serializations);
                synchronized (service.probe) {
                    assertThat(service.probe.applied).isEqualTo(24);
                    assertThat(totalSum(service)).isEqualTo(24);
                    assertThat(service.probe.deduplicated).isEqualTo(24);
                }
            }
        }
    }

    private LocalStagedJob job(
            LocalStagedHarness run,
            StagedRpcTestService service,
            int parallelism,
            long interval,
            String restore,
            boolean restart)
            throws Exception {
        String runId = run.id;
        Sink<Long> sink =
                BigtableSink.<Long>builder()
                        .table(TableDestination.of("p", "i", "t"))
                        .emulatorEndpoint("127.0.0.1:" + service.server.getPort())
                        .appProfileId("transactional")
                        .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                        .stagedOptions(
                                BigtableStagedOptions.builder()
                                        .markerFamily("flink_commit")
                                        .build())
                        .serializer(
                                (number, context) -> {
                                    LocalStagedHarness active = LocalStagedHarness.run(runId);
                                    active.admitted(number);
                                    active.staged.incrementAndGet();
                                    var input = active.input(number);
                                    return RowMutationEntry.createFromMutationUnsafe(
                                            input.row(),
                                            com.google.cloud.bigtable.data.v2.models.Mutation
                                                    .fromProtoUnsafe(input.mutations()));
                                })
                        .build();
        return new LocalStagedJob(
                run,
                directory,
                true,
                false,
                parallelism,
                24,
                interval,
                true,
                restore,
                restart,
                sink);
    }

    private static long totalSum(StagedRpcTestService service) {
        synchronized (service.probe) {
            return service.probe.cells.values().stream()
                    .mapToLong(cells -> Long.parseLong(cells.get("agg:count").toStringUtf8()))
                    .sum();
        }
    }

    private static int applied(StagedRpcTestService service) {
        synchronized (service.probe) {
            return service.probe.applied;
        }
    }
}
