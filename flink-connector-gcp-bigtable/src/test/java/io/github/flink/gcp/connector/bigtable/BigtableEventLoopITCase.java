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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClientV2;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.DefaultMutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.MutationBatcher;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.DataClientRowKeySampler;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connector's Bigtable clients run on its own gRPC event-loop group and stop it when they close
 * (ADR-0208, #2264).
 *
 * <p>gRPC's shared group is destroyed a second after its last channel closes, which for a job-jar
 * deployment can be after Flink has closed the job's class loader; under {@code
 * cluster.uncaught-exception-handling: FAIL} that exits the TaskManager or JobManager. The emulator
 * arms are the ones a test can reach; the production arms apply the same lease to the SDK's own
 * provider, which {@code BigtableClientsTest} covers. The harness's own clients run on gRPC's
 * group, not this one.
 */
class BigtableEventLoopITCase extends AbstractBigtableEmulatorITCase {

    /**
     * How long a check before a test waits for a group an earlier class left running: a MiniCluster
     * job that class cancelled closes its clients after it has returned, each within its own
     * shutdown budget.
     */
    private static final Duration LEFTOVER_TEARDOWN = Duration.ofSeconds(60);

    /**
     * A group left running by an earlier class in this reused fork would make the checks below pass
     * or fail for the wrong reason, so it is ruled out first.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(EventLoopObserver.awaitNoGroup(LEFTOVER_TEARDOWN))
                .as(
                        "no lease and no %s* thread before the test: an earlier class in this fork"
                                + " left the group running",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void theSinksClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        TableDestination destination = createTable("event-loop-sink");
        DefaultMutationBatcherFactory factory =
                new DefaultMutationBatcherFactory(
                        null, BigtableWriterOptions.builder().build(), endpoint(), null);
        try {
            MutationBatcher batcher = factory.create(destination);
            try {
                var written = batcher.add(RowMutationEntry.create("k1").setCell(FAMILY, "q", "v"));
                batcher.sendOutstanding();
                written.get(30, TimeUnit.SECONDS);

                assertThat(GrpcEventLoopThreads.alive())
                        .as(
                                "a %s* thread while the sink's client is open",
                                GrpcEventLoopThreads.PREFIX)
                        .isTrue();
            } finally {
                batcher.close();
                factory.release(destination);
            }
        } finally {
            factory.close();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the factory closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(readRows(destination)).hasSize(1);
    }

    @Test
    void aReadersClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        TableDestination destination = createTable("event-loop-reader");
        seedRows(destination, "k1");
        LazyBigtableDataClient holder =
                new LazyBigtableDataClient("event-loop reader", null, endpoint(), true);
        try {
            assertThat(holder.get(destination).readRow(TableId.of(destination.getTable()), "k1"))
                    .isNotNull();

            assertThat(GrpcEventLoopThreads.alive())
                    .as(
                            "a %s* thread while the reader's client is open",
                            GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            holder.close();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the reader's client closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    /**
     * The JobManager-side rule: the sampler's client closes when the sample returns, not when the
     * enumerator closes the sampler, so nothing is left open on the group however late that is.
     */
    @Test
    void theSamplersClientClosesWhenTheSampleReturns() throws Exception {
        TableDestination destination = createTable("event-loop-sampler");
        seedRows(destination, "k1", "k2");
        DataClientRowKeySampler sampler = new DataClientRowKeySampler(null, endpoint());
        try {
            assertThat(
                            EventLoopObserver.sawGroupDuring(
                                    () -> assertThat(sampler.sample(destination)).isNotEmpty()))
                    .as("a %s* thread while the sample ran", GrpcEventLoopThreads.PREFIX)
                    .isTrue();

            // Nothing else holds a lease, so the sample's own close is what stopped the group.
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the sample returned", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            sampler.close();
        }
    }

    /**
     * The admin clients' emulator arm runs on a plaintext channel opened on the group for the
     * client, and closing the client releases it with the call.
     */
    @Test
    void theTableAdminsClientRunsOnTheGroupAndClosesWithTheCall() throws Exception {
        TableDestination destination = createTable("event-loop-table-admin");
        BigtableTableAdmin admin = new BigtableTableAdmin(endpoint());

        assertThat(
                        EventLoopObserver.sawGroupDuring(
                                () -> {
                                    admin.validateFamilies(destination, Map.of(), false);
                                    return null;
                                }))
                .as("a %s* thread while the table was read", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the read returned", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    /**
     * The instance admin has the same arm. The emulator serves no application profiles, so the call
     * fails; reaching the emulator is what puts the channel on the group.
     */
    @Test
    void theInstanceAdminsClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        AtomicReference<ApiException> answer = new AtomicReference<>();
        assertThat(
                        EventLoopObserver.sawGroupDuring(
                                () -> {
                                    try (GrpcEventLoops.LeasedClient<BigtableInstanceAdminClientV2>
                                            client =
                                                    BigtableClients.openInstanceAdmin(
                                                            BigtableClients.instanceAdminSettings(
                                                                    endpoint(), null),
                                                            endpoint())) {
                                        client.client()
                                                .getAppProfile(
                                                        GetAppProfileRequest.newBuilder()
                                                                .setName(
                                                                        "projects/"
                                                                                + PROJECT
                                                                                + "/instances/"
                                                                                + INSTANCE
                                                                                + "/appProfiles/"
                                                                                + "default")
                                                                .build());
                                    } catch (ApiException expected) {
                                        answer.set(expected);
                                    }
                                    return null;
                                }))
                .as("a %s* thread while the instance admin was open", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the instance admin closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        // The emulator's answer, which Bigtable itself would not give: the call reached it.
        assertThat(answer.get()).as("the emulator's answer").isNotNull();
        assertThat(answer.get().getStatusCode().getCode()).isEqualTo(StatusCode.Code.UNIMPLEMENTED);
    }

    private static EmulatorEndpoint endpoint() {
        return EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint");
    }
}
