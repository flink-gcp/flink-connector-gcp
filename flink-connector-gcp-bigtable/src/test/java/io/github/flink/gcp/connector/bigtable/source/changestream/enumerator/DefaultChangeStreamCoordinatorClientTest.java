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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.bigtable.admin.v2.ChangeStreamConfig;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultChangeStreamCoordinatorClientTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "instance", "table");

    @Test
    void acceptsSingleClusterRouting() throws Exception {
        FakeOperations operations = new FakeOperations();
        operations.appProfile =
                AppProfile.fromProto(
                        com.google.bigtable.admin.v2.AppProfile.newBuilder()
                                .setName("projects/project/instances/instance/appProfiles/profile")
                                .setSingleClusterRouting(
                                        com.google.bigtable.admin.v2.AppProfile.SingleClusterRouting
                                                .newBuilder()
                                                .setClusterId("cluster"))
                                .build());

        client(operations).validateSingleClusterAppProfile();
    }

    @Test
    void rejectsMultiClusterRouting() {
        FakeOperations operations = new FakeOperations();
        operations.appProfile =
                AppProfile.fromProto(
                        com.google.bigtable.admin.v2.AppProfile.newBuilder()
                                .setName("projects/project/instances/instance/appProfiles/profile")
                                .setMultiClusterRoutingUseAny(
                                        com.google.bigtable.admin.v2.AppProfile
                                                .MultiClusterRoutingUseAny.getDefaultInstance())
                                .build());

        assertThatThrownBy(() -> client(operations).validateSingleClusterAppProfile())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multi-cluster routing");
    }

    @Test
    void convertsChangeStreamRetention() throws Exception {
        FakeOperations operations = new FakeOperations();
        operations.table = tableWithRetention(123, 456);

        assertThat(client(operations).retention())
                .isEqualTo(java.time.Duration.ofSeconds(123, 456));
    }

    @Test
    void rejectsATableWithoutChangeStreams() {
        FakeOperations operations = new FakeOperations();
        operations.table =
                Table.fromProto(
                        com.google.bigtable.admin.v2.Table.newBuilder()
                                .setName("projects/project/instances/instance/tables/table")
                                .build());

        assertThatThrownBy(() -> client(operations).retention())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Change Streams is not enabled");
    }

    @Test
    void rejectsAnEmptyInitialPartitionResponse() {
        FakeOperations operations = new FakeOperations();

        assertThatThrownBy(() -> client(operations).generateInitialPartitions())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no initial Change Streams partitions");
    }

    @Test
    void foldsTheEmptyKeyBoundsTheServiceUsesIntoUnboundedOnes() throws Exception {
        // GenerateInitialChangeStreamPartitionsUserCallable hands every partition to
        // ByteStringRange.create(start_key_closed, end_key_open), and create — unlike the
        // startClosed/endOpen setters — leaves an empty key as a bounded one. So a table's first
        // partition arrives closed at the empty key and its last open at it. Everything downstream
        // reads bound types, and the value types it flows into normalise on construction, so an
        // unfolded partition compares unequal to its own remembered copy and reads as a range that
        // begins or ends at the smallest key there is. This is the only place that can fold it.
        FakeOperations operations = new FakeOperations();
        operations.partitions =
                Arrays.asList(
                        ByteStringRange.create(ByteString.EMPTY, ByteString.copyFromUtf8("m")),
                        ByteStringRange.create(ByteString.copyFromUtf8("m"), ByteString.EMPTY));

        assertThat(client(operations).generateInitialPartitions())
                .containsExactly(
                        ByteStringRange.unbounded().endOpen("m"),
                        ByteStringRange.unbounded().startClosed("m"));
    }

    @Test
    void closesAdapterResources() throws Exception {
        FakeOperations operations = new FakeOperations();

        client(operations).close();

        assertThat(operations.closed).isTrue();
    }

    @Test
    void injectsOneRuntimeProviderIntoEveryCoordinatorClientFamily() throws Exception {
        NoCredentialsProvider provider = NoCredentialsProvider.create();
        DefaultChangeStreamCoordinatorClient client =
                new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile", null, provider);

        assertThat(client.dataSettings().getStubSettings().getCredentialsProvider())
                .isSameAs(provider);
        assertThat(client.tableAdminSettings().getCredentialsProvider()).isSameAs(provider);
        assertThat(client.instanceAdminSettings().getCredentialsProvider()).isSameAs(provider);
    }

    @Test
    void unreadableAppProfileMetadataLeavesTheCheckToBigtable() throws Exception {
        FakeOperations operations = new FakeOperations();
        operations.appProfileFailure =
                new PermissionDeniedException(
                        new RuntimeException("denied"),
                        GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                        false);

        // Preflight must not fail — a data-plane-only principal can stream without reading profile
        // metadata — but the skip is invisible unless it says so, which is the whole of the arm.
        try (LogCapture capture = LogCapture.of(DefaultChangeStreamCoordinatorClient.class)) {
            client(operations).validateSingleClusterAppProfile();

            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(message -> assertThat(message).contains("profile"));
        }
    }

    @Test
    void rejectsAnEmptyApplicationProfileId() {
        assertThatThrownBy(() -> new DefaultChangeStreamCoordinatorClient(DESTINATION, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("appProfileId must not be empty");
    }

    private static DefaultChangeStreamCoordinatorClient client(FakeOperations operations) {
        return new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile", operations);
    }

    private static Table tableWithRetention(long seconds, int nanos) {
        return Table.fromProto(
                com.google.bigtable.admin.v2.Table.newBuilder()
                        .setName("projects/project/instances/instance/tables/table")
                        .setChangeStreamConfig(
                                ChangeStreamConfig.newBuilder()
                                        .setRetentionPeriod(
                                                Duration.newBuilder()
                                                        .setSeconds(seconds)
                                                        .setNanos(nanos)))
                        .build());
    }

    private static final class FakeOperations
            implements DefaultChangeStreamCoordinatorClient.Operations {

        private AppProfile appProfile;
        private RuntimeException appProfileFailure;
        private Table table;
        private List<ByteStringRange> partitions = Collections.emptyList();
        private boolean closed;

        @Override
        public AppProfile getAppProfile() {
            if (appProfileFailure != null) {
                throw appProfileFailure;
            }
            return appProfile;
        }

        @Override
        public Table getTable() {
            return table;
        }

        @Override
        public List<ByteStringRange> generateInitialPartitions() {
            return partitions;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
