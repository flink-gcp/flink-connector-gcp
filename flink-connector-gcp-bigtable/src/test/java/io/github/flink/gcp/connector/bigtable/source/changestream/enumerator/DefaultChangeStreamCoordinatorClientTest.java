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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the coordinator client's own logic.
 *
 * <p>Each case calls the method the production path calls, with the value the client library would
 * have returned. That is the point of the shape: an earlier version of this test drove a fake
 * {@code Operations} interface the class branched on, so every assertion here covered the fake's
 * branch and none covered the statement that runs in a job — a control that reverted the fold below
 * in only the production branch passed. The RPCs themselves are not reachable from a unit test and
 * are not pretended to be; what a gated run covers is in {@code
 * BigtableChangeStreamSourceRealGcpITCase}.
 */
class DefaultChangeStreamCoordinatorClientTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "instance", "table");

    /**
     * Pins the guard that makes the lazy accessors safe against a teardown that overtakes them.
     *
     * <p>{@code volatile} alone would not: the accessors are a check-then-create, so a teardown
     * between the check and the assignment closed nothing and left the client the caller then
     * assigned owned by no one. The reconciliation scan runs on the {@code callAsync} executor and
     * {@code close()} on the coordinator thread, so that ordering is reachable.
     */
    @Test
    void refusesToBuildAClientAfterItHasBeenClosed() throws Exception {
        DefaultChangeStreamCoordinatorClient client =
                new DefaultChangeStreamCoordinatorClient(
                        DESTINATION, "single-cluster", NoCredentialsProvider.create());

        client.close();

        assertThatThrownBy(client::generateInitialPartitions)
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("was closed before it was used")
                .hasMessageContaining(DESTINATION.toString());
        assertThatThrownBy(client::retention).hasMessageContaining("was closed before it was used");
        assertThatThrownBy(client::validateSingleClusterAppProfile)
                .hasMessageContaining("was closed before it was used");
    }

    @Test
    void acceptsSingleClusterRouting() {
        assertThatCode(
                        () ->
                                DefaultChangeStreamCoordinatorClient.checkSingleClusterRouting(
                                        singleClusterProfile(), "profile"))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsMultiClusterRouting() {
        assertThatThrownBy(
                        () ->
                                DefaultChangeStreamCoordinatorClient.checkSingleClusterRouting(
                                        multiClusterProfile(), "profile"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multi-cluster routing");
    }

    @Test
    void convertsChangeStreamRetention() {
        assertThat(
                        DefaultChangeStreamCoordinatorClient.retentionOf(
                                tableWithRetention(123, 456), DESTINATION))
                .isEqualTo(java.time.Duration.ofSeconds(123, 456));
    }

    @Test
    void rejectsATableWithoutChangeStreams() {
        Table withoutChangeStream =
                Table.fromProto(
                        com.google.bigtable.admin.v2.Table.newBuilder()
                                .setName("projects/project/instances/instance/tables/table")
                                .build());

        assertThatThrownBy(
                        () ->
                                DefaultChangeStreamCoordinatorClient.retentionOf(
                                        withoutChangeStream, DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Change Streams is not enabled");
    }

    @Test
    void rejectsAnEmptyInitialPartitionResponse() {
        assertThatThrownBy(
                        () ->
                                DefaultChangeStreamCoordinatorClient.foldInitialPartitions(
                                        Collections.emptyList(), DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no initial Change Streams partitions");
    }

    @Test
    void foldsTheEmptyKeyBoundsTheServiceUsesIntoUnboundedOnes() {
        // GenerateInitialChangeStreamPartitionsUserCallable hands every partition to
        // ByteStringRange.create(start_key_closed, end_key_open), and create — unlike the
        // startClosed/endOpen setters — leaves an empty key as a bounded one. So a table's first
        // partition arrives closed at the empty key and its last open at it. Everything downstream
        // reads bound types, and the value types it flows into normalise on construction, so an
        // unfolded partition compares unequal to its own remembered copy and reads as a range that
        // begins or ends at the smallest key there is. This is the only place that can fold it.
        assertThat(
                        DefaultChangeStreamCoordinatorClient.foldInitialPartitions(
                                Arrays.asList(
                                        ByteStringRange.create(
                                                ByteString.EMPTY, ByteString.copyFromUtf8("m")),
                                        ByteStringRange.create(
                                                ByteString.copyFromUtf8("m"), ByteString.EMPTY)),
                                DESTINATION))
                .containsExactly(
                        ByteStringRange.unbounded().endOpen("m"),
                        ByteStringRange.unbounded().startClosed("m"));
    }

    @Test
    void closingBeforeAnyClientWasBuiltReleasesNothingAndFailsNothing() {
        assertThatCode(
                        () ->
                                new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile")
                                        .close())
                .doesNotThrowAnyException();
    }

    @Test
    void injectsOneRuntimeProviderIntoEveryCoordinatorClientFamily() throws Exception {
        NoCredentialsProvider provider = NoCredentialsProvider.create();
        DefaultChangeStreamCoordinatorClient client =
                new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile", provider);

        assertThat(client.dataSettings().getStubSettings().getCredentialsProvider())
                .isSameAs(provider);
        assertThat(client.tableAdminSettings().getCredentialsProvider()).isSameAs(provider);
        assertThat(client.instanceAdminSettings().getCredentialsProvider()).isSameAs(provider);
    }

    @Test
    void aReadableProfileIsCheckedRatherThanSkipped() {
        DefaultChangeStreamCoordinatorClient client =
                new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile");

        assertThatThrownBy(() -> client.validateSingleClusterAppProfile(this::multiClusterProfile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multi-cluster routing");
    }

    @Test
    void unreadableAppProfileMetadataLeavesTheCheckToBigtable() throws Exception {
        DefaultChangeStreamCoordinatorClient client =
                new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile");

        // Preflight must not fail — a data-plane-only principal can stream without reading profile
        // metadata — but the skip is invisible unless it says so, which is the whole of the arm.
        try (LogCapture capture = LogCapture.of(DefaultChangeStreamCoordinatorClient.class)) {
            client.validateSingleClusterAppProfile(
                    () -> {
                        throw new PermissionDeniedException(
                                new RuntimeException("denied"),
                                GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                                false);
                    });

            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(message -> assertThat(message).contains("profile"));
        }
    }

    @Test
    void aLookupFailureThatIsNotAPermissionDenialIsNotSwallowed() {
        DefaultChangeStreamCoordinatorClient client =
                new DefaultChangeStreamCoordinatorClient(DESTINATION, "profile");

        assertThatThrownBy(
                        () ->
                                client.validateSingleClusterAppProfile(
                                        () -> {
                                            throw new java.io.IOException("instance admin is down");
                                        }))
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("instance admin is down");
    }

    /**
     * The ASCII cases are what an emptiness check let through; U+2028 is what tells the surviving
     * idioms apart. {@code Character.isWhitespace} calls it whitespace and {@code String.trim()}
     * leaves it alone, so only that assertion fails if this check returns to {@code
     * trim().isEmpty()} — the same discrimination {@code BigtableSourceBuilderTest} and {@code
     * BigtableChangeStreamSourceBuilderTest} make for the identical rule.
     */
    @Test
    void rejectsABlankApplicationProfileId() {
        for (String blank : new String[] {"", " ", "\t", "\u2028"}) {
            assertThatThrownBy(() -> new DefaultChangeStreamCoordinatorClient(DESTINATION, blank))
                    .as("appProfileId code points %s", Arrays.toString(blank.chars().toArray()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("appProfileId must not be blank");
        }
    }

    private AppProfile singleClusterProfile() {
        return AppProfile.fromProto(
                com.google.bigtable.admin.v2.AppProfile.newBuilder()
                        .setName("projects/project/instances/instance/appProfiles/profile")
                        .setSingleClusterRouting(
                                com.google.bigtable.admin.v2.AppProfile.SingleClusterRouting
                                        .newBuilder()
                                        .setClusterId("cluster"))
                        .build());
    }

    private AppProfile multiClusterProfile() {
        return AppProfile.fromProto(
                com.google.bigtable.admin.v2.AppProfile.newBuilder()
                        .setName("projects/project/instances/instance/appProfiles/profile")
                        .setMultiClusterRoutingUseAny(
                                com.google.bigtable.admin.v2.AppProfile.MultiClusterRoutingUseAny
                                        .getDefaultInstance())
                        .build());
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
}
