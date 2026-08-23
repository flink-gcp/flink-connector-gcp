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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.bigtable.v2.ReadChangeStreamRequest;
import com.google.bigtable.v2.RowRange;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the settings and queries {@link DataClientChangeStreamOpener} builds, and for its
 * lifecycle.
 *
 * <p>The application profile has its own test here for the same reason its scan siblings carry one:
 * an opener that simply never passed the profile on would leave the shared {@code
 * BigtableDataClients} test green while streaming through the wrong compute.
 */
@Timeout(30)
class DataClientChangeStreamOpenerTest {

    private static final TableDestination TABLE =
            TableDestination.of("project", "instance", "table");

    @Test
    void injectsTheRuntimeCredentialProvider() throws Exception {
        DataClientChangeStreamOpener opener = new DataClientChangeStreamOpener("profile");
        NoCredentialsProvider provider = NoCredentialsProvider.create();
        opener.useCredentials(provider);

        assertThat(opener.settings(TABLE).getStubSettings().getCredentialsProvider())
                .isSameAs(provider);
    }

    @Test
    void carriesTheApplicationProfileToTheClient() throws Exception {
        DataClientChangeStreamOpener opener = new DataClientChangeStreamOpener("single-cluster");

        assertThat(opener.settings(TABLE).getProjectId()).isEqualTo("project");
        assertThat(opener.settings(TABLE).getInstanceId()).isEqualTo("instance");
        assertThat(opener.settings(TABLE).getAppProfileId()).isEqualTo("single-cluster");
    }

    @Test
    void travelsInTheJobGraph() throws Exception {
        DataClientChangeStreamOpener opener = new DataClientChangeStreamOpener("single-cluster");

        DataClientChangeStreamOpener back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(opener), getClass().getClassLoader());

        assertThat(back.settings(TABLE).getAppProfileId()).isEqualTo("single-cluster");
    }

    @Test
    void theBuilderWiresTheApplicationProfileIntoThisSeam() throws Exception {
        // The change-stream half of the gap the scan seams pin: the builder constructs the
        // default opener, so a profile that never reached it would stream through the instance's
        // default compute while the tests above stay green.
        DataClientChangeStreamOpener opener =
                (DataClientChangeStreamOpener)
                        TestSources.changeStreamConfig(
                                        builder -> builder.appProfileId("boost-profile"))
                                .getOpener();

        assertThat(opener.settings(TestSources.TABLE).getAppProfileId()).isEqualTo("boost-profile");
    }

    @Test
    void closingWithoutHavingOpenedReleasesNothingAndFailsNothing() {
        assertThatCode(() -> new DataClientChangeStreamOpener("profile").close())
                .doesNotThrowAnyException();
    }

    @Test
    void refusesToBuildAClientAfterItHasBeenClosed() throws IOException {
        DataClientChangeStreamOpener opener = new DataClientChangeStreamOpener("profile");
        opener.close();

        assertThatThrownBy(() -> opener.open(TABLE, wholeKeyspace(), null, new DroppingObserver()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Bigtable change-stream opener")
                .hasMessageContaining("was closed before it was used");
    }

    @Test
    void openQuerySendsExplicitClosedOpenEmptyKeysForTheWholeKeyspace() {
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-0",
                        ByteStringRange.unbounded(),
                        Collections.emptyList(),
                        Instant.parse("2026-08-12T00:00:00Z"));

        ReadChangeStreamRequest request =
                DataClientChangeStreamOpener.query(
                                TableDestination.of("project", "instance", "table"), split, null)
                        .toProto(RequestContext.create("project", "instance", "profile"));
        RowRange partition = request.getPartition().getRowRange();

        assertThat(partition.getStartKeyCase()).isEqualTo(RowRange.StartKeyCase.START_KEY_CLOSED);
        assertThat(partition.getStartKeyClosed()).isEqualTo(ByteString.EMPTY);
        assertThat(partition.getEndKeyCase()).isEqualTo(RowRange.EndKeyCase.END_KEY_OPEN);
        assertThat(partition.getEndKeyOpen()).isEqualTo(ByteString.EMPTY);
    }

    /**
     * The five seconds are documented as a fixed value on three pages and in ADR-0103, so this
     * asserts on the protobuf the client sends rather than on the constant, which would pass
     * whatever the request carried.
     */
    @Test
    void openQueryAsksTheServiceForTheDocumentedFiveSecondHeartbeat() {
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-0",
                        ByteStringRange.unbounded(),
                        Collections.emptyList(),
                        Instant.parse("2026-08-12T00:00:00Z"));

        ReadChangeStreamRequest request =
                DataClientChangeStreamOpener.query(
                                TableDestination.of("project", "instance", "table"), split, null)
                        .toProto(RequestContext.create("project", "instance", "profile"));

        assertThat(request.getHeartbeatDuration().getSeconds()).isEqualTo(5);
        assertThat(request.getHeartbeatDuration().getNanos()).isZero();
    }

    private static ChangeStreamPartitionSplit wholeKeyspace() {
        return new ChangeStreamPartitionSplit(
                "change-stream-0",
                ByteStringRange.unbounded(),
                Collections.emptyList(),
                Instant.parse("2026-08-12T00:00:00Z"));
    }

    private static final class DroppingObserver implements ResponseObserver<ChangeStreamRecord> {

        @Override
        public void onStart(StreamController controller) {}

        @Override
        public void onResponse(ChangeStreamRecord response) {}

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onComplete() {}
    }
}
