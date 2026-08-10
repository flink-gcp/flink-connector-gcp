/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.spanner.source.batch;

import com.google.cloud.spanner.TestPartitions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerBatchEnumeratorState} and its serializer. */
class SpannerBatchEnumeratorStateSerializerTest {

    private final SpannerBatchEnumeratorStateSerializer serializer =
            new SpannerBatchEnumeratorStateSerializer();

    @Test
    void aPlanSurvivesTheRoundTrip() throws Exception {
        SpannerBatchEnumeratorState state = new SpannerBatchEnumeratorState(true, splits());

        SpannerBatchEnumeratorState restored = roundTrip(state);

        assertThat(restored).isEqualTo(state);
        assertThat(restored.isPlanned()).isTrue();
        assertThat(restored.getPendingSplits()).hasSize(2);
        assertThat(restored.getPendingSplits().get(1).splitId()).isEqualTo("1");
    }

    @Test
    void aPlanFullyHandedOutIsStillPlanned() throws Exception {
        // The state that would be indistinguishable from an unplanned one if `planned` were read
        // as "the queue is non-empty" — and the one whose restore must not plan again.
        SpannerBatchEnumeratorState restored =
                roundTrip(new SpannerBatchEnumeratorState(true, Collections.emptyList()));

        assertThat(restored.isPlanned()).isTrue();
        assertThat(restored.getPendingSplits()).isEmpty();
    }

    @Test
    void anUnplannedStateSurvivesTheRoundTrip() throws Exception {
        SpannerBatchEnumeratorState restored =
                roundTrip(new SpannerBatchEnumeratorState(false, Collections.emptyList()));

        assertThat(restored.isPlanned()).isFalse();
    }

    @Test
    void anUnplannedStateCannotHoldSplits() {
        assertThatThrownBy(() -> new SpannerBatchEnumeratorState(false, splits()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("an unplanned enumerator cannot hold pending splits");
    }

    @Test
    void thePendingSplitsAreCopiedAndUnmodifiable() {
        List<PartitionSplit> splits = new java.util.ArrayList<>(splits());
        SpannerBatchEnumeratorState state = new SpannerBatchEnumeratorState(true, splits);

        splits.clear();

        assertThat(state.getPendingSplits()).hasSize(2);
        assertThatThrownBy(() -> state.getPendingSplits().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void anUnknownVersionIsRefusedByName() throws Exception {
        byte[] bytes = serializer.serialize(new SpannerBatchEnumeratorState(true, splits()));

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Unsupported Spanner batch enumerator state serialization version");
    }

    @Test
    void aNegativeSplitCountIsRefusedRatherThanAllocated() {
        byte[] corrupt = new byte[] {1, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), corrupt))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative split count");
    }

    @Test
    void theTwoFormatsCarrySeparateVersions() {
        // The split serializer embeds through version-free helpers, so the two numbers move
        // independently; asserting they are both 1 today is what makes a bump deliberate.
        assertThat(serializer.getVersion()).isEqualTo(1);
        assertThat(new PartitionSplitSerializer().getVersion()).isEqualTo(1);
    }

    private SpannerBatchEnumeratorState roundTrip(SpannerBatchEnumeratorState state)
            throws IOException {
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(state));
    }

    private static List<PartitionSplit> splits() {
        return Arrays.asList(
                new PartitionSplit(
                        "0",
                        TestPartitions.batchTransactionId(),
                        TestPartitions.queryPartition("token-0", "SELECT 1")),
                new PartitionSplit(
                        "1",
                        TestPartitions.batchTransactionId(),
                        TestPartitions.readPartition("token-1", "singers", "id")));
    }
}
