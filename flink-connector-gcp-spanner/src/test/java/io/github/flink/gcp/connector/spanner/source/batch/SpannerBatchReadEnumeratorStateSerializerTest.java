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

package io.github.flink.gcp.connector.spanner.source.batch;

import com.google.cloud.spanner.TestPartitions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerBatchReadEnumeratorState} and its serializer. */
class SpannerBatchReadEnumeratorStateSerializerTest {

    private final SpannerBatchReadEnumeratorStateSerializer serializer =
            new SpannerBatchReadEnumeratorStateSerializer();

    @Test
    void aPlanSurvivesTheRoundTrip() throws Exception {
        SpannerBatchReadEnumeratorState state = new SpannerBatchReadEnumeratorState(true, splits());

        SpannerBatchReadEnumeratorState restored = roundTrip(state);

        assertThat(restored).isEqualTo(state);
        assertThat(restored.isPlanned()).isTrue();
        assertThat(restored.getPendingSplits()).hasSize(2);
        assertThat(restored.getPendingSplits().get(1).splitId()).isEqualTo("1");
    }

    @Test
    void aPlanFullyHandedOutIsStillPlanned() throws Exception {
        // The state that would be indistinguishable from an unplanned one if `planned` were read
        // as "the queue is non-empty" — and the one whose restore must not plan again.
        SpannerBatchReadEnumeratorState restored =
                roundTrip(new SpannerBatchReadEnumeratorState(true, Collections.emptyList()));

        assertThat(restored.isPlanned()).isTrue();
        assertThat(restored.getPendingSplits()).isEmpty();
    }

    @Test
    void anUnplannedStateSurvivesTheRoundTrip() throws Exception {
        SpannerBatchReadEnumeratorState restored =
                roundTrip(new SpannerBatchReadEnumeratorState(false, Collections.emptyList()));

        assertThat(restored.isPlanned()).isFalse();
    }

    @Test
    void anUnplannedStateCannotHoldSplits() {
        assertThatThrownBy(() -> new SpannerBatchReadEnumeratorState(false, splits()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("an unplanned enumerator cannot hold pending splits");
    }

    @Test
    void thePendingSplitsAreCopiedAndUnmodifiable() {
        List<BatchReadSplit> splits = new java.util.ArrayList<>(splits());
        SpannerBatchReadEnumeratorState state = new SpannerBatchReadEnumeratorState(true, splits);

        splits.clear();

        assertThat(state.getPendingSplits()).hasSize(2);
        assertThatThrownBy(() -> state.getPendingSplits().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void anUnknownVersionIsRefusedByName() throws Exception {
        byte[] bytes = serializer.serialize(new SpannerBatchReadEnumeratorState(true, splits()));

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
        assertThat(new BatchReadSplitSerializer().getVersion()).isEqualTo(1);
    }

    private SpannerBatchReadEnumeratorState roundTrip(SpannerBatchReadEnumeratorState state)
            throws IOException {
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(state));
    }

    private static List<BatchReadSplit> splits() {
        return Arrays.asList(
                new BatchReadSplit(
                        "0",
                        TestPartitions.batchTransactionId(),
                        TestPartitions.queryPartition("token-0", "SELECT 1")),
                new BatchReadSplit(
                        "1",
                        TestPartitions.batchTransactionId(),
                        TestPartitions.readPartition("token-1", "singers", "id")));
    }
}
