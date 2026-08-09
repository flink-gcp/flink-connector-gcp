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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableScanEnumeratorStateSerializer}. */
@Timeout(30)
class BigtableScanEnumeratorStateSerializerTest {

    private final BigtableScanEnumeratorStateSerializer serializer =
            new BigtableScanEnumeratorStateSerializer();

    @Test
    void roundTripsAPlanThatHasNotBeenHandedOut() throws IOException {
        BigtableScanEnumeratorState state =
                new BigtableScanEnumeratorState(
                        true,
                        Arrays.asList(
                                new RowRangeSplit("0", ByteStringRange.unbounded().endOpen("m")),
                                new RowRangeSplit(
                                        "1", ByteStringRange.unbounded().startClosed("m"))));

        assertThat(roundTrip(state)).isEqualTo(state);
    }

    @Test
    void roundTripsAPlanThatHasBeenFullyHandedOut() throws IOException {
        // The state that distinguishes "planned, nothing left to give" from "not planned yet": a
        // restore that confused the two would sample the table again and renumber every split.
        BigtableScanEnumeratorState state =
                new BigtableScanEnumeratorState(true, Collections.emptyList());

        BigtableScanEnumeratorState back = roundTrip(state);

        assertThat(back.isPlanned()).isTrue();
        assertThat(back.getPendingSplits()).isEmpty();
    }

    @Test
    void roundTripsAnEnumeratorThatHasNotPlannedYet() throws IOException {
        BigtableScanEnumeratorState back =
                roundTrip(new BigtableScanEnumeratorState(false, Collections.emptyList()));

        assertThat(back.isPlanned()).isFalse();
        assertThat(back.getPendingSplits()).isEmpty();
    }

    @Test
    void keepsThePendingSplitsInTheOrderTheyWillBeHandedOut() throws IOException {
        BigtableScanEnumeratorState state =
                new BigtableScanEnumeratorState(
                        true,
                        Arrays.asList(
                                new RowRangeSplit(
                                        "2", ByteStringRange.unbounded().startClosed("c")),
                                new RowRangeSplit("0", ByteStringRange.unbounded().endOpen("a")),
                                new RowRangeSplit(
                                        "1",
                                        ByteStringRange.unbounded()
                                                .startClosed("a")
                                                .endOpen("c"))));

        assertThat(roundTrip(state).getPendingSplits())
                .extracting(RowRangeSplit::splitId)
                .containsExactly("2", "0", "1");
    }

    @Test
    void rejectsAVersionItDidNotWrite() throws IOException {
        byte[] serialized =
                serializer.serialize(
                        new BigtableScanEnumeratorState(false, Collections.emptyList()));

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Unsupported Bigtable scan enumerator state serialization version");
    }

    @Test
    void rejectsANegativeSplitCount() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeBoolean(true);
        out.writeInt(-1);

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), out.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative split count");
    }

    @Test
    void refusesToHoldPendingSplitsWithoutHavingPlanned() {
        assertThatThrownBy(
                        () ->
                                new BigtableScanEnumeratorState(
                                        false,
                                        Collections.singletonList(
                                                new RowRangeSplit(
                                                        "0", ByteStringRange.unbounded()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private BigtableScanEnumeratorState roundTrip(BigtableScanEnumeratorState state)
            throws IOException {
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(state));
    }
}
