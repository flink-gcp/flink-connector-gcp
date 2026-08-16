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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowRangeSplitState}. */
@Timeout(30)
class RowRangeSplitStateTest {

    private static ByteString key(String text) {
        return ByteString.copyFromUtf8(text);
    }

    private static ByteStringRange range(String startClosed, String endOpen) {
        return ByteStringRange.unbounded().startClosed(startClosed).endOpen(endOpen);
    }

    @Test
    void checkpointsTheAssignedSplitUnchangedWhenNothingHasBeenEmitted() {
        RowRangeSplit split = new RowRangeSplit("0", range("a", "z"));

        RowRangeSplitState state = new RowRangeSplitState(split);

        assertThat(state.getLastEmittedKey()).isNull();
        assertThat(state.toSplit()).isEqualTo(split);
    }

    @Test
    void checkpointsTheRangeStartingJustPastTheLastEmittedRow() {
        RowRangeSplitState state = new RowRangeSplitState(new RowRangeSplit("0", range("a", "z")));

        state.recordEmitted(key("m"));

        ByteStringRange remaining = state.toSplit().getRange();
        assertThat(remaining.getStartBound()).isEqualTo(BoundType.OPEN);
        assertThat(remaining.getStart()).isEqualTo(key("m"));
        assertThat(remaining.getEnd()).isEqualTo(key("z"));
    }

    @Test
    void keepsTheSplitIdWhileTheRangeShrinks() {
        // SourceReaderBase keys split state by splitId(), so an id derived from the range would be
        // lost the first time a checkpoint truncated it.
        RowRangeSplitState state = new RowRangeSplitState(new RowRangeSplit("7", range("a", "z")));

        state.recordEmitted(key("m"));

        assertThat(state.toSplit().splitId()).isEqualTo("7");
    }

    @Test
    void advancesToTheLatestEmittedRowOnly() {
        RowRangeSplitState state = new RowRangeSplitState(new RowRangeSplit("0", range("a", "z")));

        state.recordEmitted(key("b"));
        state.recordEmitted(key("c"));
        state.recordEmitted(key("d"));

        assertThat(state.getLastEmittedKey()).isEqualTo(key("d"));
        assertThat(state.toSplit().getRange().getStart()).isEqualTo(key("d"));
    }

    @Test
    void restoringAndCheckpointingWithoutReadingIsAFixedPoint() {
        RowRangeSplitState state = new RowRangeSplitState(new RowRangeSplit("0", range("a", "z")));
        state.recordEmitted(key("m"));
        RowRangeSplit checkpointed = state.toSplit();

        RowRangeSplit again = new RowRangeSplitState(checkpointed).toSplit();

        assertThat(again).isEqualTo(checkpointed);
    }

    @Test
    void emittingTheLastRowOfAnInclusivelyEndedRangeLeavesNothingToRead() {
        RowRangeSplitState state =
                new RowRangeSplitState(
                        new RowRangeSplit(
                                "0", ByteStringRange.unbounded().startClosed("a").endClosed("z")));

        state.recordEmitted(key("z"));

        assertThat(RowRanges.isEmpty(state.toSplit().getRange())).isTrue();
    }

    @Test
    void resumesAtKeysThatAreNotText() {
        ByteString binary = ByteString.copyFrom(new byte[] {0x00, (byte) 0xFF, 0x10});
        RowRangeSplitState state =
                new RowRangeSplitState(new RowRangeSplit("0", ByteStringRange.unbounded()));

        state.recordEmitted(binary);

        assertThat(state.toSplit().getRange().getStart()).isEqualTo(binary);
    }
}
