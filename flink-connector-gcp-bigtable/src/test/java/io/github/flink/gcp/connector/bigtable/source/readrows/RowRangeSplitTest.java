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

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowRangeSplit}. */
@Timeout(30)
class RowRangeSplitTest {

    private static ByteStringRange range(String startClosed, String endOpen) {
        return ByteStringRange.unbounded().startClosed(startClosed).endOpen(endOpen);
    }

    @Test
    void isIdentifiedByItsIdAndItsRange() {
        RowRangeSplit split = new RowRangeSplit("0", range("a", "z"));

        assertThat(split.splitId()).isEqualTo("0");
        assertThat(split)
                .isEqualTo(new RowRangeSplit("0", range("a", "z")))
                .hasSameHashCodeAs(new RowRangeSplit("0", range("a", "z")))
                .isNotEqualTo(new RowRangeSplit("1", range("a", "z")))
                // The range shrinks on every checkpoint, so two splits sharing an id are not the
                // same split.
                .isNotEqualTo(new RowRangeSplit("0", range("a", "m")));
    }

    @Test
    void doesNotShareItsRangeWithTheCallerThatSuppliedIt() {
        ByteStringRange supplied = range("a", "z");
        RowRangeSplit split = new RowRangeSplit("0", supplied);

        supplied.startClosed("m");

        assertThat(split.getRange()).isEqualTo(range("a", "z"));
    }

    @Test
    void doesNotShareItsRangeWithTheCallerThatReadsIt() {
        RowRangeSplit split = new RowRangeSplit("0", range("a", "z"));

        split.getRange().startClosed("m");

        assertThat(split.getRange()).isEqualTo(range("a", "z"));
    }

    @Test
    void acceptsAnEmptyRangeBecauseTruncationProducesOne() {
        // The reader finishes such a split without opening a stream; rejecting it here would turn
        // the normal end of a closed-ended split into a crash.
        RowRangeSplit split =
                new RowRangeSplit("0", ByteStringRange.unbounded().startOpen("z").endClosed("z"));

        assertThat(RowRanges.isEmpty(split.getRange())).isTrue();
    }

    @Test
    void rendersItsRangeRatherThanAnIdentityHash() {
        assertThat(new RowRangeSplit("7", range("row-1", "row-9")))
                .hasToString("RowRangeSplit{splitId='7', range=[row-1, row-9)}");
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> new RowRangeSplit(null, range("a", "z")))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RowRangeSplit("0", null))
                .isInstanceOf(NullPointerException.class);
    }
}
