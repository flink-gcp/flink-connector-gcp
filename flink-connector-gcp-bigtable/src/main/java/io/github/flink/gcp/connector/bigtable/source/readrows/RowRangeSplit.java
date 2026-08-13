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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.util.Objects;

/**
 * One contiguous row-key range still to be read.
 *
 * <p>The range <em>is</em> the remaining work: a checkpoint truncates it to start just past the
 * last row successfully deserialized, including one that produced no output, so a restored split
 * resumes without re-reading and without skipping. That is why no offset appears here — {@code
 * ReadRows} has no row offset to resume at, only a range to ask for — and it is what {@link
 * RowRangeSplitState} maintains while a reader works.
 *
 * <p>Three things are deliberately absent. The table and the filter, because a reader has both from
 * the source's configuration and putting them here would write the same bytes into every checkpoint
 * once per split. The planner's size estimate, because it describes the range as it was planned and
 * says nothing about the range as it stands after a checkpoint. And a {@code finished} flag,
 * because nothing could set one: {@code SourceReaderBase} removes a split's state before telling
 * the reader that the split finished.
 *
 * <p>A truncated range can be empty — a range ending inclusively at a key whose row has been
 * emitted has nothing left in it — and that is a normal state rather than a corrupt one. The split
 * reader finishes such a split without opening a stream, which is also why an empty range never
 * reaches the service.
 */
@Internal
public final class RowRangeSplit implements SourceSplit {

    private final String splitId;
    private final ByteStringRange range;

    /**
     * Creates a split.
     *
     * @param splitId the split's identity, stable for as long as the split exists
     * @param range the row-key range still to be read
     */
    public RowRangeSplit(String splitId, ByteStringRange range) {
        this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
        this.range = RowRanges.copyOf(Preconditions.checkNotNull(range, "range must not be null"));
    }

    /**
     * Returns the row-key range still to be read.
     *
     * <p>A fresh copy each time, because a {@link ByteStringRange} is mutable and this split is
     * checkpointed state: handing out the field itself would let a caller edit a split that has
     * already been assigned.
     */
    public ByteStringRange getRange() {
        return RowRanges.copyOf(range);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowRangeSplit)) {
            return false;
        }
        RowRangeSplit other = (RowRangeSplit) o;
        return splitId.equals(other.splitId) && range.equals(other.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, range);
    }

    @Override
    public String toString() {
        return "RowRangeSplit{splitId='" + splitId + "', range=" + RowRanges.format(range) + '}';
    }
}
