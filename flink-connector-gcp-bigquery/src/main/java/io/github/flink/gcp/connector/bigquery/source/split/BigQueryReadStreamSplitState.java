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

package io.github.flink.gcp.connector.bigquery.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * The mutable reading progress of one {@link BigQueryReadStreamSplit}.
 *
 * <p>Separate from the split because the two are touched by different threads: the split reader
 * runs on the fetcher thread and holds the split it was handed, while the record emitter runs on
 * the task thread and advances this state per consumed row. A single mutable type would put a field
 * the task thread writes inside an object the fetcher thread reads.
 *
 * <p>Not thread-safe, and does not need to be: every method here is called from the task thread.
 */
@Internal
public final class BigQueryReadStreamSplitState {

    private final BigQueryReadStreamSplit split;

    private long offset;

    /**
     * Creates the state of a split, seeded with the progress the split carries.
     *
     * @param split the split being read
     */
    public BigQueryReadStreamSplitState(BigQueryReadStreamSplit split) {
        this.split = Preconditions.checkNotNull(split, "split must not be null");
        this.offset = split.getOffset();
    }

    /** Returns how many input rows of this stream have been consumed successfully. */
    public long getOffset() {
        return offset;
    }

    /**
     * Records that one input row has been consumed successfully.
     *
     * <p>Called once after the deserializer returns and all of its synchronous output calls
     * succeed, including when it emitted nothing. The offset counts input rows <em>consumed from
     * the stream</em>, not output records.
     */
    public void recordConsumed() {
        offset++;
    }

    /** Returns the immutable split at the progress reached so far. */
    public BigQueryReadStreamSplit toSplit() {
        return new BigQueryReadStreamSplit(
                split.getStreamName(),
                offset,
                split.getAvroSchemaJson(),
                split.getSessionExpireTime());
    }
}
