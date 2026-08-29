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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Row;

/** A materialised Bigtable row and the stable byte estimate measured while it was decoded. */
@Internal
public final class MeasuredRow {

    private final Row row;
    private final long estimatedBytes;

    MeasuredRow(Row row, long estimatedBytes) {
        this.row = Preconditions.checkNotNull(row, "row must not be null");
        Preconditions.checkArgument(
                estimatedBytes > 0, "estimatedBytes must be positive: %s", estimatedBytes);
        this.estimatedBytes = estimatedBytes;
    }

    /** Returns the SDK row handed to the connector's deserializer. */
    public Row row() {
        return row;
    }

    /** Returns the row's saturated, positive byte estimate. */
    public long estimatedBytes() {
        return estimatedBytes;
    }
}
