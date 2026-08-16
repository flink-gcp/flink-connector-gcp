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

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Opens a {@code ReadRows} call over one row-key range.
 *
 * <p>The seam the split reader reads through, so that resuming, cancelling and reopening can be
 * tested against a stream that honours an exclusive range start — which the emulator does but a
 * unit test should not need a container to find out.
 *
 * <p>{@link Serializable} because the source configuration this travels in goes into the job graph.
 * An implementation creates its client on first use rather than in its constructor, so that
 * building a job needs no credentials.
 *
 * <p>The range and the filter are arguments rather than fields because a reader opens a different
 * range for every split it is handed and for every reopen after a wake-up, while the table and the
 * filter stay the same for the job's life. Passing both keeps the query the connector asks for
 * visible at the call site.
 */
@Internal
public interface RowStreamOpener extends Serializable, AutoCloseable {

    /**
     * Opens a read over one range.
     *
     * @param table the table to read
     * @param range the row-key range to read, which the caller guarantees is not empty
     * @param filter the server-side filter to apply, or {@code null} for none
     * @return the open stream
     * @throws IOException if the call cannot be started
     */
    RowStream open(TableDestination table, ByteStringRange range, @Nullable Filters.Filter filter)
            throws IOException;

    @Override
    void close() throws IOException;
}
