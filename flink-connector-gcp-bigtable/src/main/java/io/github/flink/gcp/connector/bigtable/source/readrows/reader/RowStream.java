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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.Row;

import javax.annotation.Nullable;

/**
 * One open {@code ReadRows} call, consumed a row at a time.
 *
 * <p>The connector's own type rather than the client's {@code ServerStream}, which cannot be
 * constructed outside the library — so a test that wanted to script a read would otherwise have to
 * run one. Row order is the service's: ascending by row key, which is what the whole resume design
 * rests on.
 *
 * <p>Consumed once and then closed, in that order or in the other one: the client's contract is
 * that a stream is either read to its end or cancelled, and {@link #close()} is where a
 * half-consumed stream is cancelled.
 *
 * <p>Not thread-safe, and the one exception is deliberate: a stream belongs to the fetcher thread
 * that opened it, while {@link #close()} is also called from the task thread by a wake-up. Nothing
 * serialises the two — interrupting a blocked read is exactly what a wake-up is for — so an
 * implementation's close has to tolerate running beside a read.
 */
@Internal
public interface RowStream extends AutoCloseable {

    /**
     * Returns the next row, blocking until one arrives.
     *
     * @return the next row, or {@code null} once the stream has ended
     */
    @Nullable
    Row next();

    /**
     * Ends the call, cancelling it if it has not been read to the end.
     *
     * <p>Declared without a checked exception, unlike {@link AutoCloseable#close()}: a close on
     * this path runs from a fetch loop and from a wake-up, and neither has anywhere to route a
     * checked failure to.
     */
    @Override
    void close();
}
