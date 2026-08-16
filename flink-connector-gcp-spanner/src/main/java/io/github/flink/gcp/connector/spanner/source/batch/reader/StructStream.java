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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.spanner.Struct;

import javax.annotation.Nullable;

/**
 * One open read of a partition, consumed a row at a time.
 *
 * <p>The connector's own type rather than the client's {@code ResultSet}, which cannot be produced
 * outside a real read — so a test that wanted to script one would otherwise have to run it.
 *
 * <p>Row order is Spanner's, and this connector promises nothing about it. A partitioned query's
 * result order is not contractual, which is why a split resumes at its start rather than at a
 * count.
 *
 * <p>Not thread-safe, and the one exception is deliberate: a stream belongs to the fetcher thread
 * that opened it, while {@link #close()} is also called from the task thread by a wake-up. Nothing
 * serialises the two — interrupting a blocked read is exactly what a wake-up is for — so an
 * implementation's close has to tolerate running beside a read.
 */
@Internal
public interface StructStream extends AutoCloseable {

    /**
     * Returns the next row, blocking until one arrives.
     *
     * @return the next row, or {@code null} once the read has ended
     */
    @Nullable
    Struct next();

    /**
     * Ends the read, cancelling it if it has not been consumed to the end.
     *
     * <p>Declared without a checked exception, unlike {@link AutoCloseable#close()}: a close on
     * this path runs from a fetch loop and from a wake-up, and neither has anywhere to route a
     * checked failure to.
     */
    @Override
    void close();
}
