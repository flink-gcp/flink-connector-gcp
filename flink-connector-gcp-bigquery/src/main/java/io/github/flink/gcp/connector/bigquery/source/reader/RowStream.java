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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;

import javax.annotation.Nullable;

/** One open {@code ReadRows} call: the response blocks of one stream, from one offset onwards. */
@Internal
public interface RowStream extends AutoCloseable {

    /**
     * Returns the next block of rows, blocking until one arrives.
     *
     * <p>Declares no checked exception on purpose: the split reader classifies an unchecked failure
     * here as either its own cancellation or a read failure, and a checked one would bypass that
     * and fail the job where a wake-up should have yielded a partial batch.
     *
     * @return the next response, or {@code null} when the stream has ended
     */
    @Nullable
    ReadRowsResponse next();

    /**
     * Unblocks a thread waiting in {@link #next()}, which then returns {@code null} or throws.
     *
     * <p>Called from another thread than the one reading. The stream is dead afterwards; a caller
     * that wants to keep reading opens a new one at the offset it had reached.
     */
    void cancel();

    @Override
    void close();
}
