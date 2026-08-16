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

import java.io.IOException;
import java.io.Serializable;

/**
 * Opens {@code ReadRows} calls on a read session's streams.
 *
 * <p>Abstracts the Storage Read API client so the reader's resume logic is unit-testable against a
 * server that honours offsets — which the BigQuery emulator does not, so no emulator test can stand
 * in for it. {@code BigQueryReadClient} cannot be subclassed usefully and this repository writes no
 * mocks, which leaves a seam as the only way.
 *
 * <p>Serializable because it is held by the source's configuration and therefore travels in the job
 * graph; implementations create their client state on first use, not in their constructor.
 */
@Internal
public interface RowStreamOpener extends Serializable, AutoCloseable {

    /**
     * Opens a stream, starting after the given number of rows.
     *
     * @param streamName the stream's resource name
     * @param offset how many rows of the stream to skip; {@code 0} reads from the beginning
     * @return the open stream; the caller owns it and must close it
     * @throws IOException if the stream cannot be opened
     */
    RowStream open(String streamName, long offset) throws IOException;

    /**
     * Registers what to run each time the underlying client retries an attempt at a stream.
     *
     * <p>Called once per subtask, on the task thread, before any stream is opened — an
     * implementation whose client captures the listener at creation may therefore ignore one
     * registered later. Default no-op, because an implementation that does not retry has nothing to
     * report and should not be made to say so.
     *
     * @param onRetry run once per retried attempt, on whichever thread the client retries from
     */
    default void setRetryListener(Runnable onRetry) {}

    /**
     * Releases whatever client state {@link #open} opened.
     *
     * @throws IOException if the release fails
     */
    @Override
    void close() throws IOException;
}
