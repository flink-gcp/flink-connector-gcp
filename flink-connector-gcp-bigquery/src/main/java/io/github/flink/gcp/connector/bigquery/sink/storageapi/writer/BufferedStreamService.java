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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import org.apache.flink.annotation.Internal;

import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;

/**
 * The Storage Write API operations of the buffered-stream write path, abstracted so writer and
 * committer logic can be unit-tested against fakes: creating buffered streams, opening offset-aware
 * appenders on them, finalizing them, and flushing rows.
 *
 * <p>Implementations own the underlying client; {@link #close()} releases it. Appenders returned by
 * {@link #openAppender} have their own lifecycle and must be closed by the caller.
 */
@Internal
public interface BufferedStreamService extends AutoCloseable {

    /**
     * Creates a buffered write stream on the destination table.
     *
     * @param destination the destination table
     * @return the created stream's name (full resource path)
     * @throws IOException if creation fails
     */
    String createBufferedStream(TableDestination destination) throws IOException;

    /**
     * Opens an offset-aware appender on the given stream.
     *
     * @param streamName the buffered write stream name
     * @param rowDescriptor the protobuf descriptor of the serialized rows
     * @return the appender
     * @throws IOException if the appender cannot be opened
     */
    OffsetRowAppender openAppender(String streamName, Descriptors.Descriptor rowDescriptor)
            throws IOException;

    /**
     * Makes every row up to the given offset (inclusive) visible in the destination table.
     *
     * @param streamName the buffered write stream name
     * @param offset the highest row offset to make visible, inclusive
     * @return the offset acknowledged by the server
     * @throws IOException if the flush fails
     */
    long flushRows(String streamName, long offset) throws IOException;

    @Override
    void close();
}
