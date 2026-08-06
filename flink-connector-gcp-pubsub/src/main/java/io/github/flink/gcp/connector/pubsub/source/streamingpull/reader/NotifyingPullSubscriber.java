/*
 * Copyright 2023 Google LLC
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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.List;

/**
 * Bridges Pub/Sub's push-style streaming pull to the pull-style {@link
 * org.apache.flink.connector.base.source.reader.splitreader.SplitReader} contract: the client
 * library delivers messages on its own threads into an in-memory buffer, and the fetcher thread
 * drains that buffer.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which exposes a single-message pull and its own notification future per subscriber.
 */
@Internal
public interface NotifyingPullSubscriber extends AutoCloseable {

    /**
     * Removes and returns up to {@code maxMessages} buffered messages, in the order the client
     * library delivered them, or an empty list when nothing is buffered.
     *
     * @param maxMessages the maximum number of messages to return
     * @return the drained messages
     * @throws IOException if the subscriber has failed permanently
     */
    List<PubsubMessage> pullMessages(int maxMessages) throws IOException;

    /**
     * Nacks every message this subscriber's split still holds and asks the client to shut down,
     * returning without waiting for it. Buffered messages are discarded — they were never emitted,
     * so Pub/Sub must redeliver them. Idempotent.
     *
     * <p>Named for {@link java.util.concurrent.ExecutorService#shutdown()}, which is the same
     * shape: it starts the shutdown and returns, and a separate call waits for it to finish.
     * Separate from {@link #close()} because the nack is what must not be skipped while the wait is
     * what costs time — a reader owning several splits shuts them all down before waiting on any,
     * so the waits overlap instead of accumulating.
     */
    void shutdown();

    /**
     * Shuts the subscriber down if it is not already shutting down, then waits for the client to
     * finish, up to the configured shutdown timeout.
     *
     * <p>An implementation must not report a failure it has already delivered through {@link
     * #pullMessages}. The reader consumes that one and fails the job on it, so a second report here
     * only adds a competing exception to a teardown the first one is already causing — which is why
     * the default implementation absorbs the one its client raises (#325). The repository-wide rule
     * this is an instance of, and what was measured about the other connectors' clients, are in the
     * root {@code CLAUDE.md}.
     *
     * @throws Exception if the shutdown itself goes wrong, for some reason other than that failure
     */
    @Override
    void close() throws Exception;
}
