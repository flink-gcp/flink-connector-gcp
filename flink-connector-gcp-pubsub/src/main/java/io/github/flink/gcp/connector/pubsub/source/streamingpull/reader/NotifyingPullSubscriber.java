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
     * Nacks every message this subscriber's split still holds and shuts the client down. Buffered
     * messages are discarded — they were never emitted, so Pub/Sub must redeliver them.
     *
     * @throws Exception if the client does not shut down cleanly
     */
    @Override
    void close() throws Exception;
}
