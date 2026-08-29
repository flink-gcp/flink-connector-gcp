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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;

/** Reports that one source reader could not admit a delivery within its subscriber-buffer bound. */
@Internal
public final class SubscriberBufferLimitExceededEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final long attemptedMessages;
    private final long attemptedBytes;
    private final long maxMessages;
    private final long maxBytes;

    public SubscriberBufferLimitExceededEvent(
            String splitId,
            long attemptedMessages,
            long attemptedBytes,
            long maxMessages,
            long maxBytes) {
        this.splitId = splitId;
        this.attemptedMessages = attemptedMessages;
        this.attemptedBytes = attemptedBytes;
        this.maxMessages = maxMessages;
        this.maxBytes = maxBytes;
    }

    public String splitId() {
        return splitId;
    }

    public long attemptedMessages() {
        return attemptedMessages;
    }

    public long attemptedBytes() {
        return attemptedBytes;
    }

    public long maxMessages() {
        return maxMessages;
    }

    public long maxBytes() {
        return maxBytes;
    }
}
