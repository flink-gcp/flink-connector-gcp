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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriberBufferLimits;

/**
 * How much a paused split may buffer before the reader stops its subscriber.
 *
 * <p>Two dimensions, and either one crossing is enough, because which of them binds depends on
 * message size: flow control admits {@code min(count limit, byte limit / message size)}, so with
 * small messages the count limit is what a buffer reaches first and with large ones the byte limit
 * is, and a bound expressed in only one of them is unreachable in the other's case.
 *
 * <p><b>Each dimension defaults to twice the flow-control limit it shadows</b>, down to the SDK's
 * own default when that is unset too — the lapse bound resolved by {@link SubscriberBufferLimits},
 * which also records why the factor is two and why the limit itself would park healthy splits. An
 * unset reader-wide budget gives each subscriber at least this bound, so the per-split park stays
 * reachable before the reader-wide response.
 */
@Internal
public final class PausedSplitBufferLimits {

    private final long maxMessages;
    private final long maxBytes;

    private PausedSplitBufferLimits(long maxMessages, long maxBytes) {
        this.maxMessages = maxMessages;
        this.maxBytes = maxBytes;
    }

    /** Resolves the limits the given options ask for. */
    public static PausedSplitBufferLimits of(PubSubSubscriberOptions options) {
        SubscriberBufferLimits shares = SubscriberBufferLimits.of(options);
        // An explicit bound is taken as given: it is the operator's number, not a base to scale.
        Long maxMessages = options.getPausedSplitBufferMaxMessages();
        Long maxBytes = options.getPausedSplitBufferMaxBytes();
        return new PausedSplitBufferLimits(
                maxMessages != null ? maxMessages : shares.lapseBoundMessages(),
                maxBytes != null ? maxBytes : shares.lapseBoundBytes());
    }

    /** Returns whether the given buffer has outgrown either limit. */
    public boolean exceededBy(BufferUsage usage) {
        return usage.messages() > maxMessages || usage.bytes() > maxBytes;
    }

    /**
     * Returns the message cap. Read by the resolution tests; the reader compares through {@link
     * #exceededBy} and reports through {@link #toString()}.
     */
    @VisibleForTesting
    long maxMessages() {
        return maxMessages;
    }

    /** Returns the byte cap, as {@link #maxMessages()} returns the message cap. */
    @VisibleForTesting
    long maxBytes() {
        return maxBytes;
    }

    @Override
    public String toString() {
        return maxMessages + " messages, " + maxBytes + " bytes";
    }
}
