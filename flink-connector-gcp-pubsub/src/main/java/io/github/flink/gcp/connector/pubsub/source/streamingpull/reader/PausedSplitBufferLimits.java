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
import org.apache.flink.util.Preconditions;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.Subscriber;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;

import javax.annotation.Nullable;

/**
 * How much a paused split may buffer before the reader stops its subscriber.
 *
 * <p>Two dimensions, and either one crossing is enough, because which of them binds depends on
 * message size: flow control admits {@code min(count limit, byte limit / message size)}, so with
 * small messages the count limit is what a buffer reaches first and with large ones the byte limit
 * is, and a bound expressed in only one of them is unreachable in the other's case.
 *
 * <p><b>Each dimension defaults to twice the flow-control limit it shadows</b>, down to the SDK's
 * own default when that is unset too. The factor is what the lapse itself is worth: the client
 * releases a whole flow-control window of permits per expiry wave, so the first wave carries a
 * paused split's buffer to {@code 2 × limit + 1} (measured: a 50-message window stepped to 101).
 * The bound is therefore still crossed by the lapse and by nothing smaller, without anybody
 * choosing a number.
 *
 * <p><b>Why not the limit itself.</b> The buffer is <em>nearly</em> a subset of what the client
 * holds outstanding, and it was tempting to call the limit unreachable and use it directly. It is
 * not, in at least three ways measured against {@code google-cloud-pubsub} 1.152.0 and gax 2.x —
 * each of which would park a healthy split, since a bound at the limit has one message of headroom:
 *
 * <ul>
 *   <li>a message larger than the byte limit is admitted anyway — gax's {@code
 *       BlockingSemaphore.acquirePartial} clamps the request to the limit and lets its permits go
 *       negative, so <em>one</em> oversized message already exceeds a bound set at that limit;
 *   <li>on a subscription with a dead-letter policy the client adds a {@code
 *       googclient_deliveryattempt} attribute <em>after</em> reserving ({@code
 *       MessageDispatcher.processBatch} reserves, then calls {@code addDeliveryInfoCount}), so
 *       every buffered message is a few dozen bytes larger than what was reserved for it;
 *   <li>a redelivery is buffered beside the copy it supersedes while {@code
 *       AckTracker.addPendingAck} nacks the superseded handle, which releases that delivery's
 *       permit — two messages held against one permit until both are drained.
 * </ul>
 *
 * <p>The SDK's defaults are read from {@link Subscriber.Builder#getDefaultFlowControlSettings()}
 * rather than mirrored as constants here. {@code DefaultSubscriberFactory} does mirror {@code
 * maxAckExtensionPeriod}'s default, and the reason it has to is that <em>that</em> SDK constant is
 * package-private; these are public, and production code already reads them.
 */
@Internal
public final class PausedSplitBufferLimits {

    private final long maxMessages;
    private final long maxBytes;

    private PausedSplitBufferLimits(long maxMessages, long maxBytes) {
        this.maxMessages = maxMessages;
        this.maxBytes = maxBytes;
    }

    /**
     * How much of the shadowed flow-control limit an unset bound allows, and the size of one
     * lease-expiry wave.
     */
    private static final long DEFAULT_FLOW_CONTROL_MULTIPLE = 2;

    /** Resolves the limits the given options ask for. */
    public static PausedSplitBufferLimits of(PubSubSubscriberOptions options) {
        FlowControlSettings sdkDefaults = Subscriber.Builder.getDefaultFlowControlSettings();
        return new PausedSplitBufferLimits(
                resolve(
                        options.getPausedSplitBufferMaxMessages(),
                        options.getFlowControlMaxOutstandingElementCount(),
                        sdkDefaults.getMaxOutstandingElementCount(),
                        "maxOutstandingElementCount"),
                resolve(
                        options.getPausedSplitBufferMaxBytes(),
                        options.getFlowControlMaxOutstandingRequestBytes(),
                        sdkDefaults.getMaxOutstandingRequestBytes(),
                        "maxOutstandingRequestBytes"));
    }

    /**
     * Returns the first limit that is set, of the paused-split cap, the flow-control limit it
     * shadows, and the SDK's default for that limit.
     *
     * <p>The SDK's own getters are nullable — {@code FlowControlSettings} allows an unset dimension
     * — so the last rung is checked rather than assumed, and an SDK that stopped setting one fails
     * at reader construction rather than resolving to nothing. A non-positive value needs no check
     * here: gax's own {@code FlowControlSettings.Builder.build()} rejects one, and this class's two
     * other rungs are validated by the options builder.
     */
    private static long resolve(
            @Nullable Long pausedSplitLimit,
            @Nullable Long flowControlLimit,
            @Nullable Long sdkDefault,
            String sdkSettingName) {
        if (pausedSplitLimit != null) {
            // Taken as given: an explicit bound is the operator's number, not a base to scale.
            return pausedSplitLimit;
        }
        if (flowControlLimit != null) {
            return withHeadroom(flowControlLimit);
        }
        Preconditions.checkState(
                sdkDefault != null,
                "The Pub/Sub client library's default flow-control settings leave %s unset, so a"
                        + " paused split's buffer has no bound to default to. Set"
                        + " pausedSplitBufferMaxMessages and pausedSplitBufferMaxBytes explicitly.",
                sdkSettingName);
        return withHeadroom(sdkDefault);
    }

    /** Saturating, because a flow-control limit near {@code Long.MAX_VALUE} must not wrap. */
    private static long withHeadroom(long flowControlLimit) {
        return flowControlLimit > Long.MAX_VALUE / DEFAULT_FLOW_CONTROL_MULTIPLE
                ? Long.MAX_VALUE
                : flowControlLimit * DEFAULT_FLOW_CONTROL_MULTIPLE;
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
