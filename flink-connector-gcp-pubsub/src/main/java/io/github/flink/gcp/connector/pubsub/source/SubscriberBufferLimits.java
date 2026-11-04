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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.Subscriber;

import javax.annotation.Nullable;

import java.util.function.LongSupplier;

/**
 * The subscriber-buffer bounds the given options resolve to, and the one relation they must keep
 * with SDK flow control.
 *
 * <p><b>Flow control is what holds a backpressured subscriber back.</b> Because the source
 * acknowledges only at checkpoint completion, a subscriber whose downstream drains more slowly than
 * Pub/Sub delivers fills its buffer until the client's flow control stops pulling, which is at most
 * one flow-control window. A bound below that window is therefore crossed by ordinary backpressure,
 * and a job resumed over a backlog crosses it again within seconds of every restart (#2020). The
 * lapse the bounds exist for is larger: once {@code maxAckExtensionPeriod} passes for a message
 * nobody is draining, the client releases a whole window of permits per expiry wave, so the first
 * wave carries the buffer to {@code 2 × limit + 1} (measured: a 50-message window stepped to 101).
 * <b>Twice the limit, the {@linkplain #lapseBoundMessages() lapse bound}, is crossed by the lapse
 * and by nothing smaller</b>, without anybody choosing a number.
 *
 * <p><b>Why not the limit itself.</b> The buffer is <em>nearly</em> a subset of what the client
 * holds outstanding, but a bound at the limit has one message of headroom, and at least three
 * healthy paths pass it (measured against {@code google-cloud-pubsub} 1.152.0 and gax 2.x):
 *
 * <ul>
 *   <li>a message larger than the byte limit is admitted anyway — gax's {@code
 *       BlockingSemaphore.acquirePartial} clamps the request to the limit and lets its permits go
 *       negative;
 *   <li>on a subscription with a dead-letter policy the client adds a {@code
 *       googclient_deliveryattempt} attribute <em>after</em> reserving, so every buffered message
 *       is a few dozen bytes larger than what was reserved for it;
 *   <li>a redelivery is buffered beside the copy it supersedes while the acknowledgement tracker
 *       nacks the superseded handle, releasing that delivery's permit.
 * </ul>
 *
 * <p>The lapse bound is the paused-split bound's default. An unset reader-wide budget gives every
 * subscriber the reader holds a share of the lapse bound, or of an explicit paused-split bound when
 * that is larger, so that the per-split park the operator configured is reached before the
 * reader-wide response; the share is per subscriber because each has a flow controller of its own,
 * and {@code parallelPullCount} does not multiply it, because the client shares one flow controller
 * across a subscriber's streams. An explicit budget is taken as given — it is the operator's
 * number, not a base to scale — and {@link PubSubSubscriberOptions.Builder#build()} rejects one
 * below the lapse bound, which no reader can hold under backpressure. A reader that holds several
 * subscribers is only known at run time, so the reader warns there instead.
 *
 * <p>The SDK's defaults are read from {@link Subscriber.Builder#getDefaultFlowControlSettings()}
 * rather than mirrored as constants: they are public, and production code already reads them.
 */
@Internal
public final class SubscriberBufferLimits {

    /** How many flow-control windows the lapse bound is worth. */
    private static final int FLOW_CONTROL_MULTIPLE = 2;

    @Nullable private final Long configuredMaxMessages;
    @Nullable private final Long configuredMaxBytes;
    private final long lapseBoundMessages;
    private final long lapseBoundBytes;
    private final long derivedShareMessages;
    private final long derivedShareBytes;

    private SubscriberBufferLimits(
            @Nullable Long configuredMaxMessages,
            @Nullable Long configuredMaxBytes,
            long lapseBoundMessages,
            long lapseBoundBytes,
            long derivedShareMessages,
            long derivedShareBytes) {
        this.configuredMaxMessages = configuredMaxMessages;
        this.configuredMaxBytes = configuredMaxBytes;
        this.lapseBoundMessages = lapseBoundMessages;
        this.lapseBoundBytes = lapseBoundBytes;
        this.derivedShareMessages = derivedShareMessages;
        this.derivedShareBytes = derivedShareBytes;
    }

    /**
     * Resolves the limits the given options ask for.
     *
     * @param options the subscriber options
     * @return the resolved limits
     */
    public static SubscriberBufferLimits of(PubSubSubscriberOptions options) {
        long lapseMessages =
                twice(
                        effectiveFlowControlMessages(
                                options.getFlowControlMaxOutstandingElementCount()));
        long lapseBytes =
                twice(
                        effectiveFlowControlBytes(
                                options.getFlowControlMaxOutstandingRequestBytes()));
        return new SubscriberBufferLimits(
                options.configuredSubscriberBufferMaxMessages(),
                options.configuredSubscriberBufferMaxBytes(),
                lapseMessages,
                lapseBytes,
                atLeast(lapseMessages, options.getPausedSplitBufferMaxMessages()),
                atLeast(lapseBytes, options.getPausedSplitBufferMaxBytes()));
    }

    /**
     * Returns fixed reader-wide limits that no subscriber count scales and no derived budget is
     * compared against.
     *
     * @param maxMessages the reader-wide message cap
     * @param maxBytes the reader-wide byte cap
     * @return the fixed limits
     */
    public static SubscriberBufferLimits fixed(long maxMessages, long maxBytes) {
        return new SubscriberBufferLimits(maxMessages, maxBytes, 0, 0, 0, 0);
    }

    /**
     * Returns the message cap for a reader holding the given number of subscribers.
     *
     * @param subscribers the subscribers the reader holds; fewer than one counts as one
     * @return the configured cap, or the {@linkplain #derivedMaxMessages(int) derived} one
     */
    public long maxMessages(int subscribers) {
        return configuredMaxMessages != null
                ? configuredMaxMessages
                : derivedMaxMessages(subscribers);
    }

    /**
     * Returns the byte cap for a reader holding the given number of subscribers.
     *
     * @param subscribers the subscribers the reader holds; fewer than one counts as one
     * @return the configured cap, or the {@linkplain #derivedMaxBytes(int) derived} one
     */
    public long maxBytes(int subscribers) {
        return maxBytes(subscribers, 0);
    }

    /**
     * Returns the byte cap for a reader holding the given number of subscribers that has received a
     * message of the given size.
     *
     * <p>The client admits a message larger than its byte limit once its window is free, so a
     * subscriber's window is never smaller than the largest message it was handed; an unset cap
     * counts that message as the window, and an explicit cap is the operator's number either way.
     *
     * @param subscribers the subscribers the reader holds; fewer than one counts as one
     * @param largestMessageBytes the largest serialized message the reader has received
     * @return the configured cap, or the derived one widened to twice that message per subscriber
     */
    public long maxBytes(int subscribers, long largestMessageBytes) {
        if (configuredMaxBytes != null) {
            return configuredMaxBytes;
        }
        return times(Math.max(derivedShareBytes, twice(largestMessageBytes)), subscribers);
    }

    /**
     * Returns the message cap an unset budget derives for the given number of subscribers: each
     * subscriber's lapse bound, or its explicit paused-split bound when that is larger.
     *
     * @param subscribers the subscribers the reader holds; fewer than one counts as one
     * @return the derived message cap, saturating at {@code Long.MAX_VALUE}
     */
    public long derivedMaxMessages(int subscribers) {
        return times(derivedShareMessages, subscribers);
    }

    /**
     * Returns the byte cap an unset budget derives for the given number of subscribers, as {@link
     * #derivedMaxMessages(int)} returns the message cap.
     *
     * @param subscribers the subscribers the reader holds; fewer than one counts as one
     * @return the derived byte cap, saturating at {@code Long.MAX_VALUE}
     */
    public long derivedMaxBytes(int subscribers) {
        return times(derivedShareBytes, subscribers);
    }

    /**
     * Returns whether both caps for the given subscriber count are at least the ones an unset
     * budget would derive, so that ordinary backpressure cannot reach them.
     *
     * @param subscribers the subscribers the reader holds
     * @return {@code true} when neither cap is below its derived value
     */
    public boolean coversDerived(int subscribers) {
        return maxMessages(subscribers) >= derivedMaxMessages(subscribers)
                && maxBytes(subscribers) >= derivedMaxBytes(subscribers);
    }

    /**
     * Returns one subscriber's lapse bound in messages: twice the effective flow-control message
     * limit, and the paused-split bound's default.
     *
     * @return the message lapse bound
     */
    public long lapseBoundMessages() {
        return lapseBoundMessages;
    }

    /**
     * Returns one subscriber's lapse bound in bytes: twice the effective flow-control byte limit,
     * and the paused-split bound's default.
     *
     * @return the byte lapse bound
     */
    public long lapseBoundBytes() {
        return lapseBoundBytes;
    }

    /**
     * Rejects an explicit budget below one subscriber's lapse bound, naming the settings with the
     * given names so the builder and the table layer can each speak in their own keys. The SDK's
     * flow-control defaults are read only for a budget that is set.
     *
     * @param maxMessages the explicit message budget, or {@code null} when it is derived
     * @param maxBytes the explicit byte budget, or {@code null} when it is derived
     * @param flowControlMessages the flow-control message limit, or {@code null} for the SDK's
     * @param flowControlBytes the flow-control byte limit, or {@code null} for the SDK's
     * @param names what to call the four settings in the rejection
     * @throws IllegalArgumentException if an explicit budget is below one subscriber's lapse bound
     */
    public static void checkCoversFlowControl(
            @Nullable Long maxMessages,
            @Nullable Long maxBytes,
            @Nullable Long flowControlMessages,
            @Nullable Long flowControlBytes,
            Names names) {
        checkCovers(
                maxMessages,
                () -> effectiveFlowControlMessages(flowControlMessages),
                names.maxMessages,
                names.flowControlMessages);
        checkCovers(
                maxBytes,
                () -> effectiveFlowControlBytes(flowControlBytes),
                names.maxBytes,
                names.flowControlBytes);
    }

    private static void checkCovers(
            @Nullable Long cap,
            LongSupplier flowControlLimit,
            String capName,
            String flowControlName) {
        if (cap == null) {
            return;
        }
        long limit = flowControlLimit.getAsLong();
        long lapseBound = twice(limit);
        Preconditions.checkArgument(
                cap >= lapseBound,
                "%s (%s) is below twice the effective %s (%s). Acknowledgements wait for a"
                        + " checkpoint, so while downstream drains more slowly than Pub/Sub"
                        + " delivers, the subscriber buffer fills up to the flow-control window;"
                        + " a cap below twice that window is reached by it, or leaves too little"
                        + " headroom for the deliveries that pass it, and crossing the cap fails"
                        + " the job, and fails it again on every restart over the backlog. Leave %s"
                        + " unset to derive it, set it to at least %s, or lower %s.",
                capName,
                cap,
                flowControlName,
                limit,
                capName,
                lapseBound,
                flowControlName);
    }

    private static long effectiveFlowControlMessages(@Nullable Long configured) {
        if (configured != null) {
            return configured;
        }
        return sdkDefault(
                Subscriber.Builder.getDefaultFlowControlSettings().getMaxOutstandingElementCount(),
                "maxOutstandingElementCount");
    }

    private static long effectiveFlowControlBytes(@Nullable Long configured) {
        if (configured != null) {
            return configured;
        }
        return sdkDefault(
                Subscriber.Builder.getDefaultFlowControlSettings().getMaxOutstandingRequestBytes(),
                "maxOutstandingRequestBytes");
    }

    /**
     * The SDK's own getters are nullable — {@link FlowControlSettings} allows an unset dimension —
     * so the last rung is checked rather than assumed, and an SDK that stopped setting one fails
     * loudly rather than resolving to nothing.
     */
    private static long sdkDefault(@Nullable Long sdkDefault, String sdkSettingName) {
        Preconditions.checkState(
                sdkDefault != null,
                "The Pub/Sub client library's default flow-control settings leave %s unset, so the"
                        + " subscriber buffers have no bound to default to. Set"
                        + " flowControlMaxOutstandingElementCount and"
                        + " flowControlMaxOutstandingRequestBytes explicitly.",
                sdkSettingName);
        return sdkDefault;
    }

    private static long atLeast(long lapseBound, @Nullable Long pausedSplitBound) {
        return pausedSplitBound == null ? lapseBound : Math.max(lapseBound, pausedSplitBound);
    }

    private static long twice(long flowControlLimit) {
        return times(flowControlLimit, FLOW_CONTROL_MULTIPLE);
    }

    /** Saturating, because a limit near {@code Long.MAX_VALUE} must not wrap. */
    private static long times(long share, int count) {
        long factor = Math.max(1, count);
        return share > Long.MAX_VALUE / factor ? Long.MAX_VALUE : share * factor;
    }

    /** The names {@link #checkCoversFlowControl} reports the four settings under. */
    public static final class Names {

        /** The builder setters' names. */
        static final Names BUILDER =
                new Names(
                        "subscriberBufferMaxMessages",
                        "subscriberBufferMaxBytes",
                        "flowControlMaxOutstandingElementCount",
                        "flowControlMaxOutstandingRequestBytes");

        private final String maxMessages;
        private final String maxBytes;
        private final String flowControlMessages;
        private final String flowControlBytes;

        /**
         * Names the four settings.
         *
         * @param maxMessages the message budget's name
         * @param maxBytes the byte budget's name
         * @param flowControlMessages the flow-control message limit's name
         * @param flowControlBytes the flow-control byte limit's name
         */
        public Names(
                String maxMessages,
                String maxBytes,
                String flowControlMessages,
                String flowControlBytes) {
            this.maxMessages = maxMessages;
            this.maxBytes = maxBytes;
            this.flowControlMessages = flowControlMessages;
            this.flowControlBytes = flowControlBytes;
        }
    }
}
