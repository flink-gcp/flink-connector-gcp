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

import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import java.io.IOException;
import java.time.Duration;

/**
 * The production {@link PullSubscriberOpener}: a {@link StreamingPullSubscriber} over the reader's
 * client factory and acknowledgement tracker.
 */
@Internal
final class DefaultPullSubscriberOpener implements PullSubscriberOpener {

    private final SubscriberFactory subscriberFactory;
    private final AckTracker ackTracker;
    private final Duration shutdownTimeout;
    private final SubscriberBufferBudget bufferBudget;

    /**
     * @param subscriberFactory creates the client backing each split
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     * @param shutdownTimeout the per-subscriber shutdown budget
     */
    DefaultPullSubscriberOpener(
            SubscriberFactory subscriberFactory,
            AckTracker ackTracker,
            Duration shutdownTimeout,
            SubscriberBufferBudget bufferBudget) {
        this.subscriberFactory = subscriberFactory;
        this.ackTracker = ackTracker;
        this.shutdownTimeout = shutdownTimeout;
        this.bufferBudget = bufferBudget;
    }

    @Override
    public PullSubscriber open(SubscriptionSplit split, Runnable dataAvailableSignal)
            throws IOException {
        return new StreamingPullSubscriber(
                split.splitId(),
                split.getSubscription(),
                subscriberFactory,
                ackTracker,
                dataAvailableSignal,
                shutdownTimeout,
                bufferBudget);
    }
}
