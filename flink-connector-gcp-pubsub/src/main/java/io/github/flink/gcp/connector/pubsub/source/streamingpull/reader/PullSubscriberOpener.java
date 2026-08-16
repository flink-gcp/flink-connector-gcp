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

/** Opens the subscriber backing one split; the seam that lets tests supply a fake client. */
@FunctionalInterface
@Internal
interface PullSubscriberOpener {

    /**
     * Opens a subscriber for the given split.
     *
     * @param split the split to consume
     * @param dataAvailableSignal invoked when the subscriber has messages or has failed
     * @return the opened subscriber
     * @throws IOException if the subscriber cannot be opened
     */
    PullSubscriber open(SubscriptionSplit split, Runnable dataAvailableSignal) throws IOException;
}
