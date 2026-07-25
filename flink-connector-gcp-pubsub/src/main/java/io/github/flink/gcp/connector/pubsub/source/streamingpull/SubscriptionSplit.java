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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.util.Objects;

/**
 * A unit of work for one reader subtask: a streaming-pull connection to one subscription.
 *
 * <p>The split carries <em>no progress state</em>. Pub/Sub has no partition offset or cursor a
 * reader could resume from — delivery state lives on the server, and a message that is not
 * acknowledged is redelivered. Consequently a split is fully described by its subscription and a
 * uid, restoring a split costs nothing, and returned splits can simply be re-derived.
 *
 * <p>The uid distinguishes several splits that target the same subscription, which the source
 * creates when parallelism exceeds the subscription count under {@link
 * io.github.flink.gcp.connector.pubsub.source.OrderingMode#NONE}. Under {@code PER_KEY} a
 * subscription appears in exactly one split, so that its ordering keys are never spread across
 * reader subtasks.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which derives the uid from the subtask index and serializes the split with protobuf.
 */
@Internal
public final class SubscriptionSplit implements SourceSplit {

    private final SubscriptionDestination subscription;
    private final String uid;
    private final String splitId;

    /**
     * Creates a split.
     *
     * @param subscription the subscription to consume
     * @param uid distinguishes splits sharing a subscription; unique within one job
     */
    public SubscriptionSplit(SubscriptionDestination subscription, String uid) {
        this.subscription = Preconditions.checkNotNull(subscription, "subscription");
        this.uid = Preconditions.checkNotNull(uid, "uid");
        this.splitId = subscription.toSubscriptionPath() + "#" + uid;
    }

    /** Returns the subscription this split consumes. */
    public SubscriptionDestination getSubscription() {
        return subscription;
    }

    /** Returns the uid distinguishing this split from others on the same subscription. */
    public String getUid() {
        return uid;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionSplit that = (SubscriptionSplit) o;
        return splitId.equals(that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    @Override
    public String toString() {
        return "SubscriptionSplit{" + splitId + "}";
    }
}
