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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Checkpointed state of the split enumerator: the subscriptions it resolved.
 *
 * <p>Split assignments are deliberately <em>not</em> checkpointed. Splits carry no progress state
 * and the assignment is a pure function of the subscription list, the ordering mode and the current
 * parallelism, so recomputing it on restore is both simpler and correct across a parallelism change
 * — reconciling stale assignments would not be.
 *
 * <p>The subscription list is recorded so that a restore whose configured subscriptions differ from
 * the checkpointed ones can be reported rather than silently taking effect.
 */
@Internal
public final class PubSubEnumeratorState {

    private final List<SubscriptionDestination> subscriptions;

    /**
     * Creates the state.
     *
     * @param subscriptions the subscriptions the enumerator resolved
     */
    public PubSubEnumeratorState(List<SubscriptionDestination> subscriptions) {
        this.subscriptions =
                Collections.unmodifiableList(
                        Preconditions.checkNotNull(subscriptions, "subscriptions"));
    }

    /** Returns the subscriptions the enumerator resolved, in assignment order. */
    public List<SubscriptionDestination> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubEnumeratorState that = (PubSubEnumeratorState) o;
        return subscriptions.equals(that.subscriptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptions);
    }

    @Override
    public String toString() {
        return "PubSubEnumeratorState{subscriptions=" + subscriptions + "}";
    }
}
