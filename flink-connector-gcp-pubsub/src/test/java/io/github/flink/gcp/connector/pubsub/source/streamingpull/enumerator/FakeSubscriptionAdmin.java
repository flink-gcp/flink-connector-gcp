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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory {@link SubscriptionAdmin} over a set of subscriptions the test declares to exist,
 * recording creations and seeks, with a scriptable failure per operation.
 */
final class FakeSubscriptionAdmin implements SubscriptionAdmin {

    private final Map<SubscriptionDestination, SubscriptionInfo> existing = new LinkedHashMap<>();

    final List<SubscriptionDestination> created = new ArrayList<>();
    final List<SubscriptionDestination> seekedSubscriptions = new ArrayList<>();
    final List<Instant> seekTimes = new ArrayList<>();

    IOException describeFailure;
    IOException createFailure;
    IOException seekFailure;
    int closeCalls;

    /** Declares a subscription to exist with default settings. */
    FakeSubscriptionAdmin withSubscription(SubscriptionDestination subscription) {
        return withSubscription(subscription, SubscriptionInfo.builder().build());
    }

    /** Declares a subscription to exist with the given settings. */
    FakeSubscriptionAdmin withSubscription(
            SubscriptionDestination subscription, SubscriptionInfo info) {
        existing.put(subscription, info);
        return this;
    }

    @Override
    @Nullable
    public SubscriptionInfo describe(SubscriptionDestination subscription) throws IOException {
        if (describeFailure != null) {
            throw describeFailure;
        }
        return existing.get(subscription);
    }

    @Override
    public void create(SubscriptionDestination subscription, SubscriptionCreateOptions options)
            throws IOException {
        if (createFailure != null) {
            throw createFailure;
        }
        created.add(subscription);
        existing.put(
                subscription,
                SubscriptionInfo.builder()
                        .messageOrderingEnabled(options.isEnableMessageOrdering())
                        .retainAckedMessages(options.isRetainAckedMessages())
                        .deadLetterPolicyConfigured(options.getDeadLetterTopic() != null)
                        .build());
    }

    @Override
    public void seek(SubscriptionDestination subscription, Instant timestamp) throws IOException {
        if (seekFailure != null) {
            throw seekFailure;
        }
        seekedSubscriptions.add(subscription);
        seekTimes.add(timestamp);
    }

    @Override
    public void close() {
        closeCalls++;
    }
}
