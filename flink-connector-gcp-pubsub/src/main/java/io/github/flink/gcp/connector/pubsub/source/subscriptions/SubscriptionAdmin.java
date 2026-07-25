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

package io.github.flink.gcp.connector.pubsub.source.subscriptions;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;

/**
 * Subscription administration used by the enumerator's startup check, abstracting the Pub/Sub admin
 * client so the check can be unit-tested without one.
 *
 * <p>Instances are created on the job manager for the split enumerator and are never shipped in the
 * job graph, so the interface is not {@link java.io.Serializable}. It is {@link AutoCloseable} so
 * an implementation holding a client can release it with the enumerator.
 */
@Internal
public interface SubscriptionAdmin extends AutoCloseable {

    /**
     * Returns the settings of the given subscription, or {@code null} if it does not exist.
     *
     * @param subscription the subscription to describe
     * @return the subscription's settings, or {@code null} when it does not exist
     * @throws IOException if the subscription cannot be read for any other reason
     */
    @Nullable
    SubscriptionInfo describe(SubscriptionDestination subscription) throws IOException;

    /**
     * Creates the given subscription with the given settings. Idempotent: creating a subscription
     * that already exists succeeds silently, leaving the existing one untouched.
     *
     * @param subscription the subscription to create
     * @param options the settings to create it with
     * @throws IOException if the creation fails for any reason other than it already existing
     */
    void create(SubscriptionDestination subscription, SubscriptionCreateOptions options)
            throws IOException;

    /**
     * Seeks the subscription to the given instant: messages published before it are marked
     * acknowledged and messages published after it unacknowledged.
     *
     * <p>This rewrites state shared by every consumer of the subscription, and Pub/Sub applies it
     * asynchronously — deliveries already in flight can take up to a minute to reflect it.
     *
     * @param subscription the subscription to seek
     * @param timestamp the publish time to seek to
     * @throws IOException if the seek fails
     */
    void seek(SubscriptionDestination subscription, Instant timestamp) throws IOException;

    @Override
    void close() throws Exception;
}
