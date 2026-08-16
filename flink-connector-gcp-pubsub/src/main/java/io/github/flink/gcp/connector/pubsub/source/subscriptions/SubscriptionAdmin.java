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
 * job graph, so the interface is not {@link java.io.Serializable}.
 *
 * <p><b>{@link #close()} can be called while another method is still running.</b> The enumerator
 * closes its admin from the scheduler thread while the check may still be in flight on a worker
 * thread, so an implementation must not tear down state that an in-flight call is using — which is
 * why the default implementation gives each call its own client instead of sharing one. Flink also
 * skips {@code close()} entirely when the coordinator never started, so nothing may depend on it
 * running.
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
     * Creates the given subscription with the given settings and returns the settings it ended up
     * with. Idempotent: creating a subscription that already exists succeeds, leaving the existing
     * one untouched — in which case the returned settings are that subscription's, not the
     * requested ones, which is why the caller is handed them rather than deriving them from the
     * options.
     *
     * @param subscription the subscription to create
     * @param options the settings to create it with
     * @return the settings the subscription has now
     * @throws IOException if the creation fails for any reason other than it already existing
     */
    SubscriptionInfo create(SubscriptionDestination subscription, SubscriptionCreateOptions options)
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
