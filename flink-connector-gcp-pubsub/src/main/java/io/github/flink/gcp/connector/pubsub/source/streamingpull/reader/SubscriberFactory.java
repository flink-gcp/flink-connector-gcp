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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.io.IOException;

/**
 * Creates the {@code google-cloud-pubsub} {@link Subscriber} backing one split.
 *
 * <p>Instances are created on the task manager inside {@code createReader} and hold no client state
 * of their own, so nothing here is shipped in the job graph. Returned subscribers are not started —
 * the caller starts each one only after registering its failure listener, so a startup failure is
 * not missed.
 */
@Internal
public interface SubscriberFactory {

    /**
     * Creates a subscriber delivering the given subscription's messages to the consumer.
     *
     * @param subscription the subscription to consume
     * @param consumer invoked for every received message
     * @return the created, not yet started subscriber
     * @throws IOException if the subscriber cannot be created
     */
    Subscriber create(SubscriptionDestination subscription, MessageConsumer consumer)
            throws IOException;

    /**
     * Receives a message together with the handle that settles it.
     *
     * <p>This stands in for the client library's two receiver interfaces, which differ only in
     * whether settling returns a future. Which one backs a given subscriber follows from the
     * subscriber options and is decided by the factory, so callers never see the difference.
     */
    @FunctionalInterface
    @Internal
    interface MessageConsumer {

        /**
         * Receives a message on one of the client library's callback threads.
         *
         * @param message the received message
         * @param ackHandle settles the message
         */
        void receive(PubsubMessage message, AckHandle ackHandle);
    }
}
