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

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;

import javax.annotation.Nullable;

/**
 * Settles one delivered message, hiding which of the client library's two receiver flavors produced
 * it.
 *
 * <p>The SDK exposes {@link AckReplyConsumer} (fire and forget) and {@link
 * AckReplyConsumerWithResponse} (returning a future) through <em>different</em> receiver
 * interfaces, chosen when the subscriber is built. Only {@link DefaultSubscriberFactory} knows
 * which one the options call for, so everything above it settles messages through this interface
 * instead.
 */
@Internal
public interface AckHandle {

    /**
     * Acknowledges the message.
     *
     * @return a future completing with the server's response, or {@code null} when the subscriber
     *     was built without acknowledgement confirmation
     */
    @Nullable
    ApiFuture<AckResponse> ack();

    /** Nacks the message, so Pub/Sub redelivers it immediately. */
    void nack();

    /**
     * Wraps a fire-and-forget consumer.
     *
     * @param consumer the SDK consumer
     * @return the handle
     */
    static AckHandle of(AckReplyConsumer consumer) {
        return new AckHandle() {

            @Override
            @Nullable
            public ApiFuture<AckResponse> ack() {
                consumer.ack();
                return null;
            }

            @Override
            public void nack() {
                consumer.nack();
            }
        };
    }

    /**
     * Wraps a consumer whose settlements report the server's response.
     *
     * @param consumer the SDK consumer
     * @return the handle
     */
    static AckHandle of(AckReplyConsumerWithResponse consumer) {
        return new AckHandle() {

            @Override
            public ApiFuture<AckResponse> ack() {
                return consumer.ack();
            }

            @Override
            public void nack() {
                // The returned future is dropped on purpose: a nack asks for redelivery, and
                // nothing downstream waits on redelivery having been registered.
                consumer.nack();
            }
        };
    }
}
