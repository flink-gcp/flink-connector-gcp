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

package io.github.flink.gcp.connector.pubsub.sink.publisher.writer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link PublisherFactory} handing out in-memory fake publishers that record published messages
 * and return scripted futures (immediate success unless scripted otherwise).
 */
final class FakePublisherFactory implements PublisherFactory {

    private static final long serialVersionUID = 1L;

    final Map<TopicDestination, FakeTopicPublisher> publishers = new LinkedHashMap<>();
    private final ArrayDeque<ApiFuture<String>> scriptedFutures = new ArrayDeque<>();
    int createCalls;

    /** Scripts the future returned by the next publish on any publisher of this factory. */
    void enqueueFuture(ApiFuture<String> future) {
        scriptedFutures.add(future);
    }

    @Override
    public TopicPublisher create(TopicDestination destination) {
        createCalls++;
        FakeTopicPublisher publisher = new FakeTopicPublisher(this);
        publishers.put(destination, publisher);
        return publisher;
    }

    /** In-memory fake of a per-topic publisher. */
    static final class FakeTopicPublisher implements TopicPublisher {

        private final FakePublisherFactory factory;
        final List<PubsubMessage> published = new ArrayList<>();
        final List<String> resumedKeys = new ArrayList<>();
        int flushCalls;
        int closeCalls;
        RuntimeException publishFailure;
        RuntimeException closeFailure;

        private FakeTopicPublisher(FakePublisherFactory factory) {
            this.factory = factory;
        }

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            if (publishFailure != null) {
                throw publishFailure;
            }
            published.add(message);
            ApiFuture<String> scripted = factory.scriptedFutures.poll();
            return scripted != null
                    ? scripted
                    : ApiFutures.immediateFuture("message-" + published.size());
        }

        @Override
        public void resumePublish(String orderingKey) {
            resumedKeys.add(orderingKey);
        }

        @Override
        public void flushOutstanding() {
            flushCalls++;
        }

        @Override
        public void close() {
            closeCalls++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
