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

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/** Child-process entry point for {@link SubscriberBufferMemoryBoundaryTest}. */
final class SubscriberBufferMemoryProbe {

    private static final String SPLIT_ID = "memory-probe";
    private static final int PAYLOAD_BYTES = 4 * 1024;

    private SubscriberBufferMemoryProbe() {}

    public static void main(String[] args) throws Exception {
        PubSubSubscriberOptions options = PubSubSubscriberOptions.builder().build();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger stops = new AtomicInteger();
        SubscriberBufferBudget budget =
                new SubscriberBufferBudget(
                        options.getSubscriberBufferMaxMessages(),
                        options.getSubscriberBufferMaxBytes(),
                        event -> rejected.incrementAndGet());
        PubSubAckTracker ackTracker = new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
        StreamingPullSubscriber subscriber =
                new StreamingPullSubscriber(
                        SPLIT_ID,
                        SubscriptionDestination.of("probe-project", "probe-subscription"),
                        ackTracker,
                        () -> {},
                        Duration.ofSeconds(1),
                        budget,
                        onFailure -> {},
                        stops::incrementAndGet,
                        (timeout, unit) -> {});
        budget.register(SPLIT_ID, subscriber::requestStop);

        byte[] payload = new byte[PAYLOAD_BYTES];
        long limit = options.getSubscriberBufferMaxMessages();
        for (int index = 0; index <= limit; index++) {
            String id = "m" + index;
            subscriber.receiveMessage(
                    PubsubMessage.newBuilder()
                            .setMessageId(id)
                            .setData(ByteString.copyFrom(payload))
                            .build(),
                    new RecordingAckHandle(id));
        }

        long buffered = subscriber.bufferUsage().messages();
        if (buffered != limit || rejected.get() != 1 || stops.get() == 0) {
            throw new AssertionError(
                    "buffered=" + buffered + ", rejected=" + rejected + ", stops=" + stops);
        }
        System.out.println("PASS buffered=" + buffered + " rejected=" + rejected.get());
        subscriber.shutdown();
        budget.unregister(SPLIT_ID);
    }
}
