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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Child-process entry point for {@link PubSubPublisherLifecycleBoundaryTest}. */
final class PubSubPublisherLifecycleProbe {

    private static final String PROJECT = "test-project";
    private static final int DESTINATIONS = 10_000;
    private static final long MIB = 1024L * 1024L;

    private PubSubPublisherLifecycleProbe() {}

    public static void main(String[] args) throws Exception {
        AtomicInteger published = new AtomicInteger();
        PubSubWriter<String> writer = null;
        try {
            PubSubPublisherOptions options =
                    PubSubPublisherOptions.builder()
                            .maxActivePublishers(100)
                            .shutdownTimeout(Duration.ofSeconds(5))
                            .build();
            FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
            TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
            writer =
                    new PubSubWriter<>(
                            TestSinkConfigs.forResolver(
                                    (element, context) -> TopicDestination.of(PROJECT, element),
                                    PubSubSerializationSchema.payload(new SimpleStringSchema()),
                                    options),
                            realPublisherFactory(options, published),
                            new FakeTopicAdmin(),
                            mailbox,
                            metrics,
                            new RetrySchedule(1, 1, 1, 0));

            int baselineThreads = liveThreads();
            long heapAtOneHundred = 0;
            Map<Integer, Long> checkpointMillis = new LinkedHashMap<>();
            for (int i = 1; i <= DESTINATIONS; i++) {
                writer.write("topic-" + i, TestContexts.NO_OP);
                if (i == 1) {
                    // Keep class loading and first-use SDK work outside the timed observations.
                    writer.flush(false);
                }
                if (i == 1 || i == 100 || i == 1_000 || i == DESTINATIONS) {
                    checkpointMillis.put(i, fastestCheckpointMillis(writer));
                }
                if (i == 100) {
                    heapAtOneHundred = usedHeapAfterGc();
                }
            }

            long heapAtTenThousand = usedHeapAfterGc();
            int threadDelta = liveThreads() - baselineThreads;
            int active = metrics.gaugeValue("activePublishers");
            long heapGrowth = heapAtTenThousand - heapAtOneHundred;
            require(active == 100, "active=" + active);
            require(published.get() == DESTINATIONS, "published=" + published);
            require(threadDelta <= 130, "threadDelta=" + threadDelta);
            require(heapGrowth <= 32 * MIB, "heapGrowth=" + heapGrowth);
            for (int count : new int[] {1, 100, 1_000, DESTINATIONS}) {
                require(
                        checkpointMillis.get(count) < 500,
                        "checkpoint" + count + "=" + checkpointMillis.get(count));
            }
            System.out.printf(
                    "PASS active=%d published=%d threadDelta=%d heapGrowthMiB=%.2f checkpoints=%s%n",
                    active,
                    published.get(),
                    threadDelta,
                    heapGrowth / (double) MIB,
                    checkpointMillis);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private static PublisherFactory realPublisherFactory(
            PubSubPublisherOptions options, AtomicInteger published) {
        return destination -> {
            Publisher.Builder builder =
                    Publisher.newBuilder(destination.toTopicPath())
                            .setCredentialsProvider(NoCredentialsProvider.create());
            DefaultPublisherFactory.configure(builder, options);
            Publisher publisher = builder.build();
            return new InterceptedPublisher(
                    new DefaultPublisherFactory.PublisherAdapter(
                            publisher, destination, null, options.getShutdownTimeout()),
                    published);
        };
    }

    private static int liveThreads() {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }

    private static long fastestCheckpointMillis(PubSubWriter<String> writer) throws Exception {
        long fastestNanos = Long.MAX_VALUE;
        for (int sample = 0; sample < 5; sample++) {
            long started = System.nanoTime();
            writer.flush(false);
            fastestNanos = Math.min(fastestNanos, System.nanoTime() - started);
        }
        return TimeUnit.NANOSECONDS.toMillis(fastestNanos);
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
        }
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void require(boolean condition, String failure) {
        if (!condition) {
            throw new AssertionError(failure);
        }
    }

    /** Holds a real SDK publisher while completing the probe's publishes without a service call. */
    private static final class InterceptedPublisher implements TopicPublisher {
        private final DefaultPublisherFactory.PublisherAdapter delegate;
        private final AtomicInteger published;

        private InterceptedPublisher(
                DefaultPublisherFactory.PublisherAdapter delegate, AtomicInteger published) {
            this.delegate = delegate;
            this.published = published;
        }

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            return ApiFutures.immediateFuture("message-" + published.incrementAndGet());
        }

        @Override
        public void resumePublish(String orderingKey) {
            delegate.resumePublish(orderingKey);
        }

        @Override
        public void flushOutstanding() {
            delegate.flushOutstanding();
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public boolean wasShutdownIncomplete() {
            return delegate.wasShutdownIncomplete();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
