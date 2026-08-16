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

import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;

/**
 * A {@link PublisherFactory} handing out in-memory fake publishers that record published messages
 * and return scripted futures (immediate success unless scripted otherwise).
 *
 * <p>The fake models the SDK publisher's paused ordering keys (#277), because the writer's repair
 * design rests on them: in {@code google-cloud-pubsub} 1.152.0 any failure of a keyed publish adds
 * the key to {@code SequentialExecutorService.CallbackExecutor.keysWithErrors} without inspecting
 * the throwable, a later publish for that key is rejected <em>before the message is batched</em>
 * with a shared static {@link CancellationException}, and nothing but an explicit {@code
 * resumePublish} clears the key. So here a failed keyed publish pauses its key, a publish on a
 * paused key is recorded in {@link FakeTopicPublisher#rejected} — not {@link
 * FakeTopicPublisher#published}, since the real message never reaches the service — and returns an
 * already-failed future without consuming a scripted one, and {@code resumePublish} unpauses.
 * Without this state a test cannot fail the eager-resume design the repair exists to rule out: a
 * publish racing a resumed key would simply succeed, and the reordering would be invisible.
 */
final class FakePublisherFactory implements PublisherFactory {

    private static final long serialVersionUID = 1L;

    /**
     * Shared instance with the SDK's own message, as {@code SequentialExecutorService} fails every
     * paused-key publish with one static {@code CancellationException}.
     */
    private static final CancellationException PAUSED_KEY_CANCELLATION =
            new CancellationException(
                    "Execution cancelled because executing previous runnable failed.");

    final Map<TopicDestination, FakeTopicPublisher> publishers = new LinkedHashMap<>();
    private final ArrayDeque<ApiFuture<String>> scriptedFutures = new ArrayDeque<>();
    int createCalls;

    /**
     * Teardown steps in the order they were called, across every publisher this factory handed out,
     * as {@code shutdown:<topic>} / {@code close:<topic>}. The writer's close has to ask every
     * publisher to shut down before it waits on any, and that ordering is only observable across
     * publishers — a per-publisher counter cannot see it.
     */
    final List<String> teardownCalls = new ArrayList<>();

    /** Scripts the future returned by the next publish on any publisher of this factory. */
    void enqueueFuture(ApiFuture<String> future) {
        scriptedFutures.add(future);
    }

    @Override
    public TopicPublisher create(TopicDestination destination) {
        createCalls++;
        FakeTopicPublisher publisher = new FakeTopicPublisher(this, destination);
        publishers.put(destination, publisher);
        return publisher;
    }

    /** In-memory fake of a per-topic publisher. */
    static final class FakeTopicPublisher implements TopicPublisher {

        private final FakePublisherFactory factory;
        private final TopicDestination destination;
        final List<PubsubMessage> published = new ArrayList<>();

        /** Keyed publishes rejected because their key was paused; never in {@link #published}. */
        final List<PubsubMessage> rejected = new ArrayList<>();

        final List<String> resumedKeys = new ArrayList<>();
        private final Set<String> pausedKeys = new LinkedHashSet<>();
        int flushCalls;
        int shutdownCalls;
        int closeCalls;
        RuntimeException publishFailure;

        /**
         * Typed {@code Throwable} so a test can script an {@code Error}, which is thrown as itself.
         */
        Throwable closeFailure;

        /** The same, for the shutdown half of the teardown. */
        Throwable shutdownFailure;

        private FakeTopicPublisher(FakePublisherFactory factory, TopicDestination destination) {
            this.factory = factory;
            this.destination = destination;
        }

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            if (publishFailure != null) {
                throw publishFailure;
            }
            String orderingKey = message.getOrderingKey();
            if (!orderingKey.isEmpty() && pausedKeys.contains(orderingKey)) {
                rejected.add(message);
                return ApiFutures.immediateFailedFuture(PAUSED_KEY_CANCELLATION);
            }
            published.add(message);
            ApiFuture<String> scripted = factory.scriptedFutures.poll();
            ApiFuture<String> future =
                    scripted != null
                            ? scripted
                            : ApiFutures.immediateFuture("message-" + published.size());
            if (!orderingKey.isEmpty()) {
                // Registered before the writer's own callback, so by the time the writer's failure
                // mail runs — however the test completes the future — the key is already paused,
                // exactly as the SDK has paused it before the application observes the failure.
                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<String>() {
                            @Override
                            public void onSuccess(String messageId) {}

                            @Override
                            public void onFailure(Throwable throwable) {
                                pausedKeys.add(orderingKey);
                            }
                        },
                        Runnable::run);
            }
            return future;
        }

        @Override
        public void resumePublish(String orderingKey) {
            resumedKeys.add(orderingKey);
            pausedKeys.remove(orderingKey);
        }

        @Override
        public void flushOutstanding() {
            flushCalls++;
        }

        @Override
        public void shutdown() {
            shutdownCalls++;
            factory.teardownCalls.add("shutdown:" + destination);
            if (shutdownFailure != null) {
                ExceptionUtils.rethrow(shutdownFailure);
            }
        }

        @Override
        public void close() {
            closeCalls++;
            factory.teardownCalls.add("close:" + destination);
            if (closeFailure != null) {
                ExceptionUtils.rethrow(closeFailure);
            }
        }
    }
}
