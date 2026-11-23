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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The {@code google-cloud-pubsub} subscriber behind one split, with the transport it runs on.
 *
 * <p>An interface rather than the concrete {@code com.google.cloud.pubsub.v1.Subscriber}, as {@code
 * TopicPublisher} is on the sink side, so the client and its hold on the connector's gRPC
 * event-loop group (ADR-0208) travel as one value, which the SDK type cannot carry, and the owner
 * releases the hold through it. Unlike {@code TopicPublisher}'s, its close releases only the
 * transport: {@link StreamingPullSubscriber} runs the stop and the bounded wait itself, so it can
 * tell a failure it already reported from a new one at teardown.
 */
@Internal
public interface SubscriberClient extends AutoCloseable {

    /**
     * Registers the permanent-failure sink, then starts the client and returns once it is running.
     * The sink is registered first so a failure raised during the start is not missed.
     *
     * @param onPermanentFailure receives the failure that stopped the client
     */
    void start(Consumer<Throwable> onPermanentFailure);

    /** Asks the client to stop, without waiting for it. */
    void stopAsync();

    /**
     * Waits a bounded time for the client to terminate.
     *
     * @param timeout how long to wait
     * @param unit the unit of {@code timeout}
     * @throws TimeoutException if the client has not terminated in time
     */
    void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException;

    /**
     * Releases the client's transport. Call it once the client has terminated or been given up on:
     * the group stops under any channel still open on it. In production the client owns its
     * channels, and a terminated one has cancelled its calls and asked them to shut down without
     * waiting for them ({@code Subscriber.runShutdown}, google-cloud-pubsub 1.160.0), so the
     * group's own shutdown is what closes whatever is left when this was its last lease. With an
     * emulator the channel is the transport's, which the client does not close, so this shuts it
     * down and waits a bounded time for it. Repeated closes release the lease only once.
     */
    @Override
    void close();
}
