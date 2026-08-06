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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * {@link PublisherFactory} building {@code google-cloud-pubsub} {@link Publisher} instances
 * configured from {@link PubSubPublisherOptions}; every knob left unset keeps the SDK default
 * (options with no batching or retry overrides leave the publisher builder untouched, so the
 * default configuration is byte-identical to the SDK's).
 *
 * <p>When an emulator endpoint is set, publishers connect to it over a plaintext channel with no
 * credentials; each publisher owns its channel and shuts it down on close.
 */
@Internal
public final class DefaultPublisherFactory implements PublisherFactory {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPublisherFactory.class);

    /**
     * Mirror of the SDK publisher's default {@code RetrySettings} ({@code
     * Publisher.Builder.DEFAULT_RETRY_SETTINGS} is package-private): retry overrides start from
     * these values so unset knobs keep the SDK behavior. A drift-guard test pins this mirror to the
     * SDK constant.
     */
    @VisibleForTesting
    static final RetrySettings DEFAULT_RETRY_SETTINGS =
            RetrySettings.newBuilder()
                    .setTotalTimeoutDuration(Duration.ofSeconds(600))
                    .setInitialRetryDelayDuration(Duration.ofMillis(100))
                    .setRetryDelayMultiplier(4)
                    .setMaxRetryDelayDuration(Duration.ofSeconds(60))
                    .setInitialRpcTimeoutDuration(Duration.ofSeconds(5))
                    .setRpcTimeoutMultiplier(4)
                    .setMaxRpcTimeoutDuration(Duration.ofSeconds(60))
                    .build();

    private final PubSubPublisherOptions options;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates a factory connecting to production Pub/Sub with application-default credentials.
     *
     * @param options the publisher tuning options
     */
    public DefaultPublisherFactory(PubSubPublisherOptions options) {
        this(options, null);
    }

    /**
     * Creates the factory.
     *
     * @param options the publisher tuning options
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Pub/Sub
     */
    public DefaultPublisherFactory(
            PubSubPublisherOptions options, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.options = options;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public TopicPublisher create(TopicDestination destination) throws IOException {
        Publisher.Builder builder = Publisher.newBuilder(destination.toTopicPath());
        ManagedChannel ownedChannel = null;
        try {
            if (emulatorEndpoint != null) {
                ownedChannel =
                        ManagedChannelBuilder.forTarget(emulatorEndpoint.getTarget())
                                .usePlaintext()
                                .build();
                builder.setChannelProvider(
                                FixedTransportChannelProvider.create(
                                        GrpcTransportChannel.create(ownedChannel)))
                        .setCredentialsProvider(NoCredentialsProvider.create());
            }
            configure(builder, options);
            return new PublisherAdapter(
                    builder.build(), destination, ownedChannel, options.getShutdownTimeout());
        } catch (IOException | RuntimeException e) {
            // The channel is owned here until the adapter takes it over on success.
            if (ownedChannel != null) {
                ownedChannel.shutdownNow();
            }
            throw e;
        }
    }

    /** Applies the options onto the publisher builder; unset knobs are left at SDK defaults. */
    @VisibleForTesting
    static void configure(Publisher.Builder builder, PubSubPublisherOptions options) {
        builder.setEnableMessageOrdering(options.isEnableMessageOrdering());
        if (options.hasBatchingOverrides()) {
            builder.setBatchingSettings(batchingSettings(options));
        }
        if (options.hasRetryOverrides()) {
            builder.setRetrySettings(retrySettings(options));
        }
    }

    /**
     * Builds the SDK batching settings: the SDK defaults overlaid with the set thresholds. The flow
     * controller is left at the SDK default of {@code LimitExceededBehavior.Ignore}, for which
     * {@code Publisher} constructs no controller at all — in-flight publishes are bounded by the
     * writer instead (see {@link PubSubPublisherOptions}).
     */
    @VisibleForTesting
    static BatchingSettings batchingSettings(PubSubPublisherOptions options) {
        BatchingSettings.Builder batching =
                Publisher.Builder.getDefaultBatchingSettings().toBuilder();
        if (options.getBatchElementCountThreshold() != null) {
            batching.setElementCountThreshold(options.getBatchElementCountThreshold());
        }
        if (options.getBatchRequestByteThreshold() != null) {
            batching.setRequestByteThreshold(options.getBatchRequestByteThreshold());
        }
        if (options.getBatchDelayThreshold() != null) {
            batching.setDelayThresholdDuration(options.getBatchDelayThreshold());
        }
        return batching.build();
    }

    /**
     * Builds the SDK retry settings: the mirrored SDK defaults overlaid with the set knobs (the
     * SDK's own defaults are package-private; see {@link #DEFAULT_RETRY_SETTINGS}).
     */
    @VisibleForTesting
    static RetrySettings retrySettings(PubSubPublisherOptions options) {
        RetrySettings.Builder retry = DEFAULT_RETRY_SETTINGS.toBuilder();
        if (options.getRetryTotalTimeout() != null) {
            retry.setTotalTimeoutDuration(options.getRetryTotalTimeout());
        }
        if (options.getRetryInitialDelay() != null) {
            retry.setInitialRetryDelayDuration(options.getRetryInitialDelay());
        }
        if (options.getRetryDelayMultiplier() != null) {
            retry.setRetryDelayMultiplier(options.getRetryDelayMultiplier());
        }
        if (options.getRetryMaxDelay() != null) {
            retry.setMaxRetryDelayDuration(options.getRetryMaxDelay());
        }
        if (options.getRetryInitialRpcTimeout() != null) {
            retry.setInitialRpcTimeoutDuration(options.getRetryInitialRpcTimeout());
        }
        if (options.getRetryRpcTimeoutMultiplier() != null) {
            retry.setRpcTimeoutMultiplier(options.getRetryRpcTimeoutMultiplier());
        }
        if (options.getRetryMaxRpcTimeout() != null) {
            retry.setMaxRpcTimeoutDuration(options.getRetryMaxRpcTimeout());
        }
        if (options.getRetryMaxAttempts() != null) {
            retry.setMaxAttempts(options.getRetryMaxAttempts());
        }
        return retry.build();
    }

    /** Adapts the SDK {@link Publisher} to the writer-facing {@link TopicPublisher} interface. */
    @VisibleForTesting
    static final class PublisherAdapter implements TopicPublisher {

        private final Publisher publisher;

        /** Package-private so a test can read the budget {@link #create} handed over. */
        @VisibleForTesting final BoundedShutdown teardown;

        private PublisherAdapter(
                Publisher publisher,
                TopicDestination destination,
                @Nullable ManagedChannel ownedChannel,
                Duration shutdownTimeout) {
            this.publisher = publisher;
            this.teardown =
                    new BoundedShutdown(
                            publisher::shutdown,
                            publisher::awaitTermination,
                            destination,
                            ownedChannel,
                            shutdownTimeout);
        }

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            return publisher.publish(message);
        }

        @Override
        public void resumePublish(String orderingKey) {
            publisher.resumePublish(orderingKey);
        }

        @Override
        public void flushOutstanding() {
            publisher.publishAllOutstanding();
        }

        @Override
        public void shutdown() {
            teardown.start();
        }

        @Override
        public void close() throws Exception {
            teardown.close();
        }
    }

    /**
     * The teardown of one SDK publisher: both of its steps on a separate thread, and one deadline
     * that the task thread's single {@code join} is the whole of.
     *
     * <p>The bound is the point, and two independent things need it. {@code Publisher.shutdown()}
     * waits on a counter of accepted publishes, uninterruptibly and with no timeout, until it is
     * exactly zero.
     *
     * <p>The counter can be left permanently above zero: the failure callback cancels the messages
     * still accumulating in a failed ordering key's un-flushed batch and removes the batch, but
     * decrements only by the size of the batch that was in flight, so those increments are never
     * returned (measured on {@code google-cloud-pubsub} 1.152.0; issue #265).
     *
     * <p>And it can simply take arbitrarily long to reach zero, with nothing defective involved:
     * with {@code enableMessageOrdering} the SDK overrides the publisher's retry settings to {@code
     * maxAttempts = Integer.MAX_VALUE} and an effectively infinite total timeout — for unkeyed
     * messages too, as its own {@code TODO} notes — so during an outage the in-flight publishes
     * retry forever and the counter never drains. An ordered sink therefore needs this bound
     * whatever the SDK version, which is why it is not written as a workaround. That same override
     * is why {@code PubSubPublisherOptions} rejects an explicit {@code retryTotalTimeout} or {@code
     * retryMaxAttempts} beside ordering: neither would reach the publisher.
     *
     * <p>A separate thread is therefore the only lever available: the wait cannot be interrupted,
     * and {@code Publisher} offers no forcible variant. It is a daemon thread so one that never
     * returns cannot keep a JVM from exiting, and a plain thread rather than an executor because
     * {@code shutdownNow()} could not interrupt that wait either — the thread would leak just the
     * same, and the executor would then need a bounded teardown of its own.
     *
     * <p><b>{@code awaitTermination} runs on that thread too</b>, rather than on the task thread
     * after a successful join, and that placement is load-bearing: gax's {@code
     * BackgroundResourceAggregation.awaitTermination} passes the <em>full</em> duration to every
     * resource in turn (its own source carries the {@code TODO subtract time already used up from
     * previous resources}), and a publisher nests several — its executor, then the stub's transport
     * channel and watchdog. Awaiting on the task thread would therefore cost a multiple of the
     * timeout, not the timeout. Here it costs the daemon thread's time and nothing else.
     *
     * <p>Anything either step throws is captured and rethrown by {@link #close()} with its own
     * type, because a thread's uncaught exception would otherwise reach only Flink's JVM-wide
     * handler — losing a teardown failure the writer used to report, and, under {@code
     * cluster.uncaught-exception-handling: FAIL}, turning it into a TaskManager exit.
     *
     * <p>The two steps are held as functional values rather than as a {@link Publisher}, which is
     * final: this is the only seam a test can drive.
     *
     * <p>Every field but {@link #failure} is confined to the task thread — {@link #start()} and
     * {@link #close()} are called only from {@code PubSubWriter}, which is single-threaded — and
     * they are published to the shutdown thread by {@code Thread.start()}. {@link #failure} is
     * written by that thread, and read after a join that may have timed out, so it is volatile.
     */
    @VisibleForTesting
    static final class BoundedShutdown implements AutoCloseable {

        /** The publisher's own bounded wait, satisfied by {@code Publisher::awaitTermination}. */
        @FunctionalInterface
        interface TerminationWait {
            boolean await(long timeout, TimeUnit unit) throws InterruptedException;
        }

        private final Runnable shutdown;
        private final TerminationWait awaitTermination;
        private final TopicDestination destination;
        @Nullable private final ManagedChannel ownedChannel;
        private final Duration timeout;

        @Nullable private Thread thread;
        private long deadlineNanos;
        @Nullable private volatile Throwable failure;

        /**
         * Set once {@link #close()} has stopped waiting, so a later failure knows to report itself.
         */
        private volatile boolean abandoned;

        BoundedShutdown(
                Runnable shutdown,
                TerminationWait awaitTermination,
                TopicDestination destination,
                @Nullable ManagedChannel ownedChannel,
                Duration timeout) {
            this.shutdown = shutdown;
            this.awaitTermination = awaitTermination;
            this.destination = destination;
            this.ownedChannel = ownedChannel;
            this.timeout = timeout;
        }

        /** The budget, for the test that checks which one {@link #create} handed over. */
        @VisibleForTesting
        Duration timeout() {
            return timeout;
        }

        /** Returns the budget the timeout has not yet used, in nanoseconds; never negative. */
        private long remainingNanos() {
            return Math.max(deadlineNanos - System.nanoTime(), 0);
        }

        /**
         * Starts the publisher's teardown and the clock, without waiting for either. Idempotent,
         * and deliberately does not restart the clock: a writer starts every publisher's teardown
         * before it closes any, and a second call resetting the deadline would turn its one timeout
         * back into one per topic.
         */
        void start() {
            if (thread != null) {
                return;
            }
            deadlineNanos = System.nanoTime() + timeout.toNanos();
            // Named after the caller as well as the topic, because the caller is the task thread
            // and Flink names it "Sink: Writer (2/4)#1" — without that, every subtask on a
            // TaskManager writing the same topic leaves identically-named threads behind, and a
            // thread dump cannot say which subtask leaked or on which attempt. Flink's own
            // SplitFetcherManager names fetcher threads the same way.
            thread =
                    new Thread(
                            this::shutdownAndAwait,
                            "pubsub-publisher-shutdown-"
                                    + destination
                                    + " for "
                                    + Thread.currentThread().getName());
            thread.setDaemon(true);
            thread.start();
        }

        private void shutdownAndAwait() {
            try {
                shutdown.run();
                long remainingNanos = remainingNanos();
                if (!awaitTermination.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                    LOG.warn(
                            "The Pub/Sub publisher for topic {} shut down but its resources did not"
                                    + " terminate within the {} of its {} shutdown budget that were"
                                    + " left; they may leak until the JVM exits.",
                            destination,
                            Duration.ofNanos(remainingNanos),
                            timeout);
                }
            } catch (Throwable t) {
                failure = t;
                if (abandoned) {
                    // Nothing will read the field: close() already gave up and returned, so this
                    // is the only report this failure will ever get. Reachable in practice — a
                    // thread outliving its job meets a closed user classloader.
                    LOG.warn(
                            "The Pub/Sub publisher for topic {} failed to shut down, after its"
                                    + " close had already given up waiting for it.",
                            destination,
                            t);
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                start();
                long waitedNanos = remainingNanos();
                if (waitedNanos > 0) {
                    // Milliseconds rounded up, so a sub-millisecond remainder never reaches
                    // join(0) — which waits forever, the very thing this class exists to bound.
                    thread.join(1 + TimeUnit.NANOSECONDS.toMillis(waitedNanos));
                }
                if (thread.isAlive()) {
                    abandoned = true;
                    // The waited time, not the configured budget: it is shared across every
                    // publisher the writer owns, so a publisher after one that hung gets none of
                    // it and would otherwise report "did not finish within 30s" having waited
                    // nothing — which reads as "raise the timeout" when the answer is elsewhere.
                    // For the same reason this is not attributed to #265: a healthy teardown that
                    // an earlier one left no time for reaches the same branch.
                    LOG.warn(
                            "The Pub/Sub publisher for topic {} did not finish shutting down within"
                                    + " the {} of its {} shutdown budget that were left; it is left to a"
                                    + " background thread, and its resources leak until the JVM exits."
                                    + " A shutdown that never returns at all is the SDK defect tracked"
                                    + " as issue"
                                    + " https://github.com/laughingman7743/flink-connector-gcp/issues/265.",
                            destination,
                            Duration.ofNanos(waitedNanos),
                            timeout);
                    return;
                }
                Throwable captured = failure;
                if (captured != null) {
                    // Rethrown with its own type: Closers.closeAll relies on that, and a wrapped
                    // Error is a different thing to Flink's Task.preProcessException.
                    ExceptionUtils.rethrowException(captured);
                }
            } catch (InterruptedException e) {
                // Restored before it propagates: Closers.closeAll collects and carries on, and the
                // join cleared the flag, so without this the rest of the writer's teardown — the
                // other publishers, the admin, the failure handler — would stop honouring the
                // cancellation that interrupted us.
                Thread.currentThread().interrupt();
                throw e;
            } finally {
                if (ownedChannel != null) {
                    ownedChannel.shutdownNow();
                }
            }
        }
    }
}
