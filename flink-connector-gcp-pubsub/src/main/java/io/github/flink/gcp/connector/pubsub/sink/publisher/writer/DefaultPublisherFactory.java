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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
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

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

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
    @Nullable private final String emulatorEndpoint;

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
     * @param emulatorEndpoint the emulator endpoint as {@code host:port} (plaintext, no
     *     credentials), or {@code null} for production Pub/Sub
     */
    public DefaultPublisherFactory(
            PubSubPublisherOptions options, @Nullable String emulatorEndpoint) {
        this.options = options;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public TopicPublisher create(TopicDestination destination) throws IOException {
        Publisher.Builder builder = Publisher.newBuilder(destination.toTopicPath());
        ManagedChannel ownedChannel = null;
        if (emulatorEndpoint != null) {
            ownedChannel = ManagedChannelBuilder.forTarget(emulatorEndpoint).usePlaintext().build();
            builder.setChannelProvider(
                            FixedTransportChannelProvider.create(
                                    GrpcTransportChannel.create(ownedChannel)))
                    .setCredentialsProvider(NoCredentialsProvider.create());
        }
        configure(builder, options);
        return new PublisherAdapter(builder.build(), destination, ownedChannel);
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
     * Builds the SDK batching settings: the SDK defaults overlaid with the set thresholds, and —
     * when a flow-control limit is set — a blocking flow controller (blocking the task thread is
     * plain backpressure; failing the job or ignoring the limit are not useful sink behaviors).
     */
    @VisibleForTesting
    static BatchingSettings batchingSettings(PubSubPublisherOptions options) {
        BatchingSettings defaults = Publisher.Builder.getDefaultBatchingSettings();
        long elementCountThreshold =
                options.getBatchElementCountThreshold() != null
                        ? options.getBatchElementCountThreshold()
                        : defaults.getElementCountThreshold();
        long requestByteThreshold =
                options.getBatchRequestByteThreshold() != null
                        ? options.getBatchRequestByteThreshold()
                        : defaults.getRequestByteThreshold();
        BatchingSettings.Builder batching = defaults.toBuilder();
        if (options.getBatchDelayThreshold() != null) {
            batching.setDelayThresholdDuration(options.getBatchDelayThreshold());
        }
        if (options.getFlowControlMaxOutstandingElementCount() != null
                || options.getFlowControlMaxOutstandingRequestBytes() != null) {
            // The SDK publisher requires both limits when flow control is enforced; an unset
            // limit becomes effectively unlimited. The publisher does not cap its batch
            // thresholds to the limits, so a batch that could never fill under them would stall
            // publishing until the delay alarm while holding permits — cap the thresholds here.
            long elementLimit = orUnlimited(options.getFlowControlMaxOutstandingElementCount());
            long byteLimit = orUnlimited(options.getFlowControlMaxOutstandingRequestBytes());
            elementCountThreshold = Math.min(elementCountThreshold, elementLimit);
            requestByteThreshold = Math.min(requestByteThreshold, byteLimit);
            batching.setFlowControlSettings(
                    FlowControlSettings.newBuilder()
                            .setMaxOutstandingElementCount(elementLimit)
                            .setMaxOutstandingRequestBytes(byteLimit)
                            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                            .build());
        }
        return batching.setElementCountThreshold(elementCountThreshold)
                .setRequestByteThreshold(requestByteThreshold)
                .build();
    }

    private static long orUnlimited(@Nullable Long limit) {
        return limit != null ? limit : Long.MAX_VALUE;
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
    private static final class PublisherAdapter implements TopicPublisher {

        private final Publisher publisher;
        private final TopicDestination destination;
        @Nullable private final ManagedChannel ownedChannel;

        private PublisherAdapter(
                Publisher publisher,
                TopicDestination destination,
                @Nullable ManagedChannel ownedChannel) {
            this.publisher = publisher;
            this.destination = destination;
            this.ownedChannel = ownedChannel;
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
        public void close() throws Exception {
            try {
                publisher.shutdown();
                if (!publisher.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "The Pub/Sub publisher for topic {} did not terminate within {}"
                                    + " seconds; its resources may leak until the JVM exits.",
                            destination,
                            SHUTDOWN_TIMEOUT_SECONDS);
                }
            } finally {
                if (ownedChannel != null) {
                    ownedChannel.shutdownNow();
                }
            }
        }
    }
}
