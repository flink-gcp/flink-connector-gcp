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

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the settings mapping of {@link DefaultPublisherFactory}. */
class DefaultPublisherFactoryTest {

    private static final BatchingSettings SDK_BATCHING_DEFAULTS =
            Publisher.Builder.getDefaultBatchingSettings();

    @Test
    void batchingSettingsOverlayOnlySetThresholdsOverSdkDefaults() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder().batchElementCountThreshold(5).build();

        BatchingSettings batching = DefaultPublisherFactory.batchingSettings(options);

        assertThat(batching.getElementCountThreshold()).isEqualTo(5);
        assertThat(batching.getRequestByteThreshold())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getRequestByteThreshold());
        assertThat(batching.getDelayThresholdDuration())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getDelayThresholdDuration());
        assertThat(batching.getFlowControlSettings())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getFlowControlSettings());
    }

    @Test
    void batchingSettingsApplyByteAndDelayThresholds() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchRequestByteThreshold(2_048)
                        .batchDelayThreshold(Duration.ofMillis(20))
                        .build();

        BatchingSettings batching = DefaultPublisherFactory.batchingSettings(options);

        assertThat(batching.getRequestByteThreshold()).isEqualTo(2_048);
        assertThat(batching.getDelayThresholdDuration()).isEqualTo(Duration.ofMillis(20));
        assertThat(batching.getElementCountThreshold())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getElementCountThreshold());
    }

    @Test
    void batchingSettingsNeverEnableTheSdkFlowController() {
        // In-flight publishes are bounded by the writer, not by the SDK: it blocks the task thread
        // instead of yielding to the mailbox, and it leaks a permit per publish cancelled on a
        // paused ordering key. Leaving the settings at the SDK default of Ignore is what keeps
        // Publisher from constructing a controller at all.
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchRequestByteThreshold(4_096)
                        .maxInFlightBytes(1_024)
                        .build();

        BatchingSettings batching = DefaultPublisherFactory.batchingSettings(options);

        assertThat(batching.getFlowControlSettings().getLimitExceededBehavior())
                .isEqualTo(FlowController.LimitExceededBehavior.Ignore);
        // The writer cap is not an SDK knob, so it does not shrink the batch thresholds.
        assertThat(batching.getRequestByteThreshold()).isEqualTo(4_096);
    }

    @Test
    void retrySettingsOverlayOnlySetKnobsOverMirroredSdkDefaults() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .retryInitialDelay(Duration.ofSeconds(1))
                        .retryMaxAttempts(7)
                        .build();

        RetrySettings retry = DefaultPublisherFactory.retrySettings(options);

        assertThat(retry.getInitialRetryDelayDuration()).isEqualTo(Duration.ofSeconds(1));
        assertThat(retry.getMaxAttempts()).isEqualTo(7);
        assertThat(retry.getTotalTimeoutDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getTotalTimeoutDuration());
        assertThat(retry.getRetryDelayMultiplier())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getRetryDelayMultiplier());
        assertThat(retry.getMaxRetryDelayDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getMaxRetryDelayDuration());
        assertThat(retry.getInitialRpcTimeoutDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS
                                .getInitialRpcTimeoutDuration());
        assertThat(retry.getRpcTimeoutMultiplier())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getRpcTimeoutMultiplier());
        assertThat(retry.getMaxRpcTimeoutDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getMaxRpcTimeoutDuration());
    }

    /**
     * Pins the mirrored retry defaults to the SDK's own (package-private) {@code
     * Publisher.Builder.DEFAULT_RETRY_SETTINGS}, so an SDK upgrade changing publisher retry
     * defaults fails loudly here instead of silently diverging.
     */
    @Test
    void mirroredRetryDefaultsMatchTheSdk() throws Exception {
        Field defaults = Publisher.Builder.class.getDeclaredField("DEFAULT_RETRY_SETTINGS");
        defaults.setAccessible(true);
        RetrySettings sdkDefaults = (RetrySettings) defaults.get(null);

        assertThat(DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS).isEqualTo(sdkDefaults);
    }

    /**
     * Wires the mapping into a real SDK publisher (a lazy plaintext channel to an unused port —
     * gRPC connects on first use, so build() succeeds offline) and asserts the settings took effect
     * on the built instance. (Ordering wiring is covered behaviorally by the emulator ITs, which
     * reuse {@code configure}; the SDK rejects ordered publishes unless the flag took effect.)
     */
    @Test
    void configureAppliesSettingsToABuiltPublisher() throws Exception {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchElementCountThreshold(5)
                        .batchDelayThreshold(Duration.ofMillis(20))
                        .build();
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("localhost:1").usePlaintext().build();
        Publisher publisher = null;
        try {
            Publisher.Builder builder =
                    Publisher.newBuilder("projects/test-project/topics/test-topic")
                            .setChannelProvider(
                                    FixedTransportChannelProvider.create(
                                            GrpcTransportChannel.create(channel)))
                            .setCredentialsProvider(NoCredentialsProvider.create());
            DefaultPublisherFactory.configure(builder, options);
            publisher = builder.build();

            assertThat(publisher.getBatchingSettings().getElementCountThreshold()).isEqualTo(5);
            assertThat(publisher.getBatchingSettings().getDelayThresholdDuration())
                    .isEqualTo(Duration.ofMillis(20));
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
            channel.shutdownNow();
        }
    }

    @Test
    void emulatorEndpointBuildsAndClosesPublisherOffline() throws Exception {
        // gRPC channels connect lazily, so building against an unreachable endpoint is safe
        // offline; the behavioral emulator coverage lives in the *ITCase classes, which all
        // publish through this factory's emulator mode.
        DefaultPublisherFactory factory =
                new DefaultPublisherFactory(PubSubPublisherOptions.defaults(), "localhost:1");
        TopicPublisher publisher = factory.create(TopicDestination.of("test-project", "t"));
        publisher.close();
    }
}
