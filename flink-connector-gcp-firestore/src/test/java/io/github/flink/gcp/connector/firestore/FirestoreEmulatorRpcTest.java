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

package io.github.flink.gcp.connector.firestore;

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.cloud.firestore.v1.stub.FirestoreStubSettings;
import com.google.cloud.grpc.GrpcTransportOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link FirestoreEmulatorRpc}'s retry settings and executor release (#2343). */
class FirestoreEmulatorRpcTest {

    /**
     * Under the default retry settings, five attempts for every unary method and the four
     * server-streaming reads, as {@code GrpcFirestoreRpc} sets them (google-cloud-firestore
     * 3.53.0).
     */
    @Test
    void theDefaultRetrySettingsGiveFiveAttempts() {
        FirestoreStubSettings.Builder settings = FirestoreStubSettings.newBuilder();

        FirestoreEmulatorRpc.applyRetrySettings(settings, ServiceOptions.getDefaultRetrySettings());

        List<Integer> attempts = new ArrayList<>();
        settings.applyToAllUnaryMethods(
                method -> {
                    attempts.add(method.getRetrySettings().getMaxAttempts());
                    return null;
                });
        attempts.add(settings.runQuerySettings().getRetrySettings().getMaxAttempts());
        attempts.add(settings.runAggregationQuerySettings().getRetrySettings().getMaxAttempts());
        attempts.add(settings.batchGetDocumentsSettings().getRetrySettings().getMaxAttempts());
        attempts.add(settings.executePipelineSettings().getRetrySettings().getMaxAttempts());
        // GrpcFirestoreRpc's literal, not this class's constant.
        assertThat(attempts).isNotEmpty().containsOnly(5);
    }

    /**
     * The four server-streaming reads {@code GrpcFirestoreRpc} gives the retry settings are the
     * stub's only server-streaming methods; a library version that adds one fails here, so that its
     * retry settings are checked against the library's before they ship.
     */
    @Test
    void theStubsServerStreamingMethodsAreTheFourItRetries() {
        List<String> streaming = new ArrayList<>();
        for (Method method : FirestoreStubSettings.Builder.class.getMethods()) {
            if (method.getParameterCount() == 0
                    && method.getReturnType() == ServerStreamingCallSettings.Builder.class) {
                streaming.add(method.getName());
            }
        }

        assertThat(streaming)
                .containsExactlyInAnyOrder(
                        "runQuerySettings",
                        "runAggregationQuerySettings",
                        "batchGetDocumentsSettings",
                        "executePipelineSettings");
    }

    @Test
    void otherRetrySettingsApplyToEveryMethod() {
        RetrySettings custom =
                ServiceOptions.getDefaultRetrySettings().toBuilder()
                        .setMaxAttempts(2)
                        .setTotalTimeoutDuration(Duration.ofSeconds(7))
                        .build();
        FirestoreStubSettings.Builder settings = FirestoreStubSettings.newBuilder();

        FirestoreEmulatorRpc.applyRetrySettings(settings, custom);

        List<RetrySettings> applied = new ArrayList<>();
        settings.applyToAllUnaryMethods(
                method -> {
                    applied.add(method.getRetrySettings());
                    return null;
                });
        applied.add(settings.runQuerySettings().getRetrySettings());
        applied.add(settings.runAggregationQuerySettings().getRetrySettings());
        applied.add(settings.batchGetDocumentsSettings().getRetrySettings());
        applied.add(settings.executePipelineSettings().getRetrySettings());
        assertThat(applied).isNotEmpty().containsOnly(custom);
    }

    /** The settings the RPC is built with carry the retry settings, not only the helper. */
    @Test
    void theStubSettingsCarryTheRetrySettings() throws Exception {
        try (GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel =
                EmulatorChannels.openLeasedPlaintextChannel(
                        FirestoreSettings.defaultGrpcTransportProviderBuilder(),
                        EmulatorEndpoint.parse("localhost:1", "e"))) {
            FirestoreStubSettings settings =
                    FirestoreEmulatorRpc.stubSettings(
                            ClientContext.newBuilder()
                                    .setTransportChannel(channel.client())
                                    .setDefaultCallContext(GrpcCallContext.createDefault())
                                    .build(),
                            ServiceOptions.getDefaultRetrySettings());

            assertThat(settings.commitSettings().getRetrySettings().getMaxAttempts()).isEqualTo(5);
            assertThat(settings.runQuerySettings().getRetrySettings().getMaxAttempts())
                    .isEqualTo(5);
        }
    }

    /**
     * The RPC built for a client hands the client the shared executor and gives it back once when
     * it shuts down, through the background resources its stub closes.
     */
    @Test
    void anRpcGivesItsSharedExecutorBackWhenItShutsDown() throws Exception {
        List<ScheduledExecutorService> given = new ArrayList<>();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:1", "e");
        try (GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel =
                EmulatorChannels.openLeasedPlaintextChannel(
                        FirestoreSettings.defaultGrpcTransportProviderBuilder(), endpoint)) {
            FirestoreOptions options =
                    FirestoreOptions.newBuilder()
                            .setProjectId("p")
                            .setCredentials(new FirestoreOptions.EmulatorCredentials())
                            .setEmulatorHost("localhost:1")
                            .setTransportOptions(
                                    GrpcTransportOptions.newBuilder()
                                            .setExecutorFactory(
                                                    new GrpcTransportOptions.ExecutorFactory<
                                                            ScheduledExecutorService>() {
                                                        @Override
                                                        public ScheduledExecutorService get() {
                                                            return executor;
                                                        }

                                                        @Override
                                                        public void release(
                                                                ScheduledExecutorService released) {
                                                            given.add(released);
                                                        }
                                                    })
                                            .build())
                            .build();

            FirestoreRpc rpc = FirestoreEmulatorRpc.create(options, channel.client());
            assertThat(rpc.getExecutor()).isSameAs(executor);
            rpc.shutdownNow();
            rpc.close();

            assertThat(given).containsExactly(executor);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void theSharedExecutorIsGivenBackOnceAndTheRetryExecutorStops() throws Exception {
        List<ScheduledExecutorService> given = new ArrayList<>();
        ScheduledExecutorService shared = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
        try {
            FirestoreEmulatorRpc.ClientExecutors executors =
                    new FirestoreEmulatorRpc.ClientExecutors(
                            new GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService>() {
                                @Override
                                public ScheduledExecutorService get() {
                                    return shared;
                                }

                                @Override
                                public void release(ScheduledExecutorService released) {
                                    given.add(released);
                                }
                            },
                            shared,
                            background);

            assertThat(executors.isShutdown()).isFalse();
            executors.shutdownNow();
            executors.shutdown();
            executors.close();

            assertThat(given).containsExactly(shared);
            assertThat(background.isShutdown()).as("the retry executor stopped").isTrue();
            assertThat(executors.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(executors.isTerminated()).isTrue();
        } finally {
            shared.shutdownNow();
            background.shutdownNow();
        }
    }

    /** The shared executor is given back even when stopping the retry executor fails. */
    @Test
    void theSharedExecutorIsGivenBackWhenTheRetryExecutorFailsToStop() {
        List<ScheduledExecutorService> given = new ArrayList<>();
        ScheduledExecutorService shared = Executors.newSingleThreadScheduledExecutor();
        ScheduledThreadPoolExecutor refusing =
                new ScheduledThreadPoolExecutor(1) {
                    @Override
                    public List<Runnable> shutdownNow() {
                        throw new SecurityException("modifyThread denied");
                    }
                };
        try {
            FirestoreEmulatorRpc.ClientExecutors executors =
                    new FirestoreEmulatorRpc.ClientExecutors(
                            new GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService>() {
                                @Override
                                public ScheduledExecutorService get() {
                                    return shared;
                                }

                                @Override
                                public void release(ScheduledExecutorService released) {
                                    given.add(released);
                                }
                            },
                            shared,
                            refusing);

            assertThatThrownBy(executors::shutdownNow)
                    .isInstanceOf(SecurityException.class)
                    .hasMessage("modifyThread denied");
            assertThat(given).containsExactly(shared);
        } finally {
            shared.shutdownNow();
            refusing.shutdown();
        }
    }
}
