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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ClientContext;
import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.stub.FirestoreStubSettings;
import com.google.cloud.firestore.v1.stub.GrpcFirestoreStub;
import com.google.cloud.grpc.GrpcTransportOptions;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.grpc.CallOptions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The RPC a Firestore Native client sends every call through when it reaches an emulator: the
 * generated stub on a plaintext channel the caller owns, on the connector's gRPC event-loop group
 * (ADR-0208, #2343).
 *
 * <p>{@code FirestoreOptions} takes only an instantiating channel provider, which gax builds the
 * channel from: it parses the endpoint as {@code host:port}, so a {@code dns:///} target fails, and
 * builds TLS credentials when a client certificate is configured. The library's own emulator
 * setting builds a provider of its own, off the group. A client built with this RPC, through {@code
 * setServiceRpcFactory}, ignores whatever provider the options carry and dials the caller's channel
 * instead.
 *
 * <p>Its client context is built by hand, as the library's {@code GrpcFirestoreRpc} builds it for a
 * {@code localhost} host or {@code NoCredentials} (google-cloud-firestore 3.53.0), not through
 * {@code ClientContext.create}, which the library used for every client this connector opened
 * before, an emulator one included. So the calls carry none of the {@code
 * google-cloud-resource-prefix}, {@code x-goog-api-client} and {@code x-goog-request-params}
 * headers, nor any header the options add, and no stream watchdog runs, as none is needed without a
 * stream idle timeout. The emulator routes each request by the database the request itself names
 * ({@code FirestoreEmulatorRoutingITCase} reads back through the library a document written to a
 * named database). Each call carries the emulator's fixed owner token ({@link
 * FirestoreOptions.EmulatorCredentials}), which the connector's emulator clients always sent. The
 * retry settings are the library's ({@link #applyRetrySettings(FirestoreStubSettings.Builder,
 * RetrySettings)}), and no API tracer is installed. Its executors are described on {@link
 * #create(FirestoreOptions, GrpcTransportChannel)}; the channel is left to its caller.
 */
@Internal
final class FirestoreEmulatorRpc extends GrpcFirestoreStub implements FirestoreRpc {

    /** The attempts the library gives every call under its default retry settings. */
    @VisibleForTesting static final int DEFAULT_MAX_ATTEMPTS = 5;

    private final ScheduledExecutorService executor;

    private FirestoreEmulatorRpc(
            FirestoreStubSettings settings,
            ClientContext context,
            ScheduledExecutorService executor)
            throws IOException {
        super(settings, context);
        this.executor = executor;
    }

    /**
     * Builds the RPC for a client of the given options on the caller's channel.
     *
     * <p>Two executors serve it, as they serve the library's RPC: the one the transport options'
     * factory hands out, shared and given back once, which {@link #getExecutor()} returns to the
     * client; and one of its own for gax's retries and timeouts, which {@code shutdownNow()} stops,
     * so a retry scheduled before a close never runs after it.
     *
     * @param options the client's options, whose transport options supply the shared executor and
     *     whose retry settings the calls take
     * @param channel the plaintext channel to the emulator, which the caller closes after the
     *     client
     * @return the RPC
     * @throws IOException if the stub cannot be built; both executors have been given back
     */
    static FirestoreRpc create(FirestoreOptions options, GrpcTransportChannel channel)
            throws IOException {
        GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> executors =
                ((GrpcTransportOptions) options.getTransportOptions()).getExecutorFactory();
        ScheduledExecutorService shared = executors.get();
        ClientExecutors clientExecutors = null;
        try {
            ScheduledExecutorService background =
                    FirestoreStubSettings.defaultExecutorProviderBuilder().build().getExecutor();
            clientExecutors = new ClientExecutors(executors, shared, background);
            Credentials credentials = new FirestoreOptions.EmulatorCredentials();
            ClientContext context =
                    ClientContext.newBuilder()
                            .setCredentials(credentials)
                            .setExecutor(background)
                            .setTransportChannel(channel)
                            .setDefaultCallContext(
                                    GrpcCallContext.of(channel.getChannel(), CallOptions.DEFAULT)
                                            .withCredentials(credentials))
                            .setBackgroundResources(List.of(clientExecutors))
                            .build();
            return new FirestoreEmulatorRpc(
                    stubSettings(context, options.getRetrySettings()), context, shared);
        } catch (IOException | RuntimeException | Error e) {
            ClientExecutors built = clientExecutors;
            Closers.closeAllSuppressing(
                    e, built != null ? built::shutdownNow : () -> executors.release(shared));
            throw e;
        }
    }

    /**
     * Returns the stub settings on the given context, with the retry settings {@code
     * GrpcFirestoreRpc} applies ({@link #applyRetrySettings(FirestoreStubSettings.Builder,
     * RetrySettings)}).
     *
     * @param context the client context
     * @param retrySettings the client's retry settings
     * @return the settings
     * @throws IOException if the settings cannot be built
     */
    @VisibleForTesting
    static FirestoreStubSettings stubSettings(ClientContext context, RetrySettings retrySettings)
            throws IOException {
        FirestoreStubSettings.Builder settings = FirestoreStubSettings.newBuilder(context);
        applyRetrySettings(settings, retrySettings);
        return settings.build();
    }

    /**
     * Applies the retry settings as {@code GrpcFirestoreRpc} does: under the default ones, five
     * attempts for every unary method and for the four server-streaming reads, as the service
     * config says; otherwise the given settings for all of them.
     *
     * @param settings the stub settings to change
     * @param retrySettings the client's retry settings
     */
    @VisibleForTesting
    static void applyRetrySettings(
            FirestoreStubSettings.Builder settings, RetrySettings retrySettings) {
        if (retrySettings.equals(ServiceOptions.getDefaultRetrySettings())) {
            settings.applyToAllUnaryMethods(
                    method -> {
                        method.retrySettings().setMaxAttempts(DEFAULT_MAX_ATTEMPTS);
                        return null;
                    });
            settings.runQuerySettings().retrySettings().setMaxAttempts(DEFAULT_MAX_ATTEMPTS);
            settings.runAggregationQuerySettings()
                    .retrySettings()
                    .setMaxAttempts(DEFAULT_MAX_ATTEMPTS);
            settings.batchGetDocumentsSettings()
                    .retrySettings()
                    .setMaxAttempts(DEFAULT_MAX_ATTEMPTS);
            settings.executePipelineSettings().retrySettings().setMaxAttempts(DEFAULT_MAX_ATTEMPTS);
        } else {
            settings.applyToAllUnaryMethods(
                    method -> {
                        method.setRetrySettings(retrySettings);
                        return null;
                    });
            settings.runQuerySettings().setRetrySettings(retrySettings);
            settings.runAggregationQuerySettings().setRetrySettings(retrySettings);
            settings.batchGetDocumentsSettings().setRetrySettings(retrySettings);
            settings.executePipelineSettings().setRetrySettings(retrySettings);
        }
    }

    @Override
    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    /**
     * The RPC's two executors as one background resource: stopping it stops the RPC's own retry
     * executor and then, whether that succeeded or not, gives the shared executor back to the
     * factory it came from, once; the factory decides whether that one stops. One resource rather
     * than two, because gax stops at the first of a client's resources that throws.
     */
    @VisibleForTesting
    static final class ClientExecutors implements BackgroundResource {

        private final GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> executors;
        private final ScheduledExecutorService shared;
        private final ScheduledExecutorService background;
        private boolean released;

        ClientExecutors(
                GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> executors,
                ScheduledExecutorService shared,
                ScheduledExecutorService background) {
            this.executors = executors;
            this.shared = shared;
            this.background = background;
        }

        private synchronized void release() {
            if (!released) {
                released = true;
                executors.release(shared);
            }
        }

        @Override
        public void shutdown() {
            stopThenRelease(background::shutdown);
        }

        @Override
        public void shutdownNow() {
            stopThenRelease(background::shutdownNow);
        }

        /**
         * Stops the retry executor and then gives the shared one back, whether the stop succeeded
         * or not; the first failure is thrown with a later one suppressed in it.
         */
        private void stopThenRelease(AutoCloseable stop) {
            try {
                Closers.closeAll(stop, this::release);
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public synchronized boolean isShutdown() {
            return released && background.isShutdown();
        }

        @Override
        public synchronized boolean isTerminated() {
            return released && background.isTerminated();
        }

        /** Waits for the retry executor; terminated only once the shared one is given back too. */
        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) throws InterruptedException {
            return background.awaitTermination(duration, unit) && isTerminated();
        }

        @Override
        public void close() {
            shutdown();
        }
    }
}
