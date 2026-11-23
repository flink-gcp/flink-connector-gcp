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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.api.gax.tracing.OpenTelemetryMetricsFactory;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.grpc.CallOptions;
import io.grpc.Deadline;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Creates a {@link TaskCreator} backed by a {@code google-cloud-tasks} {@link CloudTasksClient},
 * connecting either to production Cloud Tasks with application-default or configured
 * service-account credentials, or to an emulator over a plaintext channel with no credentials.
 *
 * <p>The client's own retry configuration is left alone: it retries {@code CreateTask} on nothing
 * at all, which is the reason the writer owns retrying (see {@code CloudTasksWriterOptions}).
 * Transport sizing is one of the two gax settings this factory does touch: a configured {@code
 * channelPoolSize} replaces the production transport provider with one carrying that many channels,
 * because the default single channel delivers far less concurrency than the writer's in-flight cap
 * (ADR-0134). The emulator arm keeps its single caller-owned channel (ADR-0081), so the constructor
 * rejects a pool beside an emulator endpoint — the sink builder and the table factory already
 * refuse the combination, and this keeps the impossible state unrepresentable here too.
 *
 * <p>The other is the tracer factory, set only when the caller passes client metrics: gax's {@code
 * OpenTelemetryMetricsFactory} over their {@code OpenTelemetry}, on the emulator arm as on the
 * production one, since gax records each call whatever the endpoint. The Cloud Tasks client has no
 * metrics of its own, so this is the only source of its RPC timings. {@code setTracerFactory} is
 * {@code @BetaApi} in the pinned gax, and gax creates the factory's histogram only when the client
 * declares its library metadata, which {@code google-cloud-tasks} 2.99.0 does; reread both on a BOM
 * bump (ADR-0186).
 *
 * <p>Checkpointed creation also uses this factory for a production-only v2beta3 queue-retention
 * readback with the same credential selection. That client surface is beta in the pinned SDK;
 * retain its published retention semantics when updating the dependency (ADR-0158).
 *
 * <p>Every channel runs on the connector's gRPC event-loop group ({@link GrpcEventLoops},
 * ADR-0208), not on gRPC's own, whose delayed destroy can run after Flink closes the job's class
 * loader. A production client holds a lease applied to its settings. The emulator arm builds its
 * channel itself from the endpoint's gRPC target, so a resolver scheme such as {@code dns:///}
 * survives and no client-certificate setting turns it into TLS; that channel holds a lease of its
 * own and closes after the client. Each close releases its lease within {@link
 * GrpcEventLoops.LeasedClient}'s bounds.
 */
@Internal
public class DefaultTaskCreatorFactory implements TaskCreatorFactory {

    private static final long serialVersionUID = 1L;

    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final Integer channelPoolSize;

    /**
     * Creates the factory.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Cloud Tasks
     * @param channelPoolSize the production transport's gRPC channel count, or {@code null} to
     *     leave the client's default transport alone; rejected beside an emulator endpoint
     */
    public DefaultTaskCreatorFactory(
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Integer channelPoolSize) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || channelPoolSize == null,
                "channelPoolSize cannot be combined with an emulator endpoint: the emulator uses"
                        + " one plaintext channel.");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.channelPoolSize = channelPoolSize;
    }

    /** The configured key-file path; the test seam pinning the sink's wiring. */
    @Nullable
    String serviceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /** The configured emulator endpoint; the test seam pinning the sink's wiring. */
    @Nullable
    EmulatorEndpoint emulatorEndpoint() {
        return emulatorEndpoint;
    }

    /** The configured channel-pool size; the test seam pinning the sink's wiring. */
    @Nullable
    Integer channelPoolSize() {
        return channelPoolSize;
    }

    @Override
    public TaskCreator create(@Nullable ClientMetrics clientMetrics) throws IOException {
        GrpcEventLoops.LeasedClient<CloudTasksClient> client = null;
        try {
            if (emulatorEndpoint == null) {
                CloudTasksSettings.Builder settings =
                        productionSettings(serviceAccountKeyFile, channelPoolSize);
                applyClientMetrics(settings, clientMetrics);
                client =
                        GrpcEventLoops.open(
                                lease -> CloudTasksClient.create(lease.applyTo(settings).build()));
            } else {
                client =
                        EmulatorChannels.openLeasedClient(
                                CloudTasksSettings.defaultGrpcTransportProviderBuilder(),
                                emulatorEndpoint,
                                channel -> {
                                    CloudTasksSettings.Builder settings =
                                            CloudTasksSettings.newBuilder()
                                                    .setTransportChannelProvider(channel)
                                                    .setCredentialsProvider(
                                                            NoCredentialsProvider.create());
                                    applyClientMetrics(settings, clientMetrics);
                                    return CloudTasksClient.create(settings.build());
                                });
            }
            return new CloudTasksClientAdapter(client, clientMetrics);
        } catch (IOException | RuntimeException | Error e) {
            // Owned here until the adapter takes it over on success; which failure is reported is
            // closeAllSuppressing's contract.
            Closers.closeAllSuppressing(e, client);
            throw e;
        }
    }

    static CloudTasksSettings.Builder productionSettings(
            @Nullable String serviceAccountKeyFile, @Nullable Integer channelPoolSize)
            throws IOException {
        CloudTasksSettings.Builder settings = CloudTasksSettings.newBuilder();
        CredentialsProvider credentials = CloudTasksCredentials.load(serviceAccountKeyFile);
        if (credentials != null) {
            settings.setCredentialsProvider(credentials);
        }
        if (channelPoolSize != null) {
            // The service's default transport provider builder, resized. Starting from it keeps
            // the provider-level default a bare builder would lose — the unbounded inbound message
            // size — while the endpoint and credentials are applied by the client context
            // regardless of which builder produced the provider.
            settings.setTransportChannelProvider(
                    CloudTasksSettings.defaultGrpcTransportProviderBuilder()
                            .setChannelPoolSettings(
                                    ChannelPoolSettings.staticallySized(channelPoolSize))
                            .build());
        }
        return settings;
    }

    /**
     * Makes the client record into the given client metrics, through gax's public OpenTelemetry
     * metrics factory; nothing is changed when they are {@code null}. Separate so that a test can
     * see the factory reach the settings on the production arm, which no emulator job exercises.
     *
     * @param settings the settings to apply it to
     * @param clientMetrics the client metrics to record into, or {@code null}
     */
    static void applyClientMetrics(
            CloudTasksSettings.Builder settings, @Nullable ClientMetrics clientMetrics) {
        if (clientMetrics != null) {
            settings.getStubSettingsBuilder()
                    .setTracerFactory(
                            new OpenTelemetryMetricsFactory(clientMetrics.openTelemetry()));
        }
    }

    /**
     * Reads queue retention using v2beta3 and the creation client's credentials.
     *
     * @param queuePath the pre-provisioned queue
     * @return its published retention, using the one-hour default for an absent field
     * @throws IOException if queue readback fails
     */
    public java.time.Duration readQueueRetention(String queuePath) throws IOException {
        Preconditions.checkState(
                emulatorEndpoint == null,
                "Queue retention readback is unavailable for an emulator endpoint.");
        try (var client = openRetentionClient(serviceAccountKeyFile)) {
            return queueRetention(client.client().getQueue(queuePath));
        } catch (RuntimeException e) {
            throw retentionReadFailure(e);
        }
    }

    /** Opens the v2beta3 client the retention readback uses, on the event-loop group. */
    static GrpcEventLoops.LeasedClient<com.google.cloud.tasks.v2beta3.CloudTasksClient>
            openRetentionClient(@Nullable String serviceAccountKeyFile) throws IOException {
        var settings = retentionSettings(serviceAccountKeyFile);
        return GrpcEventLoops.open(
                lease ->
                        com.google.cloud.tasks.v2beta3.CloudTasksClient.create(
                                lease.applyTo(settings).build()));
    }

    static IOException retentionReadFailure(RuntimeException failure) {
        var code = CloudTasksErrorClassifier.statusCode(failure);
        return new IOException(
                "Cloud Tasks queue retention readback failed: status="
                        + (code == null ? "UNCLASSIFIED" : code)
                        + "; verify cloudtasks.queues.get, queue configuration and service availability.");
    }

    static com.google.cloud.tasks.v2beta3.CloudTasksSettings.Builder retentionSettings(
            @Nullable String serviceAccountKeyFile) throws IOException {
        var settings = com.google.cloud.tasks.v2beta3.CloudTasksSettings.newBuilder();
        CredentialsProvider credentials = CloudTasksCredentials.load(serviceAccountKeyFile);
        if (credentials != null) {
            settings.setCredentialsProvider(credentials);
        }
        return settings;
    }

    static java.time.Duration queueRetention(com.google.cloud.tasks.v2beta3.Queue queue)
            throws IOException {
        if (!queue.hasTombstoneTtl()) {
            return java.time.Duration.ofHours(1);
        }
        var ttl = queue.getTombstoneTtl();
        if (ttl.getSeconds() < 0 || ttl.getNanos() < 0 || ttl.getNanos() >= 1_000_000_000) {
            throw new IOException("Cloud Tasks queue returned an invalid tombstoneTtl.");
        }
        return java.time.Duration.ofSeconds(ttl.getSeconds(), ttl.getNanos());
    }

    /**
     * Adapts the SDK {@link CloudTasksClient} to the writer-facing {@link TaskCreator}. It closes
     * the client, which releases its event-loop lease and, on the emulator arm, closes the
     * caller-owned channel after it, and the client metrics it was given last: closed client
     * metrics drop what they are given, so a call still completing would lose its recording.
     */
    static final class CloudTasksClientAdapter implements TaskCreator {

        private final GrpcEventLoops.LeasedClient<CloudTasksClient> client;
        private final UnaryCallable<CreateTaskRequest, Task> createTaskCallable;
        @Nullable private final ClientMetrics clientMetrics;

        CloudTasksClientAdapter(
                GrpcEventLoops.LeasedClient<CloudTasksClient> client,
                @Nullable ClientMetrics clientMetrics) {
            this.client = client;
            this.createTaskCallable = client.client().createTaskCallable();
            this.clientMetrics = clientMetrics;
        }

        /** The adapted client; the test seam reading the settings the factory built. */
        CloudTasksClient client() {
            return client.client();
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request) {
            return createTaskCallable.futureCall(request);
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
            return createTaskCallable.futureCall(
                    request,
                    GrpcCallContext.createDefault()
                            .withCallOptions(CallOptions.DEFAULT.withDeadline(deadline)));
        }

        @Override
        public void close() throws Exception {
            Closers.closeAll(client, clientMetrics);
        }
    }
}
