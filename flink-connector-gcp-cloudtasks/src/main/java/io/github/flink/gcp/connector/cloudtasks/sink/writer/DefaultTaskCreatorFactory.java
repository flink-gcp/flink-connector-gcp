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
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Creates a {@link TaskCreator} backed by a {@code google-cloud-tasks} {@link CloudTasksClient},
 * connecting either to production Cloud Tasks with application-default or configured
 * service-account credentials, or to an emulator over a plaintext channel with no credentials.
 *
 * <p>The client's own retry configuration is left alone: it retries {@code CreateTask} on nothing
 * at all, which is the reason the writer owns retrying (see {@code CloudTasksWriterOptions}).
 * Transport sizing is the one gax setting this factory does touch: a configured {@code
 * channelPoolSize} replaces the production transport provider with one carrying that many channels,
 * because the default single channel delivers far less concurrency than the writer's in-flight cap
 * (ADR-0134). The emulator arm keeps its single caller-owned channel (ADR-0081), so the constructor
 * rejects a pool beside an emulator endpoint — the sink builder and the table factory already
 * refuse the combination, and this keeps the impossible state unrepresentable here too.
 */
@Internal
public class DefaultTaskCreatorFactory implements TaskCreatorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskCreatorFactory.class);

    private static final long serialVersionUID = 1L;

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

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
    public TaskCreator create() throws IOException {
        ManagedChannel ownedChannel = null;
        try {
            CloudTasksSettings.Builder settings;
            if (emulatorEndpoint != null) {
                settings = CloudTasksSettings.newBuilder();
                ownedChannel = EmulatorChannels.openPlaintextChannel(emulatorEndpoint);
                settings.setTransportChannelProvider(EmulatorChannels.fixedProvider(ownedChannel))
                        .setCredentialsProvider(NoCredentialsProvider.create());
            } else {
                settings = productionSettings(serviceAccountKeyFile, channelPoolSize);
            }
            return new CloudTasksClientAdapter(
                    CloudTasksClient.create(settings.build()), ownedChannel);
        } catch (IOException | RuntimeException e) {
            // The channel is owned here until the adapter takes it over on success.
            if (ownedChannel != null) {
                ownedChannel.shutdownNow();
            }
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

    /** Adapts the SDK {@link CloudTasksClient} to the writer-facing {@link TaskCreator}. */
    private static final class CloudTasksClientAdapter implements TaskCreator {

        private final CloudTasksClient client;
        private final UnaryCallable<CreateTaskRequest, Task> createTaskCallable;
        @Nullable private final ManagedChannel ownedChannel;

        private CloudTasksClientAdapter(
                CloudTasksClient client, @Nullable ManagedChannel ownedChannel) {
            this.client = client;
            this.createTaskCallable = client.createTaskCallable();
            this.ownedChannel = ownedChannel;
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request) {
            return createTaskCallable.futureCall(request);
        }

        @Override
        public void close() throws Exception {
            try {
                client.shutdown();
                if (!client.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "The Cloud Tasks client did not terminate within {} seconds; its"
                                    + " resources may leak until the JVM exits.",
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
