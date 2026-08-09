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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
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
 * connecting either to production Cloud Tasks with application-default credentials or to an
 * emulator over a plaintext channel with no credentials.
 *
 * <p>The client's own retry configuration is left alone: it retries {@code CreateTask} on nothing
 * at all, which is the reason the writer owns retrying (see {@code CloudTasksWriterOptions}).
 */
@Internal
public class DefaultTaskCreatorFactory implements TaskCreatorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskCreatorFactory.class);

    private static final long serialVersionUID = 1L;

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates a factory connecting to production Cloud Tasks with application-default credentials.
     */
    public DefaultTaskCreatorFactory() {
        this(null);
    }

    /**
     * Creates the factory.
     *
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Cloud Tasks
     */
    public DefaultTaskCreatorFactory(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public TaskCreator create() throws IOException {
        ManagedChannel ownedChannel = null;
        try {
            CloudTasksSettings.Builder settings = CloudTasksSettings.newBuilder();
            if (emulatorEndpoint != null) {
                ownedChannel = EmulatorChannels.openPlaintextChannel(emulatorEndpoint);
                settings.setTransportChannelProvider(EmulatorChannels.fixedProvider(ownedChannel))
                        .setCredentialsProvider(NoCredentialsProvider.create());
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
