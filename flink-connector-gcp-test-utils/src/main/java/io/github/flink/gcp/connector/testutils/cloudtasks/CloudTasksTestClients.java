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

package io.github.flink.gcp.connector.testutils.cloudtasks;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

/**
 * A stock {@link CloudTasksClient} over one plaintext channel to the emulator, with no credentials,
 * for queue administration and task inspection.
 *
 * <p>Stock and deliberately <em>unshaded</em>: in the SQL module's smoke test this runs beside the
 * uber-jar's relocated copy, so that the two demonstrably coexist on one classpath — the same
 * property {@code testutils.bigtable.BigtableTestClients} exists to show.
 *
 * <p>The instance owns the channel: {@link #close()} closes the client and shuts the channel down,
 * and a construction that fails half-way shuts it down too, so a caller's teardown only ever sees a
 * fully-constructed instance.
 */
@Internal
public final class CloudTasksTestClients implements AutoCloseable {

    private final ManagedChannel channel;
    private final CloudTasksClient client;

    private CloudTasksTestClients(ManagedChannel channel, CloudTasksClient client) {
        this.channel = channel;
        this.client = client;
    }

    /** Opens a client against the emulator at {@code endpoint} ({@code host:port}). */
    public static CloudTasksTestClients forEmulator(String endpoint) throws IOException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
        try {
            CloudTasksClient client =
                    CloudTasksClient.create(
                            CloudTasksSettings.newBuilder()
                                    .setTransportChannelProvider(
                                            FixedTransportChannelProvider.create(
                                                    GrpcTransportChannel.create(channel)))
                                    .setCredentialsProvider(NoCredentialsProvider.create())
                                    .build());
            return new CloudTasksTestClients(channel, client);
        } catch (IOException | RuntimeException e) {
            channel.shutdownNow();
            throw e;
        }
    }

    public CloudTasksClient client() {
        return client;
    }

    @Override
    public void close() {
        try {
            client.close();
        } finally {
            channel.shutdownNow();
        }
    }
}
