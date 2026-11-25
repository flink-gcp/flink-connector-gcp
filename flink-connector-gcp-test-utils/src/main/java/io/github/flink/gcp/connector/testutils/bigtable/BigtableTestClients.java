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

package io.github.flink.gcp.connector.testutils.bigtable;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClientV2;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.ManagedChannelBuilder;
import org.testcontainers.containers.BigtableEmulatorContainer;

import java.io.IOException;

/**
 * Stock Bigtable clients pointed at an emulator container.
 *
 * <p>Harness-owned and deliberately <em>unshaded</em>: in the SQL module's smoke test these run
 * beside the uber-jar's relocated copies, so that the two demonstrably coexist on one classpath —
 * which is the property an uber-jar exists to provide and no jar-content assertion can show.
 */
@Internal
public final class BigtableTestClients {

    private BigtableTestClients() {}

    /**
     * Opens a table admin client against the emulator, over one plaintext channel that the client
     * owns, with no credentials.
     *
     * <p>The client takes protobuf requests that name their project and instance in full, so it is
     * not bound to either.
     *
     * @param emulator the running container
     * @return the client, which the caller closes
     * @throws IOException if the client cannot be created
     */
    public static BigtableTableAdminClientV2 adminClient(BigtableEmulatorContainer emulator)
            throws IOException {
        return BigtableTableAdminClientV2.create(
                BaseBigtableTableAdminSettings.newBuilder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .setTransportChannelProvider(
                                BaseBigtableTableAdminSettings.defaultGrpcTransportProviderBuilder()
                                        .setEndpoint(
                                                emulator.getHost()
                                                        + ":"
                                                        + emulator.getEmulatorPort())
                                        .setChannelPoolSettings(
                                                ChannelPoolSettings.staticallySized(1))
                                        .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                                        .build())
                        .build());
    }

    /**
     * Opens a data client against the emulator.
     *
     * @param emulator the running container
     * @param project the project the rows are addressed under
     * @param instance the instance the rows are addressed under
     * @return the client, which the caller closes
     * @throws IOException if the client cannot be created
     */
    public static BigtableDataClient dataClient(
            BigtableEmulatorContainer emulator, String project, String instance)
            throws IOException {
        return BigtableDataClient.create(
                BigtableDataSettings.newBuilderForEmulator(
                                emulator.getHost(), emulator.getEmulatorPort())
                        .setProjectId(project)
                        .setInstanceId(instance)
                        .build());
    }
}
