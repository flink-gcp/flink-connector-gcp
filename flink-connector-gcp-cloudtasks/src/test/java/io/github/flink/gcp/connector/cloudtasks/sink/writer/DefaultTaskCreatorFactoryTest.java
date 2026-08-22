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

import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultTaskCreatorFactory}. */
class DefaultTaskCreatorFactoryTest {

    @TempDir Path tempDir;

    @Test
    void configuredCredentialsReachTheClientSettings() throws Exception {
        Path keyFile = ServiceAccountKeyFiles.create(tempDir);

        CloudTasksSettings settings =
                DefaultTaskCreatorFactory.productionSettings(keyFile.toString(), null).build();

        assertThat(settings.getCredentialsProvider()).isInstanceOf(FixedCredentialsProvider.class);
        assertThat(settings.getCredentialsProvider().getCredentials())
                .isInstanceOf(ServiceAccountCredentials.class);
    }

    @Test
    void absentConfiguredCredentialsLeaveApplicationDefaultsInEffect() throws Exception {
        CloudTasksSettings settings =
                DefaultTaskCreatorFactory.productionSettings(null, null).build();

        assertThat(settings.getCredentialsProvider()).isInstanceOf(GoogleCredentialsProvider.class);
    }

    @Test
    void aConfiguredChannelPoolSizesTheProductionTransport() throws Exception {
        CloudTasksSettings settings = DefaultTaskCreatorFactory.productionSettings(null, 4).build();

        InstantiatingGrpcChannelProvider provider =
                (InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider();
        assertThat(provider.getChannelPoolSettings())
                .isEqualTo(ChannelPoolSettings.staticallySized(4));
        // Sizing the pool must not cost the one provider-level default the service's builder
        // carries: the unbounded inbound message size a bare provider builder would cap at gRPC's
        // 4 MiB. The endpoint is not asserted because it never lives on the provider — the client
        // context applies it either way.
        assertThat(provider.toBuilder().getMaxInboundMessageSize()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void theClientDefaultTransportIsASingleChannel() throws Exception {
        // Pins the SDK default that ADR-0134's "unset = one channel" claim rests on; the unset arm
        // itself touches no transport code. If a gax bump fails this, the factory is not at fault:
        // update ADR-0134, the channelPoolSize javadoc and the docs' throughput formula instead.
        CloudTasksSettings settings =
                DefaultTaskCreatorFactory.productionSettings(null, null).build();

        InstantiatingGrpcChannelProvider provider =
                (InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider();
        assertThat(provider.getChannelPoolSettings())
                .isEqualTo(ChannelPoolSettings.staticallySized(1));
    }

    @Test
    void buildsAndClosesAProductionCreatorWithConfiguredCredentials() throws Exception {
        // The pool rides along so create() and close() run once against a real multi-channel
        // client; the channels connect lazily, so nothing here talks to the service.
        Path keyFile = ServiceAccountKeyFiles.create(tempDir);

        TaskCreator creator = new DefaultTaskCreatorFactory(keyFile.toString(), null, 2).create();

        assertThat(creator).isNotNull();
        creator.close();
    }

    @Test
    void rejectsAChannelPoolBesideAnEmulatorEndpoint() {
        // The sink builder and the table factory refuse the combination first; this keeps the
        // impossible state unrepresentable at the factory too (ADR-0081, ADR-0134).
        assertThatThrownBy(
                        () ->
                                new DefaultTaskCreatorFactory(
                                        null,
                                        EmulatorEndpoint.parse(
                                                "localhost:8123", "emulatorEndpoint"),
                                        4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("channelPoolSize");
    }

    @Test
    void buildsAndClosesAnEmulatorBackedCreatorWithoutCredentials() throws Exception {
        // Nothing here talks to the endpoint: the channel connects lazily, so this covers the
        // plaintext/no-credentials wiring the emulator integration tests (#25) build on.
        TaskCreator creator =
                new DefaultTaskCreatorFactory(
                                null,
                                EmulatorEndpoint.parse("localhost:8123", "emulatorEndpoint"),
                                null)
                        .create();

        assertThat(creator).isNotNull();
        creator.close();
    }

    @Test
    void theSinkPassesItsConfigurationIntoTheProductionFactory() {
        // The production createWriter wiring is three config lookups, and a dropped or swapped
        // one compiles — getMaxInFlightTasks() autoboxes into the Integer parameter — so the
        // factory the sink builds is asserted field by field.
        CloudTasksSinkConfig<String> config =
                TestSinkConfigs.config(
                        TestSinkConfigs.builder()
                                .serviceAccountKeyFile("/var/run/secrets/gcp/key.json")
                                .writerOptions(
                                        CloudTasksWriterOptions.builder()
                                                .channelPoolSize(3)
                                                .build()));

        DefaultTaskCreatorFactory factory =
                new CloudTasksCreateTaskSink<>(config).taskCreatorFactory();

        assertThat(factory.serviceAccountKeyFile()).isEqualTo("/var/run/secrets/gcp/key.json");
        assertThat(factory.emulatorEndpoint()).isNull();
        assertThat(factory.channelPoolSize()).isEqualTo(3);
    }

    @Test
    void isSerializableIntoTheJobGraph() throws Exception {
        DefaultTaskCreatorFactory emulator =
                new DefaultTaskCreatorFactory(
                        null, EmulatorEndpoint.parse("localhost:8123", "emulatorEndpoint"), null);
        DefaultTaskCreatorFactory pooled =
                new DefaultTaskCreatorFactory("/var/run/secrets/gcp/key.json", null, 4);

        DefaultTaskCreatorFactory restoredEmulator =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(emulator), getClass().getClassLoader());
        DefaultTaskCreatorFactory restoredPooled =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(pooled), getClass().getClassLoader());

        assertThat(restoredEmulator.emulatorEndpoint()).isNotNull();
        assertThat(restoredEmulator.channelPoolSize()).isNull();
        assertThat(restoredPooled.serviceAccountKeyFile())
                .isEqualTo("/var/run/secrets/gcp/key.json");
        assertThat(restoredPooled.channelPoolSize()).isEqualTo(4);
    }
}
