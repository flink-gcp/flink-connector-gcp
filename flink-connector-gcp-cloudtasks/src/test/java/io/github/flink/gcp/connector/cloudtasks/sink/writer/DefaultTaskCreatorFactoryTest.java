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

import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.api.gax.tracing.OpenTelemetryMetricsFactory;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.cloud.tasks.v2.stub.CloudTasksStub;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.cloudtasks.CloudTasksClientMetrics;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultTaskCreatorFactory}. */
class DefaultTaskCreatorFactoryTest {

    @TempDir Path tempDir;

    @AfterEach
    void noLeaseIsLeftOpen() {
        // The event-loop group is process-wide, so a creator left open would keep it running into
        // the next test and hide a release that never happened.
        assertThat(GrpcEventLoops.isRunning()).as("an event-loop lease was left open").isFalse();
    }

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
    void theProductionArmInstallsGaxsOpenTelemetryMetricsFactoryForClientMetrics()
            throws Exception {
        // Through create() on the arm no emulator job reaches; the ITCase covers the emulator arm.
        // The channels connect lazily, so nothing here talks to the service.
        Path keyFile = ServiceAccountKeyFiles.create(tempDir);
        DefaultTaskCreatorFactory factory =
                new DefaultTaskCreatorFactory(keyFile.toString(), null, null);

        try (var exporting =
                        (DefaultTaskCreatorFactory.CloudTasksClientAdapter)
                                factory.create(ClientMetrics.create("rpc.method"));
                var silent =
                        (DefaultTaskCreatorFactory.CloudTasksClientAdapter) factory.create(null)) {
            assertThat(tracerFactory(exporting)).isInstanceOf(OpenTelemetryMetricsFactory.class);
            assertThat(tracerFactory(silent)).isNotInstanceOf(OpenTelemetryMetricsFactory.class);
        }
    }

    private static Object tracerFactory(DefaultTaskCreatorFactory.CloudTasksClientAdapter adapter) {
        return adapter.client().getSettings().getStubSettings().getTracerFactory();
    }

    @Test
    void aProductionCreatorBuildsItsChannelsOnTheEventLoopGroup() throws Exception {
        // Both transports: the client's default one and a resized pool, which the lease must not
        // cost its size. The channels connect lazily, so nothing here talks to the service.
        Path keyFile = ServiceAccountKeyFiles.create(tempDir);

        for (Integer pool : new Integer[] {null, 2}) {
            TaskCreator creator =
                    new DefaultTaskCreatorFactory(keyFile.toString(), null, pool).create(null);
            try {
                assertThat(GrpcEventLoops.isRunning())
                        .as("the creator holds an event-loop lease")
                        .isTrue();
                InstantiatingGrpcChannelProvider provider =
                        provider(
                                ((DefaultTaskCreatorFactory.CloudTasksClientAdapter) creator)
                                        .client()
                                        .getSettings()
                                        .getTransportChannelProvider());
                assertThat(provider.getChannelPoolSettings())
                        .isEqualTo(ChannelPoolSettings.staticallySized(pool == null ? 1 : pool));
                assertBuildsChannelsOnTheGroup(provider);
            } finally {
                creator.close();
            }
            assertThat(GrpcEventLoops.isRunning()).as("the creator released its lease").isFalse();
        }
    }

    @Test
    void theRetentionReadbackBuildsItsChannelsOnTheEventLoopGroup() throws Exception {
        // A key file rather than ADC: the client resolves its credentials as it is created, and a
        // build machine need not have application-default credentials.
        Path keyFile = ServiceAccountKeyFiles.create(tempDir);
        try (var client = DefaultTaskCreatorFactory.openRetentionClient(keyFile.toString())) {
            assertBuildsChannelsOnTheGroup(
                    provider(client.client().getSettings().getTransportChannelProvider()));
        }
    }

    private static InstantiatingGrpcChannelProvider provider(TransportChannelProvider provider) {
        assertThat(provider).isInstanceOf(InstantiatingGrpcChannelProvider.class);
        return (InstantiatingGrpcChannelProvider) provider;
    }

    /**
     * Builds a channel through the provider's configurator, connects it to a local socket and
     * checks that a thread of the connector's group serves it: the group starts a thread only when
     * a channel registers on it, and gRPC's own threads are named otherwise.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertBuildsChannelsOnTheGroup(InstantiatingGrpcChannelProvider provider)
            throws Exception {
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no group thread before the channel connects")
                .isTrue();
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator =
                provider.toBuilder().getChannelConfigurator();
        assertThat(configurator).as("the provider carries the lease's configurator").isNotNull();
        try (ServerSocket server = new ServerSocket(0)) {
            ManagedChannel channel =
                    configurator
                            .apply(
                                    NettyChannelBuilder.forAddress(
                                                    "127.0.0.1", server.getLocalPort())
                                            .usePlaintext())
                            .build();
            try {
                channel.getState(true);
                assertThat(GrpcEventLoopThreads.awaitAlive())
                        .as("a %s* thread serves the channel", GrpcEventLoopThreads.PREFIX)
                        .isTrue();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
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
    void anEmulatorCreatorRunsItsOwnPlaintextChannelOnTheEventLoopGroup() throws Exception {
        // Both target forms EmulatorEndpoint accepts: host:port, and a scheme-prefixed gRPC target,
        // whose resolver scheme the channel keeps rather than splitting it into a host and a port.
        // A socket each, so that a reconnect left over from one form cannot answer for the other.
        for (String scheme : new String[] {"", "dns:///"}) {
            try (ServerSocket server = new ServerSocket(0)) {
                server.setSoTimeout(10_000);
                String target = scheme + "127.0.0.1:" + server.getLocalPort();
                assertThat(GrpcEventLoopThreads.awaitNone())
                        .as("no group thread before the creator opens")
                        .isTrue();
                TaskCreator creator =
                        new DefaultTaskCreatorFactory(
                                        null,
                                        EmulatorEndpoint.parse(target, "emulatorEndpoint"),
                                        null)
                                .create(null);
                try {
                    DefaultTaskCreatorFactory.CloudTasksClientAdapter adapter =
                            (DefaultTaskCreatorFactory.CloudTasksClientAdapter) creator;
                    TransportChannelProvider provider =
                            adapter.client().getSettings().getTransportChannelProvider();
                    assertThat(provider.shouldAutoClose())
                            .as("the client does not close the caller-owned channel")
                            .isFalse();
                    assertThat(adapter.client().getSettings().getCredentialsProvider())
                            .isInstanceOf(NoCredentialsProvider.class);

                    ManagedChannel channel =
                            (ManagedChannel)
                                    ((GrpcTransportChannel) provider.getTransportChannel())
                                            .getChannel();
                    channel.getState(true);
                    // The connection reaching this socket, opening with HTTP/2's cleartext
                    // preface rather than a TLS handshake, shows the target resolved to it and
                    // the channel is plaintext; the thread name shows which group serves it.
                    try (Socket accepted = server.accept()) {
                        accepted.setSoTimeout(10_000);
                        byte[] preface = new byte[3];
                        accepted.getInputStream().readNBytes(preface, 0, 3);
                        assertThat(new String(preface, StandardCharsets.US_ASCII))
                                .as("the first bytes %s sends", target)
                                .isEqualTo("PRI");
                    }
                    assertThat(GrpcEventLoopThreads.awaitAlive())
                            .as("a %s* thread serves %s", GrpcEventLoopThreads.PREFIX, target)
                            .isTrue();
                } finally {
                    creator.close();
                }
                assertThat(GrpcEventLoops.isRunning())
                        .as("the creator released both leases")
                        .isFalse();
            }
        }
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
    void retentionReadbackUsesTheSameExplicitCredentialsOrAdcAndValidatesItsDuration()
            throws Exception {
        Path key = ServiceAccountKeyFiles.create(tempDir);
        assertThat(
                        DefaultTaskCreatorFactory.retentionSettings(key.toString())
                                .getCredentialsProvider()
                                .getCredentials())
                .isEqualTo(
                        DefaultTaskCreatorFactory.productionSettings(key.toString(), null)
                                .getCredentialsProvider()
                                .getCredentials());
        assertThat(DefaultTaskCreatorFactory.retentionSettings(null).getCredentialsProvider())
                .isInstanceOf(GoogleCredentialsProvider.class);
        var queue = com.google.cloud.tasks.v2beta3.Queue.newBuilder();
        assertThat(DefaultTaskCreatorFactory.queueRetention(queue.build()))
                .isEqualTo(java.time.Duration.ofHours(1));
        queue.setTombstoneTtl(com.google.protobuf.Duration.newBuilder().setSeconds(7200));
        assertThat(DefaultTaskCreatorFactory.queueRetention(queue.build()))
                .isEqualTo(java.time.Duration.ofHours(2));
        queue.setTombstoneTtl(com.google.protobuf.Duration.newBuilder().setSeconds(-1));
        assertThatThrownBy(() -> DefaultTaskCreatorFactory.queueRetention(queue.build()))
                .hasMessageContaining("invalid tombstoneTtl");
        queue.setTombstoneTtl(com.google.protobuf.Duration.newBuilder().setNanos(1_000_000_000));
        assertThatThrownBy(() -> DefaultTaskCreatorFactory.queueRetention(queue.build()))
                .hasMessageContaining("invalid tombstoneTtl");
    }

    @Test
    void emulatorRetentionReadbackFailsBeforeLoadingCredentials() {
        var factory =
                new DefaultTaskCreatorFactory(
                        "/missing-must-not-be-read.json",
                        EmulatorEndpoint.parse("localhost:8123", "emulatorEndpoint"),
                        null);
        assertThatThrownBy(() -> factory.readQueueRetention("projects/p/locations/l/queues/q"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unavailable for an emulator endpoint");
    }

    @Test
    void retentionDiagnosticsKeepStatusWithoutVendorDetailsOrCause() {
        for (var status :
                new io.grpc.Status[] {
                    io.grpc.Status.NOT_FOUND,
                    io.grpc.Status.PERMISSION_DENIED,
                    io.grpc.Status.UNAVAILABLE
                }) {
            var failure =
                    DefaultTaskCreatorFactory.retentionReadFailure(
                            new IllegalStateException(
                                    "private wrapper",
                                    status.withDescription("private vendor payload")
                                            .asRuntimeException()));
            assertThat(failure)
                    .hasMessageContaining("status=" + status.getCode())
                    .hasMessageNotContaining("private")
                    .hasNoCause();
        }
        assertThat(
                        DefaultTaskCreatorFactory.retentionReadFailure(
                                new IllegalStateException("private")))
                .hasMessageContaining("status=UNCLASSIFIED")
                .hasMessageNotContaining("private")
                .hasNoCause();
    }

    @Test
    void stagedCallPassesTheOriginalAbsoluteDeadlineThroughTheProductionAdapter() throws Exception {
        var observed =
                new java.util.concurrent.atomic.AtomicReference<
                        com.google.api.gax.rpc.ApiCallContext>();
        var stub =
                new com.google.cloud.tasks.v2.stub.CloudTasksStub() {
                    @Override
                    public com.google.api.gax.rpc.UnaryCallable<
                                    com.google.cloud.tasks.v2.CreateTaskRequest,
                                    com.google.cloud.tasks.v2.Task>
                            createTaskCallable() {
                        return new com.google.api.gax.rpc.UnaryCallable<>() {
                            @Override
                            public com.google.api.core.ApiFuture<com.google.cloud.tasks.v2.Task>
                                    futureCall(
                                            com.google.cloud.tasks.v2.CreateTaskRequest request,
                                            com.google.api.gax.rpc.ApiCallContext context) {
                                observed.set(context);
                                return com.google.api.core.ApiFutures.immediateFuture(
                                        request.getTask());
                            }
                        };
                    }

                    @Override
                    public void close() {}

                    @Override
                    public void shutdown() {}

                    @Override
                    public void shutdownNow() {}

                    @Override
                    public boolean isShutdown() {
                        return true;
                    }

                    @Override
                    public boolean isTerminated() {
                        return true;
                    }

                    @Override
                    public boolean awaitTermination(
                            long duration, java.util.concurrent.TimeUnit unit) {
                        return true;
                    }
                };
        var request = com.google.cloud.tasks.v2.CreateTaskRequest.getDefaultInstance();
        try (var adapter =
                new DefaultTaskCreatorFactory.CloudTasksClientAdapter(
                        GrpcEventLoops.open(
                                lease -> com.google.cloud.tasks.v2.CloudTasksClient.create(stub)),
                        null)) {
            var deadline = io.grpc.Deadline.after(-1, java.util.concurrent.TimeUnit.SECONDS);
            adapter.createTask(request, deadline).get();
            assertThat(
                            ((com.google.api.gax.grpc.GrpcCallContext) observed.get())
                                    .getCallOptions()
                                    .getDeadline())
                    .isSameAs(deadline);
        }
    }

    @Test
    void theAdapterClosesItsClientMetricsOnlyAfterItsClientTerminates() throws Exception {
        MetricListener listener = new MetricListener();
        ClientMetrics metrics = CloudTasksClientMetrics.register(listener.getMetricGroup());
        DoubleHistogram histogram =
                metrics.openTelemetry()
                        .getMeterProvider()
                        .get("gax")
                        .histogramBuilder("gcp.client.request.duration")
                        .setUnit("s")
                        .build();
        Attributes createTask =
                Attributes.of(
                        AttributeKey.stringKey("rpc.method"),
                        "google.cloud.tasks.v2.CloudTasks/CreateTask");

        // A call completing while the client terminates still records; one after close does not.
        new DefaultTaskCreatorFactory.CloudTasksClientAdapter(
                        GrpcEventLoops.open(
                                lease ->
                                        com.google.cloud.tasks.v2.CloudTasksClient.create(
                                                new IdleStub(
                                                        () -> histogram.record(0.25, createTask)))),
                        metrics)
                .close();
        histogram.record(0.5, createTask);

        assertThat(listener.getCounter("rpcMethod", "CreateTask", "operations"))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(1));
        assertThat(listener.getCounter("rpcMethod", "CreateTask", "operationTimeMillis"))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(250));
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

    /** A stub that serves no call and terminates at once, running a hook as it does. */
    private static final class IdleStub extends CloudTasksStub {

        private final Runnable onTermination;

        private IdleStub(Runnable onTermination) {
            this.onTermination = onTermination;
        }

        @Override
        public UnaryCallable<CreateTaskRequest, Task> createTaskCallable() {
            return new UnaryCallable<>() {
                @Override
                public ApiFuture<Task> futureCall(
                        CreateTaskRequest request, ApiCallContext context) {
                    throw new AssertionError("No call is expected");
                }
            };
        }

        @Override
        public void close() {}

        @Override
        public void shutdown() {}

        @Override
        public void shutdownNow() {}

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public boolean isTerminated() {
            return true;
        }

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) {
            onTermination.run();
            return true;
        }
    }
}
