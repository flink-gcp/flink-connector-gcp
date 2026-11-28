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

import com.google.api.core.ApiFunction;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.cloud.grpc.GrpcTransportOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreClientsTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "orders");
    private static final EmulatorEndpoint EMULATOR =
            EmulatorEndpoint.parse("localhost:8080", "emulatorEndpoint");

    /** The library reads the variable in every build below, which the tests model on their own. */
    @BeforeAll
    static void theEmulatorVariableIsUnset() {
        assertThat(System.getenv(FirestoreClients.EMULATOR_HOST_VARIABLE))
                .as(
                        "%s is set, and the client library would read it in every build here;"
                                + " unset it to run these tests",
                        FirestoreClients.EMULATOR_HOST_VARIABLE)
                .isNull();
    }

    @Test
    void theDatabaseReachesTheSettings() {
        FirestoreOptions settings =
                FirestoreClients.settings(DATABASE, null, credentials(), null).build();

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getDatabaseId()).isEqualTo("orders");
    }

    @Test
    void credentialsCannotBeCombinedWithAnEmulator() {
        assertThatThrownBy(() -> FirestoreClients.settings(DATABASE, EMULATOR, credentials(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Without an endpoint, the client dials what the variable names, as the library would. */
    @Test
    void anInheritedEmulatorVariableIsFollowedWithAWarningWhenNoEmulatorIsConfigured() {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            assertThat(FirestoreClients.emulator(DATABASE, null, variable -> "dns:///stray:9000"))
                    .isEqualTo(EmulatorEndpoint.parse("dns:///stray:9000", "e"));

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(
                            "FIRESTORE_EMULATOR_HOST",
                            "dns:///stray:9000",
                            "projects/p/databases/orders",
                            "placeholder token");
        }
    }

    @Test
    void noVariableAndNoEndpointMeanTheService() {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            assertThat(FirestoreClients.emulator(DATABASE, null, variable -> null)).isNull();

            assertThat(capture.getMessages()).isEmpty();
        }
    }

    /**
     * The service arm keeps the provider the library built, with the configurator it carries (the
     * library's tracing one when a tracer provider is installed; a stand-in here), inside the
     * composed one it installs.
     */
    @Test
    void theServiceProviderKeepsItsConfiguratorAndGainsTheGroups() {
        AtomicInteger libraryConfigurations = new AtomicInteger();
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> library =
                builder -> {
                    libraryConfigurations.incrementAndGet();
                    return builder;
                };
        FirestoreOptions.Builder settings =
                FirestoreClients.settings(DATABASE, null, credentials(), null)
                        .setChannelProvider(
                                FirestoreSettings.defaultGrpcTransportProviderBuilder()
                                        .setChannelConfigurator(library)
                                        .build());

        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            FirestoreOptions options = FirestoreClients.serviceOptions(settings, lease);

            assertThat(options.getEmulatorHost()).isNull();
            InstantiatingGrpcChannelProvider provider =
                    (InstantiatingGrpcChannelProvider) options.getTransportChannelProvider();
            provider.toBuilder()
                    .getChannelConfigurator()
                    .apply(ManagedChannelBuilder.forTarget("localhost:1"));
            assertThat(libraryConfigurations)
                    .as("the library's configurator still runs")
                    .hasValue(1);
            assertThat(provider.toBuilder().getChannelConfigurator())
                    .as("the installed configurator composes the group's with the library's")
                    .isNotSameAs(library);
        } finally {
            lease.close();
        }
    }

    /**
     * The service arm keeps the provider the library sets up for options with no provider of their
     * own, compared here with one the library set up independently.
     */
    @Test
    void theServiceProviderIsTheLibrarysOwn() {
        InstantiatingGrpcChannelProvider library =
                (InstantiatingGrpcChannelProvider)
                        FirestoreOptions.newBuilder()
                                .setProjectId("p")
                                .setCredentials(credentials())
                                .build()
                                .getTransportChannelProvider();

        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            InstantiatingGrpcChannelProvider provider =
                    (InstantiatingGrpcChannelProvider)
                            FirestoreClients.serviceOptions(
                                            FirestoreClients.settings(
                                                    DATABASE, null, credentials(), null),
                                            lease)
                                    .getTransportChannelProvider();

            assertThat(provider.getEndpoint()).isNotNull().isEqualTo(library.getEndpoint());
            assertThat(provider.toBuilder().getMaxInboundMessageSize())
                    .isNotNull()
                    .isEqualTo(library.toBuilder().getMaxInboundMessageSize());
        } finally {
            lease.close();
        }
    }

    /**
     * The emulator arm sends every call through the connector's RPC on the caller's channel, with
     * the emulator's fixed token, and sets the emulator host so the library never reads {@value
     * FirestoreClients#EMULATOR_HOST_VARIABLE}.
     */
    @Test
    void theEmulatorArmSendsEveryCallThroughTheConnectorsRpc() throws Exception {
        try (GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel =
                EmulatorChannels.openLeasedPlaintextChannel(
                        FirestoreSettings.defaultGrpcTransportProviderBuilder(), EMULATOR)) {
            FirestoreOptions options =
                    FirestoreClients.emulatorOptions(
                            FirestoreClients.settings(DATABASE, EMULATOR, null, null),
                            EMULATOR,
                            channel.client());
            FirestoreRpc rpc = (FirestoreRpc) options.getRpc();
            try {
                assertThat(rpc).isInstanceOf(FirestoreEmulatorRpc.class);
                assertThat(options.getEmulatorHost()).isEqualTo("localhost:8080");
                assertThat(options.getCredentials())
                        .isInstanceOf(FirestoreOptions.EmulatorCredentials.class);
            } finally {
                rpc.shutdownNow();
                rpc.close();
            }
        }
    }

    /** A variable set beside a configured endpoint is ignored, empty or not, with a warning. */
    @ParameterizedTest
    @ValueSource(strings = {"stray:9000", ""})
    void theVariableYieldsToTheConfiguredEndpointWithAWarning(String value) {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            assertThat(FirestoreClients.emulator(DATABASE, EMULATOR, variable -> value))
                    .isSameAs(EMULATOR);

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(
                            "FIRESTORE_EMULATOR_HOST",
                            "'" + value + "'",
                            "is ignored",
                            "localhost:8080");
        }
    }

    /** A variable that names no {@code host:port} fails the client, naming the variable. */
    @Test
    void aMalformedVariableWithoutAnEndpointIsRefused() {
        assertThatThrownBy(() -> FirestoreClients.emulator(DATABASE, null, variable -> ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FIRESTORE_EMULATOR_HOST");
    }

    /**
     * A client whose creation fails gives back the executor the library took for it, which the
     * library's {@code GrpcFirestoreRpc} keeps when its constructor fails (#2343): here gax cannot
     * read the provider's {@code dns:///} endpoint.
     */
    @Test
    void aFailedClientGivesItsExecutorBack() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease is open before the test").isFalse();
        RecordingExecutorFactory executors = new RecordingExecutorFactory();
        // The factory goes in through the settings, where a caller would set one.
        FirestoreOptions.Builder settings =
                FirestoreClients.settings(DATABASE, null, credentials(), null)
                        .setTransportOptions(
                                GrpcTransportOptions.newBuilder()
                                        .setExecutorFactory(executors)
                                        .build())
                        .setChannelProvider(
                                FirestoreSettings.defaultGrpcTransportProviderBuilder()
                                        .setEndpoint("dns:///localhost:1")
                                        .build());

        assertThatThrownBy(() -> FirestoreClients.open(DATABASE, null, settings, variable -> null))
                .isInstanceOf(IOException.class);
        assertThat(executors.taken).as("an executor was taken").isPositive();
        assertThat(executors.given)
                .as("every executor taken was given back")
                .isEqualTo(executors.taken);
        assertThat(GrpcEventLoops.isRunning()).as("the lease was released").isFalse();
    }

    /**
     * An emulator endpoint with an IPv6 literal in an {@code http://} scheme or as a gRPC target's
     * DNS server builds: the library checks the emulator host as part of an {@code http://} URL,
     * which either, left on the endpoint, would fail.
     */
    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({"http://[::1]:1, [::1]:1", "dns://[::1]/localhost:1, localhost:1"})
    void anIpv6EmulatorEndpointBuilds(String configured, String emulatorHost) throws Exception {
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse(configured, "emulatorEndpoint");

        try (GrpcEventLoops.LeasedClient<Firestore> client =
                FirestoreClients.open(
                        DATABASE,
                        endpoint,
                        FirestoreClients.settings(DATABASE, endpoint, null, null),
                        variable -> null)) {
            assertThat(client.client().getOptions().getEmulatorHost()).isEqualTo(emulatorHost);
        }
    }

    @Test
    void theReleasingFactoryGivesBackOnlyWhatIsStillHeld() {
        RecordingExecutorFactory executors = new RecordingExecutorFactory();
        FirestoreClients.ReleasingExecutorFactory releasing =
                new FirestoreClients.ReleasingExecutorFactory(executors);

        ScheduledExecutorService first = releasing.get();
        ScheduledExecutorService second = releasing.get();
        releasing.release(first);
        releasing.releaseHeld();
        releasing.releaseHeld();
        // A late release from the library, after the cleanup gave the executor back already.
        releasing.release(second);

        assertThat(executors.taken).isEqualTo(2);
        assertThat(executors.given).isEqualTo(2);
    }

    /** Counts what it hands out and gets back; one executor, never shut down here. */
    private static final class RecordingExecutorFactory
            implements GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> {

        private final ScheduledExecutorService executor =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        private int taken;
        private int given;

        @Override
        public synchronized ScheduledExecutorService get() {
            taken++;
            return executor;
        }

        @Override
        public synchronized void release(ScheduledExecutorService released) {
            given++;
        }
    }

    /** Building the client opens no connection, so this needs no service. */
    @Test
    void theClientHoldsALeaseUntilItCloses() throws Exception {
        assertThat(GrpcEventLoops.isRunning()).as("a lease is open before the test").isFalse();
        GrpcEventLoops.LeasedClient<Firestore> client =
                FirestoreClients.open(
                        DATABASE,
                        null,
                        FirestoreClients.settings(DATABASE, null, credentials(), null));
        try {
            assertThat(GrpcEventLoops.isRunning()).as("the client's lease").isTrue();
        } finally {
            client.close();
        }
        assertThat(GrpcEventLoops.isRunning()).as("the lease was released").isFalse();
    }

    private static GoogleCredentials credentials() {
        return GoogleCredentials.create(new AccessToken("t", null));
    }

    /** A client closes with {@code shutdownNow()} first, so no call holds {@code close()}. */
    @Test
    void aClientIsShutDownBeforeItCloses() throws Exception {
        List<String> steps = new ArrayList<>();

        FirestoreClients.close(client(steps, null, null));

        assertThat(steps).containsExactly("shutdownNow", "close");
    }

    /** A failed {@code shutdownNow()} still closes the client, and is the failure thrown. */
    @Test
    void aFailedShutdownStillClosesAndKeepsItsFailure() {
        List<String> steps = new ArrayList<>();
        IllegalStateException shutdown = new IllegalStateException("shutdownNow failed");
        IllegalStateException close = new IllegalStateException("close failed");

        assertThatThrownBy(() -> FirestoreClients.close(client(steps, shutdown, close)))
                .isSameAs(shutdown)
                .hasSuppressedException(close);
        assertThat(steps).containsExactly("shutdownNow", "close");
    }

    /** One failure, should both steps throw the same instance, is thrown once and kept. */
    @Test
    void oneFailureFromBothStepsIsThrownAsItIs() {
        List<String> steps = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("released twice");

        assertThatThrownBy(() -> FirestoreClients.close(client(steps, failure, failure)))
                .isSameAs(failure);
        assertThat(steps).containsExactly("shutdownNow", "close");
    }

    /**
     * Without credentials found, the first build derives a credentials provider whose credentials
     * are {@code null}; the second build must not carry it, or it fails. Run in a JVM whose
     * Application Default Credentials point at a missing file, since that lookup reads the process
     * environment.
     */
    @Test
    void optionsBuildWithoutCredentialsFound(@TempDir Path directory) throws Exception {
        // Surefire's own class path when it runs the tests in-process; the JVM's otherwise.
        String classPath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Path outputFile = directory.resolve("probe.out");
        ProcessBuilder probe =
                new ProcessBuilder(
                                ProcessHandle.current().info().command().orElse("java"),
                                "-cp",
                                classPath,
                                NoCredentialsOptionsProbe.class.getName())
                        .redirectErrorStream(true)
                        .redirectOutput(outputFile.toFile());
        probe.environment()
                .put(
                        "GOOGLE_APPLICATION_CREDENTIALS",
                        directory.resolve("missing.json").toString());
        probe.environment().remove(FirestoreClients.EMULATOR_HOST_VARIABLE);
        Process process = probe.start();
        boolean finished;
        try {
            finished = process.waitFor(60, TimeUnit.SECONDS);
        } finally {
            // Also when the wait is interrupted, so a stalled probe never outlives the test.
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor();
            }
        }
        // Decoded leniently: the child's diagnostics need not be valid UTF-8.
        String output = new String(Files.readAllBytes(outputFile), UTF_8);

        assertThat(finished).as("the probe finished; its output:%n%s", output).isTrue();
        assertThat(process.exitValue()).as("the probe's exit; its output:%n%s", output).isZero();
    }

    /** A client that records its shutdown and close, failing either as given. */
    private static Firestore client(
            List<String> steps,
            @Nullable RuntimeException shutdownFailure,
            @Nullable RuntimeException closeFailure) {
        return (Firestore)
                Proxy.newProxyInstance(
                        Firestore.class.getClassLoader(),
                        new Class<?>[] {Firestore.class},
                        (proxy, method, args) -> {
                            String name = method.getName();
                            if (!name.equals("shutdownNow") && !name.equals("close")) {
                                throw new UnsupportedOperationException(name);
                            }
                            steps.add(name);
                            RuntimeException failure =
                                    name.equals("close") ? closeFailure : shutdownFailure;
                            if (failure != null) {
                                throw failure;
                            }
                            return null;
                        });
    }
}
