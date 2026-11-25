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
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.v1.FirestoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.grpc.ForwardingChannelBuilder2;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    void theDatabaseAndTheEmulatorReachTheSettings() {
        FirestoreOptions settings =
                FirestoreClients.settings(DATABASE, EMULATOR, null, null, variable -> null).build();

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getDatabaseId()).isEqualTo("orders");
        assertThat(
                        ((InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider())
                                .getEndpoint())
                .isEqualTo("localhost:8080");
    }

    @Test
    void credentialsCannotBeCombinedWithAnEmulator() {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));

        assertThatThrownBy(
                        () ->
                                FirestoreClients.settings(
                                        DATABASE, EMULATOR, credentials, null, v -> null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anInheritedEmulatorVariableIsWarnedAboutWhenNoEmulatorIsConfigured() {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreClients.settings(
                    DATABASE,
                    null,
                    GoogleCredentials.create(new AccessToken("t", null)),
                    null,
                    variable ->
                            variable.equals(FirestoreClients.EMULATOR_HOST_VARIABLE)
                                    ? "stray:9000"
                                    : null);

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(
                            "FIRESTORE_EMULATOR_HOST",
                            "stray:9000",
                            "projects/p/databases/orders",
                            "shared event-loop group");
        }
    }

    @Test
    void theVariableIsNotWarnedAboutBesideAConfiguredEmulator() {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreClients.settings(DATABASE, EMULATOR, null, null, variable -> "stray:9000");

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
                FirestoreClients.settings(DATABASE, null, credentials(), null, variable -> null)
                        .setChannelProvider(
                                FirestoreSettings.defaultGrpcTransportProviderBuilder()
                                        .setChannelConfigurator(library)
                                        .build());

        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            FirestoreOptions options =
                    FirestoreClients.options(DATABASE, null, settings, lease, variable -> null);

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
                            FirestoreClients.options(
                                            DATABASE,
                                            null,
                                            FirestoreClients.settings(
                                                    DATABASE, null, credentials(), null, v -> null),
                                            lease,
                                            variable -> null)
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
     * The emulator arm dials the configured endpoint through the API's own provider, on the group,
     * with the emulator's fixed token, rather than through the library's emulator setting.
     */
    @Test
    void theEmulatorArmUsesTheApisProviderRatherThanTheLibrarysEmulatorSetting() {
        FirestoreOptions.Builder settings =
                FirestoreClients.settings(DATABASE, EMULATOR, null, null, v -> null);
        PlaintextRecorder channel = new PlaintextRecorder();
        ((InstantiatingGrpcChannelProvider) settings.build().getTransportChannelProvider())
                .toBuilder().getChannelConfigurator().apply(channel);
        assertThat(channel.plaintext).as("the emulator's channel is plaintext").isTrue();

        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            FirestoreOptions options =
                    FirestoreClients.options(DATABASE, EMULATOR, settings, lease, v -> null);

            assertThat(options.getEmulatorHost()).isNull();
            InstantiatingGrpcChannelProvider provider =
                    (InstantiatingGrpcChannelProvider) options.getTransportChannelProvider();
            assertThat(provider.getEndpoint()).isEqualTo("localhost:8080");
            // The library's emulator provider sets none, which leaves gRPC's 4 MiB.
            assertThat(provider.toBuilder().getMaxInboundMessageSize())
                    .isEqualTo(Integer.MAX_VALUE);
            assertThat(options.getCredentials())
                    .isInstanceOf(FirestoreOptions.EmulatorCredentials.class);
        } finally {
            lease.close();
        }
    }

    /**
     * With {@value FirestoreClients#EMULATOR_HOST_VARIABLE} set beside a configured endpoint, the
     * configured endpoint still wins, through the library's emulator setting, and the warning says
     * the client is off the group.
     */
    @Test
    void theVariableYieldsToTheConfiguredEndpointWithAWarning() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreOptions options =
                    FirestoreClients.options(
                            DATABASE,
                            EMULATOR,
                            FirestoreClients.settings(DATABASE, EMULATOR, null, null, v -> null),
                            lease,
                            variable -> "stray:9000");

            assertThat(options.getEmulatorHost()).isEqualTo("localhost:8080");
            assertThat(
                            ((InstantiatingGrpcChannelProvider)
                                            options.getTransportChannelProvider())
                                    .getEndpoint())
                    .isEqualTo("localhost:8080");
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(
                            "FIRESTORE_EMULATOR_HOST",
                            "stray:9000",
                            "localhost:8080",
                            "shared event-loop group");
        } finally {
            lease.close();
        }
    }

    /**
     * The library refuses a value it cannot parse, an empty one included, but never sees it beside
     * a configured endpoint.
     */
    @Test
    void anEmptyVariableBesideAConfiguredEndpointIsNotParsed() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreOptions options =
                    FirestoreClients.options(
                            DATABASE,
                            EMULATOR,
                            FirestoreClients.settings(DATABASE, EMULATOR, null, null, v -> null),
                            lease,
                            variable -> "");

            assertThat(options.getEmulatorHost()).isEqualTo("localhost:8080");
            assertThat(capture.getMessages()).singleElement().asString().contains("set to ''");
        } finally {
            lease.close();
        }
    }

    /** Without an endpoint, the library's own emulator host stays; settings(...) warned already. */
    @Test
    void anInheritedEmulatorHostWithoutAnEndpointIsLeftToTheLibrary() {
        FirestoreOptions.Builder settings =
                FirestoreClients.settings(DATABASE, null, credentials(), null, v -> null)
                        .setEmulatorHost("stray:9000");

        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreOptions options =
                    FirestoreClients.options(DATABASE, null, settings, lease, variable -> null);

            assertThat(options.getEmulatorHost()).isEqualTo("stray:9000");
            assertThat(capture.getMessages()).isEmpty();
        } finally {
            lease.close();
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
                        FirestoreClients.settings(DATABASE, null, credentials(), null, v -> null));
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

    /** A channel builder that records whether it was made plaintext. */
    private static final class PlaintextRecorder
            extends ForwardingChannelBuilder2<PlaintextRecorder> {

        private final ManagedChannelBuilder<?> delegate =
                ManagedChannelBuilder.forTarget("localhost:1");
        private boolean plaintext;

        @Override
        protected ManagedChannelBuilder<?> delegate() {
            return delegate;
        }

        @Override
        public PlaintextRecorder usePlaintext() {
            plaintext = true;
            return super.usePlaintext();
        }
    }
}
