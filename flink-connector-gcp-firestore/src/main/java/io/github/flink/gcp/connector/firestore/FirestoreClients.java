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
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.cloud.grpc.GrpcTransportOptions;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.UnaryOperator;

/**
 * Builds the Firestore client every direction of this connector opens, on the connector's gRPC
 * event-loop group (ADR-0208).
 *
 * <p><b>For the service</b>, {@code FirestoreOptions} takes only an instantiating gRPC provider and
 * is {@code final}, and the client builds its channels from the provider it is given. {@link
 * #open(DatabaseDestination, EmulatorEndpoint, FirestoreOptions.Builder)} therefore builds the
 * options once, takes the provider they carry, which carries the library's tracing configurator
 * only when a tracer provider is installed, puts it on the group and builds them again. The
 * transport's executor factory is wrapped, so that a client whose creation fails gives back the
 * executor the library's {@code GrpcFirestoreRpc} took before it failed, which that constructor
 * keeps (google-cloud-firestore 3.53.0, #2343).
 *
 * <p><b>For an emulator</b>, the client sends every call through {@link FirestoreEmulatorRpc}, set
 * through {@code setServiceRpcFactory}, on a plaintext channel the connector opens on the group
 * with the API's channel defaults and cuts after the client closes; {@link FirestoreEmulatorRpc}
 * says why an instantiating provider cannot carry it (#2343, ADR-0208). The endpoint's {@code
 * http://} or {@code https://} scheme is removed; a resolver scheme such as {@code dns:///} is kept
 * for gRPC.
 *
 * <p><b>The client library also reads {@value #EMULATOR_HOST_VARIABLE} on its own</b> whenever no
 * emulator host was set, so an emulator client always sets one and the library never reads the
 * variable. {@code emulator(...)} decides instead: beside a configured endpoint ({@code
 * emulatorEndpoint(...)}, or the Table API's {@code emulator-endpoint}, ADR-0064) the variable is
 * ignored; without one, the client dials the emulator it names, as the library would have; either
 * way with a warning. Failing the job on the variable stays declined (ADR-0171).
 */
@Internal
public final class FirestoreClients {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreClients.class);

    /** The environment variable the client library reads an emulator host from. */
    @VisibleForTesting static final String EMULATOR_HOST_VARIABLE = "FIRESTORE_EMULATOR_HOST";

    private FirestoreClients() {}

    /**
     * Assembles client settings with an optional runtime-loaded credential override, for {@link
     * #open(DatabaseDestination, EmulatorEndpoint, FirestoreOptions.Builder)}.
     *
     * @param database the database the client will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service, unless
     *     {@value #EMULATOR_HOST_VARIABLE} names a host
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @param retrySettings the transport's retry settings, or {@code null} for the library's
     * @return the settings, not yet built
     */
    public static FirestoreOptions.Builder settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable RetrySettings retrySettings) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        FirestoreOptions.Builder settings =
                FirestoreOptions.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId());
        if (credentialsOverride != null) {
            settings.setCredentials(credentialsOverride);
        }
        if (retrySettings != null) {
            settings.setRetrySettings(retrySettings);
        }
        return settings;
    }

    /**
     * Opens a client from settings assembled by {@link #settings(DatabaseDestination,
     * EmulatorEndpoint, Credentials, RetrySettings)}, under a lease of the connector's gRPC
     * event-loop group.
     *
     * @param database the database to reach, for the failure message
     * @param emulatorEndpoint the emulator the settings were assembled for, or {@code null}
     * @param settings the settings to open the client from; building them changes the builder
     * @return the client paired with its lease; close the holder, which closes the client as {@link
     *     #close(Firestore)} does, then shuts an emulator's channel down without waiting for its
     *     calls and closes it, and then releases the lease
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<Firestore> open(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            FirestoreOptions.Builder settings)
            throws IOException {
        return open(database, emulatorEndpoint, settings, System::getenv);
    }

    @VisibleForTesting
    static GrpcEventLoops.LeasedClient<Firestore> open(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            FirestoreOptions.Builder settings,
            UnaryOperator<String> environment)
            throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        @Nullable EmulatorEndpoint emulator = emulator(database, emulatorEndpoint, environment);
        if (emulator == null) {
            String failure = "Failed to create the Firestore client for " + database + ".";
            return GrpcEventLoops.openAutoCloseable(
                    lease -> {
                        FirestoreOptions options;
                        try {
                            options = serviceOptions(settings, lease);
                        } catch (RuntimeException e) {
                            throw new IOException(failure, e);
                        }
                        try {
                            return options.getService();
                        } catch (RuntimeException | Error e) {
                            Closers.closeAllSuppressing(
                                    e,
                                    ((ReleasingExecutorFactory)
                                                    ((GrpcTransportOptions)
                                                                    options.getTransportOptions())
                                                            .getExecutorFactory())
                                            ::releaseHeld);
                            if (e instanceof Error) {
                                throw (Error) e;
                            }
                            throw new IOException(failure, e);
                        }
                    },
                    FirestoreClients::close);
        }
        EmulatorEndpoint target = emulator.withoutHttpScheme();
        GrpcEventLoops.LeasedClient<GrpcTransportChannel> channel =
                EmulatorChannels.openLeasedPlaintextChannel(
                        FirestoreSettings.defaultGrpcTransportProviderBuilder(), target);
        try {
            return GrpcEventLoops.openAutoCloseable(
                    lease -> {
                        try {
                            return emulatorOptions(settings, target, channel.client()).getService();
                        } catch (RuntimeException e) {
                            throw new IOException(
                                    "Failed to create the Firestore client for "
                                            + database
                                            + " on the emulator at "
                                            + emulator
                                            + ".",
                                    e);
                        }
                    },
                    // The channel is cut before its bounded close, which would otherwise wait five
                    // seconds for a stream still open; the client's own shutdown leaves the
                    // caller's channel alone.
                    client ->
                            Closers.closeAll(
                                    () -> close(client), channel.client()::shutdownNow, channel));
        } catch (IOException | RuntimeException | Error e) {
            Closers.closeAllSuppressing(e, channel);
            throw e;
        }
    }

    /**
     * Closes a client without waiting for its calls: for a service client, {@code shutdownNow()}
     * cancels them, then {@code close()} waits up to a second each for its channel pool, its
     * executor and its stream watchdog to terminate. An emulator client's channel is the caller's,
     * which {@link #open(DatabaseDestination, EmulatorEndpoint, FirestoreOptions.Builder)} cuts
     * after this returns; here its client stops its own retry executor and gives the shared one
     * back.
     *
     * <p>{@code close()} alone waits for a query stream still open until the stream's deadline,
     * five minutes by default (measured 2026-11-25 against a server that never answered, with
     * google-cloud-firestore 3.53.0): cancelling the future of {@code Query.get()} or of a
     * read-only transaction ends the caller's wait but not the stream, since the library discards
     * the stream's controller. A component closes its client once it has stopped using it or has
     * stopped waiting for it, so a call this cancels fails rather than holding the close.
     *
     * @param client the client
     * @throws Exception if the close fails; both steps run, and a failure of {@code shutdownNow()}
     *     is thrown with a different failure of {@code close()} suppressed in it
     */
    @VisibleForTesting
    static void close(Firestore client) throws Exception {
        Closers.closeAll(client::shutdownNow, client);
    }

    /**
     * Returns the emulator a client dials: the configured endpoint, or else the one {@value
     * #EMULATOR_HOST_VARIABLE} names, which the library would have dialled; {@code null} for the
     * service. Either use of the variable is warned about: ignored beside a configured endpoint,
     * followed without one.
     */
    @VisibleForTesting
    @Nullable
    static EmulatorEndpoint emulator(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            UnaryOperator<String> environment) {
        String inherited = environment.apply(EMULATOR_HOST_VARIABLE);
        if (emulatorEndpoint != null) {
            if (inherited != null) {
                LOG.warn(
                        "{} is set to '{}' in this process's environment, and is ignored: the"
                                + " client for {} dials the configured emulatorEndpoint '{}'.",
                        EMULATOR_HOST_VARIABLE,
                        inherited,
                        database,
                        emulatorEndpoint.getTarget());
            }
            return emulatorEndpoint;
        }
        if (inherited == null) {
            return null;
        }
        LOG.warn(
                "{} is set to '{}' in this process's environment and no emulatorEndpoint is"
                        + " configured. As the Firestore client library would, the client sends"
                        + " every request for {} to it, over plaintext with a placeholder token"
                        + " instead of the job's credentials, rather than to the Firestore service."
                        + " Unset the variable, or configure emulatorEndpoint(...) if the emulator"
                        + " is intended.",
                EMULATOR_HOST_VARIABLE,
                inherited,
                database);
        return EmulatorEndpoint.parse(inherited, EMULATOR_HOST_VARIABLE);
    }

    /**
     * Builds the settings for the service with their channel provider on the group and the executor
     * factory they carry, the library's default unless a caller set one, wrapped in a {@link
     * ReleasingExecutorFactory}.
     *
     * <p>Built again from the same settings rather than from the first build's {@code toBuilder()},
     * which would carry the credentials provider the first build derived: without credentials
     * found, that is a {@code NoCredentialsProvider} whose {@code null} credentials fail the second
     * build.
     */
    @VisibleForTesting
    static FirestoreOptions serviceOptions(
            FirestoreOptions.Builder settings, GrpcEventLoops.Lease lease) {
        FirestoreOptions built = settings.build();
        ReleasingExecutorFactory executors =
                new ReleasingExecutorFactory(
                        ((GrpcTransportOptions) built.getTransportOptions()).getExecutorFactory());
        return settings.setTransportOptions(
                        GrpcTransportOptions.newBuilder().setExecutorFactory(executors).build())
                .setChannelProvider(lease.applyTo(built.getTransportChannelProvider()))
                .build();
    }

    /**
     * Builds the settings for an emulator: every call goes through an {@link FirestoreEmulatorRpc}
     * on the given channel, with the emulator's fixed token. The emulator host is set, to the
     * {@code host:port} the channel dials ({@link EmulatorEndpoint#httpAuthority()}), so that the
     * library does not read {@value #EMULATOR_HOST_VARIABLE}; the provider it builds from it is
     * never used, but the library checks the host as part of an {@code http://} URL, which an
     * {@code http://} scheme left on the endpoint would fail for an IPv6 literal, and a gRPC target
     * naming its DNS server by an IPv6 literal, such as {@code dns://[::1]/host:port}, would fail
     * for any host.
     */
    @VisibleForTesting
    static FirestoreOptions emulatorOptions(
            FirestoreOptions.Builder settings,
            EmulatorEndpoint emulator,
            GrpcTransportChannel channel) {
        return settings.setEmulatorHost(emulator.httpAuthority())
                .setCredentialsProvider(
                        FixedCredentialsProvider.create(new FirestoreOptions.EmulatorCredentials()))
                .setServiceRpcFactory(
                        options -> {
                            try {
                                return FirestoreEmulatorRpc.create(options, channel);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                .build();
    }

    /**
     * The transport's executor factory, remembering what it has handed out and not yet been given
     * back, so that a client whose creation failed after taking an executor gives it back.
     */
    @VisibleForTesting
    static final class ReleasingExecutorFactory
            implements GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> {

        private final GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> delegate;
        private final List<ScheduledExecutorService> held = new ArrayList<>();

        ReleasingExecutorFactory(
                GrpcTransportOptions.ExecutorFactory<ScheduledExecutorService> delegate) {
            this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null");
        }

        @Override
        public ScheduledExecutorService get() {
            ScheduledExecutorService executor = delegate.get();
            synchronized (held) {
                held.add(executor);
            }
            return executor;
        }

        /**
         * Gives an executor back, unless {@link #releaseHeld()} already has: a failed creation's
         * cleanup may race a late release from the library's own shutdown.
         */
        @Override
        public void release(ScheduledExecutorService executor) {
            boolean stillHeld;
            synchronized (held) {
                stillHeld = held.remove(executor);
            }
            if (stillHeld) {
                delegate.release(executor);
            }
        }

        /** Gives back every executor handed out and not given back yet. */
        void releaseHeld() {
            List<ScheduledExecutorService> toRelease;
            synchronized (held) {
                toRelease = new ArrayList<>(held);
                held.clear();
            }
            for (ScheduledExecutorService executor : toRelease) {
                delegate.release(executor);
            }
        }
    }
}
