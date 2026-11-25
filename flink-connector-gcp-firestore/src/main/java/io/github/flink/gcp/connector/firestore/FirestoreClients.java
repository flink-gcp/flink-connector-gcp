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
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.v1.FirestoreSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.UnaryOperator;

/**
 * Builds the Firestore client every direction of this connector opens, on the connector's gRPC
 * event-loop group (ADR-0208).
 *
 * <p>{@code FirestoreOptions} takes only an instantiating gRPC provider and is {@code final}, so a
 * caller-owned channel could reach the client only through a hand-written {@code FirestoreRpc}, and
 * the client builds its channels from the provider it is given. {@link #open(DatabaseDestination,
 * EmulatorEndpoint, FirestoreOptions.Builder)} therefore builds the options once, takes the
 * provider they carry, which for the service carries the library's tracing configurator only when a
 * tracer provider is installed, puts it on the group and builds them again. For an emulator, {@link
 * #settings(DatabaseDestination, EmulatorEndpoint, Credentials, RetrySettings)} sets a plaintext
 * provider built from the API's own defaults, with the emulator's fixed token, in place of {@code
 * setEmulatorHost}, whose provider the library builds itself, at gRPC's 4 MiB inbound limit and off
 * the group. Two failures of such a provider remain for an emulator: gax parses its target as
 * {@code host:port}, so a {@code dns:///} target fails, and it builds TLS credentials when {@code
 * GOOGLE_API_USE_CLIENT_CERTIFICATE} asks for a client certificate and one is available (ADR-0208).
 *
 * <p><b>The client library also reads {@value #EMULATOR_HOST_VARIABLE} on its own</b>, whenever no
 * emulator host was set explicitly, and then replaces both the channel provider and any configured
 * credentials with the emulator's. This connector configures an emulator only through its own
 * endpoint setting, the builders' {@code emulatorEndpoint(...)} and the Table API's {@code
 * emulator-endpoint} (ADR-0064), and cannot switch the library's lookup off. Without an endpoint, a
 * job started in an environment that carries the variable would write to whatever it names, which
 * {@link #settings(DatabaseDestination, EmulatorEndpoint, Credentials, RetrySettings)} warns about
 * rather than letting it pass silently. With an endpoint, the variable is looked up first and the
 * client is built on the endpoint through {@code setEmulatorHost}, so the configured endpoint still
 * wins and the library never parses the variable's value. Either way the client then runs on the
 * provider the library built, on gRPC's shared event-loop group, and the warning says so.
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
        return settings(
                database, emulatorEndpoint, credentialsOverride, retrySettings, System::getenv);
    }

    @VisibleForTesting
    static FirestoreOptions.Builder settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable RetrySettings retrySettings,
            UnaryOperator<String> environment) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        FirestoreOptions.Builder settings =
                FirestoreOptions.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId());
        if (emulatorEndpoint != null) {
            settings.setChannelProvider(
                            FirestoreSettings.defaultGrpcTransportProviderBuilder()
                                    .setEndpoint(emulatorEndpoint.getTarget())
                                    .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                                    .build())
                    .setCredentialsProvider(
                            FixedCredentialsProvider.create(
                                    new FirestoreOptions.EmulatorCredentials()));
        } else {
            String inherited = environment.apply(EMULATOR_HOST_VARIABLE);
            if (inherited != null) {
                LOG.warn(
                        "{} is set to '{}' in this process's environment and no emulatorEndpoint"
                                + " is configured. The Firestore client library reads that"
                                + " variable itself and will send every request for {} to it,"
                                + " over plaintext with a placeholder token instead of the job's"
                                + " credentials, rather than to the Firestore service, and the"
                                + " client runs on gRPC's shared event-loop group rather than the"
                                + " connector's. Unset the variable, or configure"
                                + " emulatorEndpoint(...) if the emulator is intended.",
                        EMULATOR_HOST_VARIABLE,
                        inherited,
                        database);
            }
            if (credentialsOverride != null) {
                settings.setCredentials(credentialsOverride);
            }
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
     *     #close(Firestore)} does and then releases the lease
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<Firestore> open(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            FirestoreOptions.Builder settings)
            throws IOException {
        Preconditions.checkNotNull(settings, "settings must not be null");
        return GrpcEventLoops.openAutoCloseable(
                lease -> {
                    try {
                        return options(database, emulatorEndpoint, settings, lease, System::getenv)
                                .getService();
                    } catch (RuntimeException e) {
                        throw new IOException(
                                "Failed to create the Firestore client for " + database + ".", e);
                    }
                },
                FirestoreClients::close);
    }

    /**
     * Closes a client without waiting for its calls: {@code shutdownNow()} cancels them, then
     * {@code close()} waits up to a second each for its channel pool, its executor and its stream
     * watchdog to terminate.
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
     * Builds the settings with their channel provider on the group, or without it where the library
     * takes an emulator host from {@value #EMULATOR_HOST_VARIABLE}.
     *
     * <p>With an endpoint configured, the variable is looked up here, before the library would: a
     * value the library cannot parse, an empty one included, would otherwise fail the build, which
     * the configured endpoint did not before the client ran on the group.
     */
    @VisibleForTesting
    static FirestoreOptions options(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            FirestoreOptions.Builder settings,
            GrpcEventLoops.Lease lease,
            UnaryOperator<String> environment) {
        if (emulatorEndpoint != null) {
            String inherited = environment.apply(EMULATOR_HOST_VARIABLE);
            if (inherited != null) {
                LOG.warn(
                        "{} is set to '{}' in this process's environment, and the Firestore client"
                                + " library reads it in place of the configured emulatorEndpoint"
                                + " '{}'. The client for {} is built on the configured endpoint"
                                + " through the library's own emulator setting instead, so it runs"
                                + " on gRPC's shared event-loop group rather than the connector's,"
                                + " with gRPC's 4 MiB inbound message limit. Unset the variable to"
                                + " keep the client on the connector's group.",
                        EMULATOR_HOST_VARIABLE,
                        inherited,
                        emulatorEndpoint.getTarget(),
                        database);
                return settings.setEmulatorHost(emulatorEndpoint.getTarget()).build();
            }
        }
        FirestoreOptions built = settings.build();
        if (built.getEmulatorHost() != null) {
            // Taken from the variable without an endpoint; settings(...) warned about it already.
            return built;
        }
        // Built again from the same settings rather than from built.toBuilder(), which would carry
        // the credentials provider the first build derived: without credentials found, that is a
        // NoCredentialsProvider whose null credentials fail the second build.
        return settings.setChannelProvider(lease.applyTo(built.getTransportChannelProvider()))
                .build();
    }
}
