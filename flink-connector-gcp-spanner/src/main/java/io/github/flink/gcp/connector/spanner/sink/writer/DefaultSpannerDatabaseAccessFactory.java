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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.auth.Credentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.TransactionOption;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Opens a real Spanner service handle and wraps it in a {@link SpannerServiceAdapter}.
 *
 * <p>The client metrics it was given, if any, are the database access's from {@link #create()} on:
 * its close releases them after the service handle, because closed client metrics drop what they
 * are given and a handle still closing would lose whatever it recorded meanwhile. A creation that
 * fails releases them too.
 */
@Internal
public final class DefaultSpannerDatabaseAccessFactory implements SpannerDatabaseAccessFactory {

    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final SpannerWriterOptions writerOptions;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final Credentials credentialsOverride;
    private final boolean exportClientMetricsToCloudMonitoring;

    /**
     * The client metrics the client records into, or {@code null} when nothing is exported to
     * Flink. Transient: they are runtime state the writer creates on its own metric group.
     */
    @Nullable private final transient ClientMetrics clientMetrics;

    /**
     * Creates the factory.
     *
     * @param database the database to write to
     * @param writerOptions the writer tuning options
     * @param emulatorEndpoint the emulator to write to, or {@code null} for the real service
     */
    public DefaultSpannerDatabaseAccessFactory(
            DatabaseDestination database,
            SpannerWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(database, writerOptions, emulatorEndpoint, null);
    }

    /** Creates the factory with credentials loaded by the writer runtime. */
    public DefaultSpannerDatabaseAccessFactory(
            DatabaseDestination database,
            SpannerWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride) {
        this(
                database,
                writerOptions,
                emulatorEndpoint,
                credentialsOverride,
                SpannerClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING,
                null);
    }

    /**
     * Creates the factory with the client-metrics choices.
     *
     * @param database the database to write to
     * @param writerOptions the writer tuning options
     * @param emulatorEndpoint the emulator to write to, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the writer runtime, or {@code null} for ADC
     * @param exportClientMetricsToCloudMonitoring whether the client keeps its built-in Cloud
     *     Monitoring export
     * @param clientMetrics the client metrics the client records into, which the database access
     *     closes, or {@code null} to export nothing to Flink
     */
    public DefaultSpannerDatabaseAccessFactory(
            DatabaseDestination database,
            SpannerWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            boolean exportClientMetricsToCloudMonitoring,
            @Nullable ClientMetrics clientMetrics) {
        this.database = database;
        this.writerOptions = writerOptions;
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentialsOverride = credentialsOverride;
        this.exportClientMetricsToCloudMonitoring = exportClientMetricsToCloudMonitoring;
        this.clientMetrics = clientMetrics;
    }

    @Override
    public SpannerDatabaseAccess create() throws IOException {
        return create(SpannerClients.open(database, settings(), clientMetrics));
    }

    /** Builds the writer's client settings, exposed for verifying its runtime configuration. */
    @VisibleForTesting
    SpannerOptions settings() {
        SpannerOptions.Builder settings =
                SpannerClients.settings(
                        database,
                        emulatorEndpoint,
                        credentialsOverride,
                        clientMetrics,
                        exportClientMetricsToCloudMonitoring)
                        .toBuilder();
        // Re-check at the TaskManager boundary: a Java-serialized options instance can bypass the
        // public builder, and a positive sub-millisecond value becomes the SDK's zero sentinel.
        settings.getSpannerStubSettingsBuilder()
                .batchWriteSettings()
                .setSimpleTimeoutNoRetriesDuration(
                        OptionChecks.checkAtLeastOneMilli(
                                writerOptions.getBatchWriteTimeout(), "batchWriteTimeout"));
        return settings.build();
    }

    @VisibleForTesting
    SpannerDatabaseAccess create(Spanner spanner) {
        try {
            DatabaseClient client =
                    spanner.getDatabaseClient(
                            DatabaseId.of(
                                    database.getProject(),
                                    database.getInstance(),
                                    database.getDatabase()));
            TransactionOption[] transactionOptions = transactionOptions(writerOptions);
            return new SpannerServiceAdapter(
                    database.toString(),
                    client::getDialect,
                    statement -> client.singleUse().executeQuery(statement),
                    groups -> client.batchWriteAtLeastOnce(groups, transactionOptions),
                    () -> SpannerClients.close(spanner, clientMetrics));
        } catch (Throwable e) {
            // The service handle is this factory's until the adapter takes it over; nothing else
            // would ever close it. Throwable rather than Exception for the same reason every
            // creation guard in this project uses it: a first classload can fail with an Error.
            Closers.closeAllSuppressing(e, spanner::close, clientMetrics);
            throw e;
        }
    }

    /**
     * Maps the writer options onto the transaction options every batch write carries. Unset knobs
     * contribute nothing, leaving the service's own handling in place rather than restating it.
     */
    @VisibleForTesting
    static TransactionOption[] transactionOptions(SpannerWriterOptions options) {
        List<TransactionOption> transactionOptions = new ArrayList<>(2);
        if (options.getMaxCommitDelay() != null) {
            transactionOptions.add(Options.maxCommitDelay(options.getMaxCommitDelay()));
        }
        if (options.getRpcPriority() != null) {
            transactionOptions.add(Options.priority(options.getRpcPriority().toSpanner()));
        }
        return transactionOptions.toArray(new TransactionOption[0]);
    }
}
