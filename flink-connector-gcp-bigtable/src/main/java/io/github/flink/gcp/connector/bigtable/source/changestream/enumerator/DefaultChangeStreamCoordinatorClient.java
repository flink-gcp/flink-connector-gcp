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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.AppProfileName;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.TableName;
import com.google.cloud.bigtable.admin.v2.BaseBigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.PerCallClientRunner;
import io.github.flink.gcp.connector.bigtable.BigtableClients;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Native java-bigtable implementation of the coordinator operations.
 *
 * <p>One client belongs to one enumerator: {@link DefaultChangeStreamCoordinatorClientFactory}
 * mints it, and the source mints one per {@code createEnumerator} and {@code restoreEnumerator}
 * ({@code docs/adr/0128}). It holds no SDK client between calls: each operation opens the client
 * family it needs on the connector's gRPC event-loop group, makes its one call and closes it, and
 * {@link #close()} cancels a call still in flight ({@link PerCallClientRunner}, ADR-0208). The
 * enumerator runs on the JobManager, which Flink 2.2.1 lets release the job's class loader before
 * the enumerator closes, so a client held until then could tear its group down on a closed loader
 * and exit the JobManager. That includes the reconciliation scan, which opens a data client every
 * {@code RECONCILIATION_SCAN_INTERVAL}; minting this object opens nothing.
 *
 * <p>The data client never keeps the built-in Cloud Monitoring export, whatever the source's
 * choice: the export's own {@code MetricServiceClient} would put a channel on gRPC's event-loop
 * group on the JobManager at every call, each client would report under a client id of its own, and
 * each close would flush the export. The source's choice governs its readers' clients. Client
 * metrics exported to Flink are unaffected.
 *
 * <p><b>Threads.</b> The enumerator's calls run on the executor {@code
 * SplitEnumeratorContext#callAsync} hands the work to, while {@link #close()} can run on another
 * thread, normally Flink's closer thread, the one it starts to close the source coordinator; there,
 * {@code SourceCoordinator.close()} closes the enumerator before it shuts that executor down.
 * {@link PerCallClientRunner} prepares each call's settings and starts the call under its monitor,
 * builds the client outside it, and makes its closed state one-way: a call that overtakes the
 * teardown is refused, a client that finishes building after it is closed unused, and a call the
 * teardown overtakes is cancelled. The teardown therefore never waits for a client being built,
 * which for a data client includes channel priming. The credentials are cleared once the runner is
 * closed, so no settings are built from them after it and nothing reaches Bigtable as the process's
 * application default credentials.
 */
@Internal
public final class DefaultChangeStreamCoordinatorClient implements ChangeStreamCoordinatorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultChangeStreamCoordinatorClient.class);

    private final TableDestination table;
    private final String appProfileId;
    private final PerCallClientRunner runner;

    @Nullable private volatile CredentialsProvider credentials;

    /** The enumerator's client metrics; only the data client records into them. */
    @Nullable private volatile ClientMetrics clientMetrics;

    public DefaultChangeStreamCoordinatorClient(TableDestination table, String appProfileId) {
        this(table, appProfileId, null);
    }

    /**
     * Creates the client with the provider its owner loaded.
     *
     * @param table the table whose change stream is coordinated
     * @param appProfileId the single-cluster application profile to route through
     * @param credentials the provider to build all three client families with, or {@code null} to
     *     leave application default credentials in place
     */
    public DefaultChangeStreamCoordinatorClient(
            TableDestination table,
            String appProfileId,
            @Nullable CredentialsProvider credentials) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.appProfileId =
                Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        Preconditions.checkArgument(!appProfileId.isBlank(), "appProfileId must not be blank");
        this.credentials = credentials;
        this.runner = new PerCallClientRunner("Bigtable Change Streams coordinator for " + table);
    }

    @Override
    public void validateSingleClusterAppProfile() throws Exception {
        validateSingleClusterAppProfile(
                () ->
                        runner.call(
                                this::instanceAdminSettingsBuilder,
                                settings -> BigtableClients.openInstanceAdmin(settings, null),
                                client ->
                                        client.getAppProfileCallable()
                                                .futureCall(appProfileRequest())));
    }

    /** The profile lookup the preflight sends. */
    GetAppProfileRequest appProfileRequest() {
        return GetAppProfileRequest.newBuilder()
                .setName(
                        AppProfileName.of(table.getProject(), table.getInstance(), appProfileId)
                                .toString())
                .build();
    }

    /**
     * The preflight around a profile lookup, taking the lookup as an argument.
     *
     * <p>The one seam a test needs that a value cannot provide: the arm below is entered by the
     * <em>lookup</em> failing, so a test that passed an {@link AppProfile} could not reach it.
     * Everything the check itself does is {@link #checkSingleClusterRouting}, which takes the
     * profile and needs no seam at all.
     *
     * @param lookup reads the application profile's metadata
     * @throws Exception if the lookup fails with anything but a permission denial
     */
    @VisibleForTesting
    void validateSingleClusterAppProfile(Callable<AppProfile> lookup) throws Exception {
        try {
            checkSingleClusterRouting(lookup.call(), appProfileId);
        } catch (PermissionDeniedException e) {
            // A data-plane-only principal may be able to stream changes without reading app-profile
            // metadata. The ReadChangeStream failure is translated by the reader if the profile is
            // actually multi-cluster; preflight validates only metadata this principal can see.
            LOG.warn(
                    "Cannot preflight application profile {} because its metadata is not readable;"
                            + " Bigtable will validate that it uses single-cluster routing.",
                    appProfileId);
        }
    }

    /**
     * Rejects a profile whose routing policy Change Streams cannot be read through.
     *
     * <p>Anything but single-cluster routing is refused, including a routing policy this client's
     * protobuf does not know, which arrives with no routing case set.
     */
    static void checkSingleClusterRouting(AppProfile profile, String appProfileId) {
        Preconditions.checkArgument(
                profile.hasSingleClusterRouting(),
                "Bigtable Change Streams requires a single-cluster application profile, but app"
                        + " profile '%s' does not use single-cluster routing.",
                appProfileId);
    }

    @Override
    public Duration retention() throws Exception {
        Table description =
                runner.call(
                        this::tableAdminSettingsBuilder,
                        settings -> BigtableClients.openTableAdmin(settings, null),
                        client -> client.getTableCallable().futureCall(tableRequest()));
        return retentionOf(description, table);
    }

    /**
     * The table read the retention comes from: the schema view, which carries the change-stream
     * configuration and is the view the client's model method read through.
     */
    GetTableRequest tableRequest() {
        return GetTableRequest.newBuilder()
                .setName(
                        TableName.of(table.getProject(), table.getInstance(), table.getTable())
                                .toString())
                .setView(Table.View.SCHEMA_VIEW)
                .build();
    }

    /** Converts the table's retention, rejecting a table that has no change stream enabled. */
    static Duration retentionOf(Table description, TableDestination table) {
        Preconditions.checkState(
                description.hasChangeStreamConfig(),
                "Bigtable Change Streams is not enabled on %s; enable a change stream before"
                        + " starting this source.",
                table);
        com.google.protobuf.Duration retention =
                description.getChangeStreamConfig().getRetentionPeriod();
        return Duration.ofSeconds(retention.getSeconds(), retention.getNanos());
    }

    @Override
    public List<ByteStringRange> generateInitialPartitions() throws Exception {
        List<ByteStringRange> discovered =
                runner.call(
                        this::dataSettingsBuilder,
                        BigtableClients::openData,
                        client ->
                                client.generateInitialChangeStreamPartitionsCallable()
                                        .all()
                                        .futureCall(table.getTable()));
        return foldInitialPartitions(discovered, table);
    }

    /**
     * Folds the empty-key bounds the service sends, and rejects an empty response.
     *
     * <p>{@code GenerateInitialChangeStreamPartitionsUserCallable} hands every partition to {@code
     * ByteStringRange.create(start_key_closed, end_key_open)}, which — unlike the {@code
     * startClosed}/{@code endOpen} setters — leaves an empty key as a bounded one, so a table's
     * first partition arrives closed at the empty key and its last open at it. {@code
     * RowRanges.copyAll} rebuilds through the setters, which is what folds it. This is the only
     * place that can do it for a service partition.
     */
    static List<ByteStringRange> foldInitialPartitions(
            List<ByteStringRange> discovered, TableDestination table) {
        List<ByteStringRange> partitions = RowRanges.copyAll(discovered);
        Preconditions.checkState(
                !partitions.isEmpty(),
                "Bigtable returned no initial Change Streams partitions for %s.",
                table);
        return partitions;
    }

    /** Builds the data client's settings; public so the source's tests can read them. */
    @VisibleForTesting
    public BigtableDataSettings dataSettings() throws IOException {
        return dataSettingsBuilder().build();
    }

    private BigtableDataSettings.Builder dataSettingsBuilder() {
        BigtableDataSettings.Builder settings =
                BigtableDataClients.settings(table, appProfileId, null, credentials);
        BigtableDataClients.applyClientMetrics(settings, clientMetrics, false);
        return settings;
    }

    @Override
    public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
        this.clientMetrics = clientMetrics;
    }

    @VisibleForTesting
    BaseBigtableTableAdminSettings tableAdminSettings() throws IOException {
        return tableAdminSettingsBuilder().build();
    }

    private BaseBigtableTableAdminSettings.Builder tableAdminSettingsBuilder() {
        return BigtableClients.tableAdminSettings(null, credentials);
    }

    @VisibleForTesting
    BaseBigtableInstanceAdminSettings instanceAdminSettings() throws IOException {
        return instanceAdminSettingsBuilder().build();
    }

    private BaseBigtableInstanceAdminSettings.Builder instanceAdminSettingsBuilder() {
        return BigtableClients.instanceAdminSettings(null, credentials);
    }

    /** Refuses later calls, cancels the one in flight and closes its client. */
    @Override
    public void close() {
        // Closed first: a call that has not opened its client by now is refused, so none can
        // build settings from the credentials cleared below.
        runner.close();
        credentials = null;
        clientMetrics = null;
    }
}
