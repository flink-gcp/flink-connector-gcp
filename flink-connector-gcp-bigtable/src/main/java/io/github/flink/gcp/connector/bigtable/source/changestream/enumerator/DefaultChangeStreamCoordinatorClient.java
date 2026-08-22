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
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Native java-bigtable implementation of the coordinator operations.
 *
 * <p>One client belongs to one enumerator: {@link DefaultChangeStreamCoordinatorClientFactory}
 * mints it, and the source mints one per {@code createEnumerator} and {@code restoreEnumerator}
 * ({@code docs/adr/0128}). The three client families are built on first use, so minting one opens
 * nothing.
 *
 * <p><b>What that does not settle.</b> Per-enumerator ownership removes the sharing between
 * enumerators; it does not order this object's own two threads. The enumerator's reconciliation
 * scan runs on the executor {@code SplitEnumeratorContext#callAsync} hands the work to, on an
 * interval, while {@link #close()} runs on the coordinator thread — and {@code
 * SourceCoordinator.close()} closes the enumerator before it shuts that executor down.
 *
 * <p>Building a client and closing one are therefore guarded by this object's monitor, and a
 * one-way {@code closed} flag makes a scan that overtakes the teardown refuse rather than build.
 * {@code volatile} alone would not have done it: the lazy accessors are a check-then-create, so a
 * teardown that ran between the check and the assignment saw {@code null}, closed nothing, and left
 * the client the scan then assigned owned by no one — a leaked JobManager-side channel and
 * executor, and with the credentials already nulled, one reaching Bigtable as the process's
 * application default credentials. The flag is one-way because this client belongs to one
 * enumerator ({@code docs/adr/0128}) and ends with it.
 *
 * <p>The monitor is held across the client's construction, as the module's other lazy holder does,
 * so a teardown racing a first use waits for it rather than racing it. Nothing here makes an RPC
 * under the monitor.
 */
@Internal
public final class DefaultChangeStreamCoordinatorClient implements ChangeStreamCoordinatorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultChangeStreamCoordinatorClient.class);

    private final TableDestination table;
    private final String appProfileId;

    private volatile boolean closed;

    @Nullable private volatile BigtableDataClient dataClient;
    @Nullable private volatile BigtableTableAdminClient tableAdminClient;
    @Nullable private volatile BigtableInstanceAdminClient instanceAdminClient;
    @Nullable private volatile CredentialsProvider credentials;

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
    }

    @Override
    public void validateSingleClusterAppProfile() throws Exception {
        validateSingleClusterAppProfile(
                () -> instanceAdmin().getAppProfile(table.getInstance(), appProfileId));
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

    /** Rejects a profile whose routing policy Change Streams cannot be read through. */
    static void checkSingleClusterRouting(AppProfile profile, String appProfileId) {
        Preconditions.checkArgument(
                profile.getPolicy() instanceof AppProfile.SingleClusterRoutingPolicy,
                "Bigtable Change Streams requires a single-cluster application profile, but app"
                        + " profile '%s' uses multi-cluster routing.",
                appProfileId);
    }

    @Override
    public Duration retention() throws Exception {
        return retentionOf(tableAdmin().getTable(table.getTable()), table);
    }

    /** Converts the client's retention, rejecting a table that has no change stream enabled. */
    static Duration retentionOf(Table description, TableDestination table) {
        org.threeten.bp.Duration clientRetention = description.getChangeStreamRetention();
        Preconditions.checkState(
                clientRetention != null,
                "Bigtable Change Streams is not enabled on %s; enable a change stream before"
                        + " starting this source.",
                table);
        return Duration.ofSeconds(clientRetention.getSeconds(), clientRetention.getNano());
    }

    @Override
    public List<ByteStringRange> generateInitialPartitions() throws Exception {
        List<ByteStringRange> discovered = new ArrayList<>();
        dataClient()
                .generateInitialChangeStreamPartitions(table.getTable())
                .forEach(discovered::add);
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

    private synchronized BigtableDataClient dataClient() throws Exception {
        checkOpen();
        if (dataClient == null) {
            dataClient = BigtableDataClient.create(dataSettings());
        }
        return dataClient;
    }

    private synchronized BigtableTableAdminClient tableAdmin() throws Exception {
        checkOpen();
        if (tableAdminClient == null) {
            tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings());
        }
        return tableAdminClient;
    }

    private synchronized BigtableInstanceAdminClient instanceAdmin() throws Exception {
        checkOpen();
        if (instanceAdminClient == null) {
            instanceAdminClient = BigtableInstanceAdminClient.create(instanceAdminSettings());
        }
        return instanceAdminClient;
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException(
                    "The Bigtable Change Streams coordinator for "
                            + table
                            + " was closed before it was used.");
        }
    }

    BigtableDataSettings dataSettings() throws IOException {
        return BigtableDataClients.settings(table, appProfileId, null, credentials).build();
    }

    BigtableTableAdminSettings tableAdminSettings() throws IOException {
        BigtableTableAdminSettings.Builder settings =
                BigtableTableAdminSettings.newBuilder()
                        .setProjectId(table.getProject())
                        .setInstanceId(table.getInstance());
        if (credentials != null) {
            settings.setCredentialsProvider(credentials);
        }
        return settings.build();
    }

    BigtableInstanceAdminSettings instanceAdminSettings() throws IOException {
        BigtableInstanceAdminSettings.Builder settings =
                BigtableInstanceAdminSettings.newBuilder().setProjectId(table.getProject());
        if (credentials != null) {
            settings.setCredentialsProvider(credentials);
        }
        return settings.build();
    }

    @Override
    public void close() throws Exception {
        BigtableDataClient data;
        BigtableTableAdminClient tableAdmin;
        BigtableInstanceAdminClient instanceAdmin;
        synchronized (this) {
            closed = true;
            data = dataClient;
            tableAdmin = tableAdminClient;
            instanceAdmin = instanceAdminClient;
            dataClient = null;
            tableAdminClient = null;
            instanceAdminClient = null;
            credentials = null;
        }
        // Released outside the monitor: a lazy build in flight holds it, and the flag above has
        // already stopped anything new, so waiting here would only delay a teardown.
        Closers.closeAll(data, tableAdmin, instanceAdmin);
    }
}
