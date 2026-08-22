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

/** Native java-bigtable implementation of the coordinator operations. */
@Internal
public final class DefaultChangeStreamCoordinatorClient implements ChangeStreamCoordinatorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultChangeStreamCoordinatorClient.class);

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final String appProfileId;

    @Nullable private transient BigtableDataClient dataClient;
    @Nullable private transient BigtableTableAdminClient tableAdminClient;
    @Nullable private transient BigtableInstanceAdminClient instanceAdminClient;
    @Nullable private transient CredentialsProvider credentials;

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

    private BigtableDataClient dataClient() throws Exception {
        if (dataClient == null) {
            dataClient = BigtableDataClient.create(dataSettings());
        }
        return dataClient;
    }

    private BigtableTableAdminClient tableAdmin() throws Exception {
        if (tableAdminClient == null) {
            tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings());
        }
        return tableAdminClient;
    }

    private BigtableInstanceAdminClient instanceAdmin() throws Exception {
        if (instanceAdminClient == null) {
            instanceAdminClient = BigtableInstanceAdminClient.create(instanceAdminSettings());
        }
        return instanceAdminClient;
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
        Closers.closeAll(dataClient, tableAdminClient, instanceAdminClient);
        dataClient = null;
        tableAdminClient = null;
        instanceAdminClient = null;
        credentials = null;
    }
}
