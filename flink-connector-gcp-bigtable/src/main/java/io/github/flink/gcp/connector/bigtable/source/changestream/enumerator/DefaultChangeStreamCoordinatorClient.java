/*
 * Copyright 2026 laughingman7743
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
import org.apache.flink.util.Preconditions;

import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Native java-bigtable implementation of the coordinator operations. */
@Internal
public final class DefaultChangeStreamCoordinatorClient implements ChangeStreamCoordinatorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultChangeStreamCoordinatorClient.class);

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final String appProfileId;
    @Nullable private final transient Operations testOperations;

    @Nullable private transient BigtableDataClient dataClient;
    @Nullable private transient BigtableTableAdminClient tableAdminClient;
    @Nullable private transient BigtableInstanceAdminClient instanceAdminClient;

    public DefaultChangeStreamCoordinatorClient(TableDestination table, String appProfileId) {
        this(table, appProfileId, null);
    }

    DefaultChangeStreamCoordinatorClient(
            TableDestination table, String appProfileId, @Nullable Operations testOperations) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.appProfileId =
                Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        Preconditions.checkArgument(!appProfileId.isEmpty(), "appProfileId must not be empty");
        this.testOperations = testOperations;
    }

    @Override
    public void validateSingleClusterAppProfile() throws Exception {
        try {
            AppProfile profile =
                    testOperations == null
                            ? instanceAdmin().getAppProfile(table.getInstance(), appProfileId)
                            : testOperations.getAppProfile();
            Preconditions.checkArgument(
                    profile.getPolicy() instanceof AppProfile.SingleClusterRoutingPolicy,
                    "Bigtable Change Streams requires a single-cluster application profile, but"
                            + " app profile '%s' uses multi-cluster routing.",
                    appProfileId);
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

    @Override
    public Duration retention() throws Exception {
        Table description =
                testOperations == null
                        ? tableAdmin().getTable(table.getTable())
                        : testOperations.getTable();
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
        List<ByteStringRange> partitions = new ArrayList<>();
        if (testOperations == null) {
            dataClient()
                    .generateInitialChangeStreamPartitions(table.getTable())
                    .forEach(partitions::add);
        } else {
            partitions.addAll(testOperations.generateInitialPartitions());
        }
        Preconditions.checkState(
                !partitions.isEmpty(),
                "Bigtable returned no initial Change Streams partitions for %s.",
                table);
        return partitions;
    }

    private BigtableDataClient dataClient() throws Exception {
        if (dataClient == null) {
            dataClient =
                    BigtableDataClient.create(
                            BigtableDataClients.settings(table, appProfileId, null).build());
        }
        return dataClient;
    }

    private BigtableTableAdminClient tableAdmin() throws Exception {
        if (tableAdminClient == null) {
            tableAdminClient =
                    BigtableTableAdminClient.create(table.getProject(), table.getInstance());
        }
        return tableAdminClient;
    }

    private BigtableInstanceAdminClient instanceAdmin() throws Exception {
        if (instanceAdminClient == null) {
            instanceAdminClient = BigtableInstanceAdminClient.create(table.getProject());
        }
        return instanceAdminClient;
    }

    @Override
    public void close() throws Exception {
        if (testOperations == null) {
            Closers.closeAll(dataClient, tableAdminClient, instanceAdminClient);
        } else {
            testOperations.close();
        }
        dataClient = null;
        tableAdminClient = null;
        instanceAdminClient = null;
    }

    interface Operations extends AutoCloseable {

        AppProfile getAppProfile() throws Exception;

        Table getTable() throws Exception;

        List<ByteStringRange> generateInitialPartitions() throws Exception;
    }
}
