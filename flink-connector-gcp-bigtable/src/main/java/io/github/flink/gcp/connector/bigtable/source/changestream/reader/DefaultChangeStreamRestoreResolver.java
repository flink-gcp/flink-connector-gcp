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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.CredentialsProvider;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Retention-aware reader restore resolution backed by Bigtable table metadata. */
@Internal
public final class DefaultChangeStreamRestoreResolver implements ChangeStreamRestoreResolver {

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private transient StartPositionResolver resolver;
    @Nullable private transient CredentialsProvider credentialsOverride;

    public DefaultChangeStreamRestoreResolver(TableDestination table, String appProfileId) {
        this(table, appProfileId, null);
    }

    public DefaultChangeStreamRestoreResolver(
            TableDestination table, String appProfileId, @Nullable String serviceAccountKeyFile) {
        this.table = table;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    @Override
    public ChangeStreamPartitionSplit resolve(
            ChangeStreamPartitionSplit split, @Nullable StartPosition fallback) throws Exception {
        if (resolver == null) {
            try (DefaultChangeStreamCoordinatorClient client =
                    new DefaultChangeStreamCoordinatorClient(
                            table, appProfileId, serviceAccountKeyFile, credentialsOverride)) {
                Duration retention = client.retention();
                resolver = StartPositionResolver.create(getClass(), () -> retention);
            }
        }
        Optional<Instant> restart =
                resolver.resolveRestored(
                        RowRanges.format(split.getPartition()), split.getLowWatermark(), fallback);
        return restart.map(split::restartAt).orElse(split);
    }

    /** Supplies the provider loaded when the TaskManager creates the source reader. */
    public void setCredentialsOverride(@Nullable CredentialsProvider credentialsOverride) {
        this.credentialsOverride = credentialsOverride;
    }
}
