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
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Retention-aware reader restore resolution backed by Bigtable table metadata.
 *
 * <p>Reads that metadata through a table-admin client, which is why the reader that owns this loads
 * one provider scoped for data <em>and</em> table admin and hands the same one to the stream
 * opener: two loads would produce two providers, each scoped for half of what the reader does.
 */
@Internal
public final class DefaultChangeStreamRestoreResolver implements ChangeStreamRestoreResolver {

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final String appProfileId;
    @Nullable private transient StartPositionResolver resolver;
    @Nullable private transient CredentialsProvider credentials;

    public DefaultChangeStreamRestoreResolver(TableDestination table, String appProfileId) {
        this.table = table;
        this.appProfileId = appProfileId;
    }

    @Override
    public ChangeStreamPartitionSplit resolve(
            ChangeStreamPartitionSplit split, @Nullable StartPosition fallback) throws Exception {
        if (resolver == null) {
            try (DefaultChangeStreamCoordinatorClient client =
                    new DefaultChangeStreamCoordinatorClient(table, appProfileId, credentials)) {
                Duration retention = client.retention();
                resolver = StartPositionResolver.create(getClass(), () -> retention);
            }
        }
        Optional<Instant> restart =
                resolver.resolveRestored(
                        RowRanges.format(split.getPartition()), split.getLowWatermark(), fallback);
        return restart.map(split::restartAt).orElse(split);
    }

    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {
        this.credentials = credentials;
    }
}
