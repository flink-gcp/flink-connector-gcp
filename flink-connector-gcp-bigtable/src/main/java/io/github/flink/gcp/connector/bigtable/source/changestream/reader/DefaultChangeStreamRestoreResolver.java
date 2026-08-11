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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import javax.annotation.Nullable;

import java.util.Optional;

/** Retention-aware reader restore resolution backed by Bigtable table metadata. */
@Internal
public final class DefaultChangeStreamRestoreResolver implements ChangeStreamRestoreResolver {

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final String appProfileId;
    @Nullable private transient StartPositionResolver resolver;

    public DefaultChangeStreamRestoreResolver(TableDestination table, String appProfileId) {
        this.table = table;
        this.appProfileId = appProfileId;
    }

    @Override
    public ChangeStreamPartitionSplit resolve(
            ChangeStreamPartitionSplit split, Optional<StartPosition> fallback) throws Exception {
        if (resolver == null) {
            try (DefaultChangeStreamCoordinatorClient client =
                    new DefaultChangeStreamCoordinatorClient(table, appProfileId)) {
                java.time.Duration retention = client.retention();
                resolver = StartPositionResolver.create(getClass(), () -> retention);
            }
        }
        Optional<java.time.Instant> restart =
                resolver.resolveRestored(
                        RowRanges.format(split.getPartition()), split.getLowWatermark(), fallback);
        return restart.map(split::restartAt).orElse(split);
    }
}
