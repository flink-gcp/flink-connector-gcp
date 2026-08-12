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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.DataClientChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.DefaultChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Optional;

/** Builds a {@link BigtableChangeStreamSource}. */
@PublicEvolving
public final class BigtableChangeStreamSourceBuilder<T> {

    @Nullable private TableDestination table;
    @Nullable private BigtableChangeStreamDeserializationSchema<T> deserializer;
    @Nullable private String appProfileId;
    @Nullable private String serviceAccountKeyFile;
    private StartPosition startPosition = StartPosition.latest();
    private Optional<StartPosition> resumeFallback = Optional.empty();
    @Nullable private Instant endTime;
    @Nullable private ChangeStreamOpener opener;
    @Nullable private ChangeStreamRestoreResolver restoreResolver;

    BigtableChangeStreamSourceBuilder() {}

    public BigtableChangeStreamSourceBuilder<T> table(TableDestination table) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        return this;
    }

    public BigtableChangeStreamSourceBuilder<T> deserializer(
            BigtableChangeStreamDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    public BigtableChangeStreamSourceBuilder<T> appProfileId(String appProfileId) {
        Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        Preconditions.checkArgument(
                !appProfileId.trim().isEmpty(), "appProfileId must not be blank");
        this.appProfileId = appProfileId;
        return this;
    }

    /**
     * Authenticates Change Streams with the service-account JSON key at the given path instead of
     * application-default credentials. The JobManager reads it when a fresh or restored coordinator
     * starts. Each TaskManager reads it when a reader first opens a stream or resolves an expired
     * restored split. Every eligible process must therefore see the same path.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public BigtableChangeStreamSourceBuilder<T> serviceAccountKeyFile(
            String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    public BigtableChangeStreamSourceBuilder<T> startPosition(StartPosition startPosition) {
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        return this;
    }

    public BigtableChangeStreamSourceBuilder<T> resumeFallback(StartPosition resumeFallback) {
        this.resumeFallback =
                Optional.of(
                        Preconditions.checkNotNull(
                                resumeFallback, "resumeFallback must not be null"));
        return this;
    }

    public BigtableChangeStreamSourceBuilder<T> endTime(Instant endTime) {
        this.endTime = Preconditions.checkNotNull(endTime, "endTime must not be null");
        return this;
    }

    @VisibleForTesting
    BigtableChangeStreamSourceBuilder<T> opener(ChangeStreamOpener opener) {
        this.opener = opener;
        return this;
    }

    @VisibleForTesting
    BigtableChangeStreamSourceBuilder<T> restoreResolver(
            ChangeStreamRestoreResolver restoreResolver) {
        this.restoreResolver = restoreResolver;
        return this;
    }

    public BigtableChangeStreamSource<T> build() {
        Preconditions.checkState(table != null, "A table is required: set table(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required: set deserializer(...).");
        Preconditions.checkState(
                appProfileId != null, "An app profile is required: set appProfileId(...).");
        return new BigtableChangeStreamSource<>(
                new BigtableChangeStreamSourceConfig<>(
                        table,
                        deserializer,
                        appProfileId,
                        serviceAccountKeyFile,
                        startPosition,
                        resumeFallback,
                        endTime,
                        opener != null
                                ? opener
                                : new DataClientChangeStreamOpener(
                                        appProfileId, serviceAccountKeyFile),
                        restoreResolver != null
                                ? restoreResolver
                                : new DefaultChangeStreamRestoreResolver(
                                        table, appProfileId, serviceAccountKeyFile)));
    }
}
