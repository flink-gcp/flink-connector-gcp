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

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;

/** Serializable configuration for a Bigtable Change Streams source. */
@Internal
public final class BigtableChangeStreamSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    final TableDestination table;
    final BigtableChangeStreamDeserializationSchema<T> deserializer;
    final String appProfileId;
    @Nullable final String serviceAccountKeyFile;
    final StartPosition startPosition;
    @Nullable final StartPosition resumeFallback;
    @Nullable final Instant endTime;
    final int maxConcurrentStreamsPerSubtask;
    final ChangeStreamOpener opener;
    final ChangeStreamRestoreResolver restoreResolver;
    @Nullable final ChangeStreamCoordinatorClient coordinatorClient;

    BigtableChangeStreamSourceConfig(
            TableDestination table,
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            String appProfileId,
            @Nullable String serviceAccountKeyFile,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable Instant endTime,
            int maxConcurrentStreamsPerSubtask,
            ChangeStreamOpener opener,
            ChangeStreamRestoreResolver restoreResolver,
            @Nullable ChangeStreamCoordinatorClient coordinatorClient) {
        this.table = table;
        this.deserializer = deserializer;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.startPosition = startPosition;
        this.resumeFallback = resumeFallback.orElse(null);
        this.endTime = endTime;
        this.maxConcurrentStreamsPerSubtask = maxConcurrentStreamsPerSubtask;
        this.opener = opener;
        this.restoreResolver = restoreResolver;
        this.coordinatorClient = coordinatorClient;
    }

    public TableDestination getTable() {
        return table;
    }

    public BigtableChangeStreamDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /** Returns the service-account key-file path, or {@code null} to use ADC. */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Nullable
    public Instant getEndTime() {
        return endTime;
    }

    public int getMaxConcurrentStreamsPerSubtask() {
        return maxConcurrentStreamsPerSubtask;
    }

    public ChangeStreamOpener getOpener() {
        return opener;
    }

    public Optional<StartPosition> getResumeFallback() {
        return Optional.ofNullable(resumeFallback);
    }

    public ChangeStreamRestoreResolver getRestoreResolver() {
        return restoreResolver;
    }
}
