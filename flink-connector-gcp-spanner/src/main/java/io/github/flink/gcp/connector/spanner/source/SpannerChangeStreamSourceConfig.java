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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Immutable configuration assembled by {@link SpannerChangeStreamSourceBuilder}. */
@Internal
public final class SpannerChangeStreamSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    private final String changeStreamName;
    private final SpannerChangeStreamDeserializationSchema<T> deserializer;
    private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    private final Duration absentRetentionFallback;
    private final long heartbeatMillis;
    private final SpannerRpcPriority rpcPriority;
    private final int maxConcurrentQueriesPerSubtask;
    @Nullable private final String serviceAccountKeyFile;
    private final SpannerChangeStreamRecordFilter recordFilter;
    @Nullable private final Instant endTimestamp;
    private final SpannerChangeStreamCoordinatorClientFactory coordinatorClientFactory;
    private final SpannerChangeStreamQueryClientFactory queryClientFactory;

    SpannerChangeStreamSourceConfig(
            SpannerDatabase database,
            String changeStreamName,
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            Duration absentRetentionFallback,
            long heartbeatMillis,
            SpannerRpcPriority rpcPriority,
            int maxConcurrentQueriesPerSubtask,
            @Nullable String serviceAccountKeyFile,
            SpannerChangeStreamRecordFilter recordFilter,
            @Nullable Instant endTimestamp,
            SpannerChangeStreamCoordinatorClientFactory coordinatorClientFactory,
            SpannerChangeStreamQueryClientFactory queryClientFactory) {
        this.database = database;
        this.changeStreamName = changeStreamName;
        this.deserializer = deserializer;
        this.startPosition = startPosition;
        this.resumeFallback = resumeFallback;
        this.absentRetentionFallback = absentRetentionFallback;
        this.heartbeatMillis = heartbeatMillis;
        this.rpcPriority = rpcPriority;
        this.maxConcurrentQueriesPerSubtask = maxConcurrentQueriesPerSubtask;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.recordFilter = recordFilter;
        this.endTimestamp = endTimestamp;
        this.coordinatorClientFactory = coordinatorClientFactory;
        this.queryClientFactory = queryClientFactory;
    }

    public SpannerDatabase getDatabase() {
        return database;
    }

    public SpannerChangeStreamDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    public String getChangeStreamName() {
        return changeStreamName;
    }

    public StartPosition getStartPosition() {
        return startPosition;
    }

    public Optional<StartPosition> getResumeFallback() {
        return Optional.ofNullable(resumeFallback);
    }

    public Duration getAbsentRetentionFallback() {
        return absentRetentionFallback;
    }

    public long getHeartbeatMillis() {
        return heartbeatMillis;
    }

    public SpannerRpcPriority getRpcPriority() {
        return rpcPriority;
    }

    public int getMaxConcurrentQueriesPerSubtask() {
        return maxConcurrentQueriesPerSubtask;
    }

    public Optional<String> getServiceAccountKeyFile() {
        return Optional.ofNullable(serviceAccountKeyFile);
    }

    public SpannerChangeStreamRecordFilter getRecordFilter() {
        return recordFilter;
    }

    @Nullable
    public Instant getEndTimestamp() {
        return endTimestamp;
    }

    public SpannerChangeStreamQueryClientFactory getQueryClientFactory() {
        return queryClientFactory;
    }

    public SpannerChangeStreamCoordinatorClientFactory getCoordinatorClientFactory() {
        return coordinatorClientFactory;
    }
}
