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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.DefaultSpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.DefaultSpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Builds a {@link SpannerChangeStreamSource}. */
@PublicEvolving
public final class SpannerChangeStreamSourceBuilder<T> {

    public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(2);
    public static final Duration DEFAULT_ABSENT_RETENTION_FALLBACK = Duration.ofDays(7);
    public static final int DEFAULT_MAX_CONCURRENT_QUERIES_PER_SUBTASK = 8;

    private static final Duration MIN_HEARTBEAT_INTERVAL = Duration.ofSeconds(1);
    private static final Duration MAX_HEARTBEAT_INTERVAL = Duration.ofMinutes(5);

    @Nullable private SpannerDatabase database;
    @Nullable private String changeStreamName;
    @Nullable private SpannerChangeStreamDeserializationSchema<T> deserializer;
    private StartPosition startPosition = StartPosition.latest();
    private Optional<StartPosition> resumeFallback = Optional.empty();
    private Duration absentRetentionFallback = DEFAULT_ABSENT_RETENTION_FALLBACK;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private SpannerRpcPriority rpcPriority = SpannerRpcPriority.HIGH;
    private int maxConcurrentQueriesPerSubtask = DEFAULT_MAX_CONCURRENT_QUERIES_PER_SUBTASK;
    @Nullable private Instant endTimestamp;
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    @Nullable private SpannerChangeStreamCoordinatorClientFactory coordinatorClientFactory;
    @Nullable private SpannerChangeStreamQueryClientFactory queryClientFactory;

    SpannerChangeStreamSourceBuilder() {}

    public SpannerChangeStreamSourceBuilder<T> database(SpannerDatabase database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    public SpannerChangeStreamSourceBuilder<T> changeStreamName(String changeStreamName) {
        Preconditions.checkNotNull(changeStreamName, "changeStreamName must not be null");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(changeStreamName),
                "changeStreamName must not be blank");
        Preconditions.checkArgument(
                changeStreamName.equals(changeStreamName.trim()),
                "changeStreamName must not have leading or trailing whitespace");
        this.changeStreamName = changeStreamName;
        return this;
    }

    public SpannerChangeStreamSourceBuilder<T> deserializer(
            SpannerChangeStreamDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    public SpannerChangeStreamSourceBuilder<T> startPosition(StartPosition startPosition) {
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        return this;
    }

    public SpannerChangeStreamSourceBuilder<T> resumeFallback(StartPosition resumeFallback) {
        this.resumeFallback =
                Optional.of(
                        Preconditions.checkNotNull(
                                resumeFallback, "resumeFallback must not be null"));
        return this;
    }

    /**
     * Sets the retention used when {@code INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS} has no explicit
     * retention row. The default is seven days. It must be longer than the one-minute retention
     * safety margin used when resolving the earliest readable timestamp.
     */
    public SpannerChangeStreamSourceBuilder<T> absentRetentionFallback(Duration fallback) {
        Preconditions.checkNotNull(fallback, "fallback must not be null");
        StartPositionResolver.validateRetention(fallback, "fallback");
        this.absentRetentionFallback = fallback;
        return this;
    }

    /** Sets the service heartbeat interval, from one second through five minutes. */
    public SpannerChangeStreamSourceBuilder<T> heartbeatInterval(Duration interval) {
        Preconditions.checkNotNull(interval, "interval must not be null");
        Preconditions.checkArgument(
                interval.compareTo(MIN_HEARTBEAT_INTERVAL) >= 0
                        && interval.compareTo(MAX_HEARTBEAT_INTERVAL) <= 0,
                "interval must be between %s and %s, but was %s",
                MIN_HEARTBEAT_INTERVAL,
                MAX_HEARTBEAT_INTERVAL,
                interval);
        Preconditions.checkArgument(
                interval.toNanos() % 1_000_000 == 0,
                "interval must be expressible as whole milliseconds, but was %s",
                interval);
        this.heartbeatInterval = interval;
        return this;
    }

    public SpannerChangeStreamSourceBuilder<T> rpcPriority(SpannerRpcPriority rpcPriority) {
        this.rpcPriority = Preconditions.checkNotNull(rpcPriority, "rpcPriority must not be null");
        return this;
    }

    /**
     * Bounds the TVF partition queries opened concurrently by one source subtask.
     *
     * <p>The default is eight. Job-wide configured capacity is source parallelism multiplied by
     * this value; it is a connector bound, not a published Spanner quota.
     */
    public SpannerChangeStreamSourceBuilder<T> maxConcurrentQueriesPerSubtask(int maximum) {
        Preconditions.checkArgument(maximum > 0, "maximum must be positive, but was %s", maximum);
        this.maxConcurrentQueriesPerSubtask = maximum;
        return this;
    }

    public SpannerChangeStreamSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
        return this;
    }

    @VisibleForTesting
    SpannerChangeStreamSourceBuilder<T> coordinatorClientFactory(
            SpannerChangeStreamCoordinatorClientFactory factory) {
        this.coordinatorClientFactory = factory;
        return this;
    }

    @VisibleForTesting
    SpannerChangeStreamSourceBuilder<T> queryClientFactory(
            SpannerChangeStreamQueryClientFactory factory) {
        this.queryClientFactory = factory;
        return this;
    }

    @VisibleForTesting
    SpannerChangeStreamSourceBuilder<T> endTimestamp(Instant endTimestamp) {
        this.endTimestamp =
                Preconditions.checkNotNull(endTimestamp, "endTimestamp must not be null");
        return this;
    }

    public SpannerChangeStreamSource<T> build() {
        Preconditions.checkState(database != null, "A database is required: set database(...).");
        Preconditions.checkState(
                changeStreamName != null,
                "A change-stream name is required: set changeStreamName(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required: set deserializer(...).");
        SpannerChangeStreamCoordinatorClientFactory coordinatorFactory =
                coordinatorClientFactory != null
                        ? coordinatorClientFactory
                        : new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                database,
                                changeStreamName,
                                absentRetentionFallback,
                                emulatorEndpoint);
        SpannerChangeStreamQueryClientFactory readerFactory =
                queryClientFactory != null
                        ? queryClientFactory
                        : new DefaultSpannerChangeStreamQueryClientFactory(
                                database,
                                changeStreamName,
                                rpcPriority,
                                maxConcurrentQueriesPerSubtask,
                                emulatorEndpoint);
        return new SpannerChangeStreamSource<>(
                new SpannerChangeStreamSourceConfig<>(
                        database,
                        changeStreamName,
                        deserializer,
                        startPosition,
                        resumeFallback.orElse(null),
                        absentRetentionFallback,
                        heartbeatInterval.toMillis(),
                        rpcPriority,
                        maxConcurrentQueriesPerSubtask,
                        endTimestamp,
                        coordinatorFactory,
                        readerFactory));
    }
}
