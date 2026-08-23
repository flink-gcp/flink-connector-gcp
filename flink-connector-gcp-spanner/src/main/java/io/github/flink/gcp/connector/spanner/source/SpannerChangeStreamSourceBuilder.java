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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.DefaultSpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.DefaultSpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Builds a {@link SpannerChangeStreamSource}. */
@Public
public final class SpannerChangeStreamSourceBuilder<T> {

    public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(2);
    public static final Duration DEFAULT_ABSENT_RETENTION_FALLBACK = Duration.ofDays(7);
    public static final int DEFAULT_MAX_CONCURRENT_QUERIES_PER_SUBTASK = 8;

    private static final Duration MIN_HEARTBEAT_INTERVAL = Duration.ofSeconds(1);
    private static final Duration MAX_HEARTBEAT_INTERVAL = Duration.ofMinutes(5);

    @Nullable private DatabaseDestination database;
    @Nullable private String changeStreamName;
    @Nullable private SpannerChangeStreamDeserializationSchema<T> deserializer;
    private StartPosition startPosition = StartPosition.latest();
    @Nullable private StartPosition resumeFallback;
    private Duration absentRetentionFallback = DEFAULT_ABSENT_RETENTION_FALLBACK;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private SpannerRpcPriority rpcPriority = SpannerRpcPriority.HIGH;
    private int maxConcurrentQueriesPerSubtask = DEFAULT_MAX_CONCURRENT_QUERIES_PER_SUBTASK;
    private List<Pattern> tableIncludeList = Collections.emptyList();
    private List<Pattern> tableExcludeList = Collections.emptyList();
    private List<Pattern> columnIncludeList = Collections.emptyList();
    private List<Pattern> columnExcludeList = Collections.emptyList();
    private boolean skipMessagesWithoutChange;
    @Nullable private String serviceAccountKeyFile;
    @Nullable private Instant endTimestamp;
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    @Nullable private SpannerChangeStreamCoordinatorClientFactory coordinatorClientFactory;
    @Nullable private SpannerChangeStreamQueryClientFactory queryClientFactory;

    SpannerChangeStreamSourceBuilder() {}

    /**
     * Sets the database holding the change stream. Required.
     *
     * @param database the database
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> database(DatabaseDestination database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets the change stream to read. Required.
     *
     * <p>The name is the one the {@code CREATE CHANGE STREAM} statement gave it; the source calls
     * the read function Spanner generates for it.
     *
     * @param changeStreamName the change stream name, not blank and without surrounding whitespace
     * @return this builder
     */
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

    /**
     * Sets the schema turning each data-change record into output records. Required.
     *
     * <p>Zero or more outputs per record, collected synchronously; see the schema's own
     * documentation for what emitting nothing means.
     *
     * @param deserializer the deserialization schema
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> deserializer(
            SpannerChangeStreamDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Sets where a fresh stream begins. Optional; defaults to {@link StartPosition#latest()}.
     *
     * <p>Resolved once, on the coordinator. It is ignored on a restore, where the checkpointed
     * partition ledger decides — see {@link #resumeFallback(StartPosition)} for what happens when
     * that ledger has expired.
     *
     * @param startPosition the start position
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> startPosition(StartPosition startPosition) {
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        return this;
    }

    /**
     * Sets where to restart when a restored partition position has fallen outside the stream's
     * retention. Optional; unset means such a restore <em>fails</em>.
     *
     * <p>Setting it is a decision to lose data rather than stop: the coordinator discards the whole
     * checkpointed ledger and starts one new query from here, so the unavailable interval is never
     * read and records at or after this position can be delivered again.
     *
     * @param resumeFallback the fallback start position
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> resumeFallback(StartPosition resumeFallback) {
        this.resumeFallback =
                Preconditions.checkNotNull(resumeFallback, "resumeFallback must not be null");
        return this;
    }

    /**
     * Sets the retention used when {@code INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS} has no explicit
     * retention row. The default is seven days. It must be longer than the one-minute retention
     * safety margin used when resolving the earliest readable timestamp.
     *
     * @param fallback the retention to assume
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> absentRetentionFallback(Duration fallback) {
        Preconditions.checkNotNull(fallback, "absentRetentionFallback must not be null");
        StartPositionResolver.validateRetention(fallback, "absentRetentionFallback");
        this.absentRetentionFallback = fallback;
        return this;
    }

    /**
     * Sets the service heartbeat interval. Optional; defaults to two seconds.
     *
     * <p>Heartbeats are what advance a quiet partition's watermark, so this is the upper bound on
     * how long the source's event time can stand still while the stream is idle.
     *
     * @param interval the heartbeat interval, from one second through five minutes and expressible
     *     in whole milliseconds
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> heartbeatInterval(Duration interval) {
        Preconditions.checkNotNull(interval, "heartbeatInterval must not be null");
        Preconditions.checkArgument(
                interval.compareTo(MIN_HEARTBEAT_INTERVAL) >= 0
                        && interval.compareTo(MAX_HEARTBEAT_INTERVAL) <= 0,
                "heartbeatInterval must be between %s and %s, but was %s",
                MIN_HEARTBEAT_INTERVAL,
                MAX_HEARTBEAT_INTERVAL,
                interval);
        Preconditions.checkArgument(
                interval.toNanos() % 1_000_000 == 0,
                "heartbeatInterval must be expressible as whole milliseconds, but was %s",
                interval);
        this.heartbeatInterval = interval;
        return this;
    }

    /**
     * Sets the priority Spanner schedules the partition queries at. Optional; unset leaves the
     * service's own handling in place, which is the same as {@code HIGH}.
     *
     * <p>Applies to every partition query in both dialects. Change Streams queries do not use Data
     * Boost, so this is the only lever over what they cost the instance.
     *
     * @param rpcPriority the priority
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> rpcPriority(SpannerRpcPriority rpcPriority) {
        this.rpcPriority = Preconditions.checkNotNull(rpcPriority, "rpcPriority must not be null");
        return this;
    }

    /**
     * Bounds the TVF partition queries opened concurrently by one source subtask.
     *
     * <p>The default is eight. Job-wide configured capacity is source parallelism multiplied by
     * this value; it is a connector bound, not a published Spanner quota.
     *
     * @param maximum the per-subtask query bound, positive
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> maxConcurrentQueriesPerSubtask(int maximum) {
        Preconditions.checkArgument(
                maximum > 0,
                "maxConcurrentQueriesPerSubtask must be positive, but was %s",
                maximum);
        this.maxConcurrentQueriesPerSubtask = maximum;
        return this;
    }

    /**
     * Includes tables whose Spanner-reported names fully match at least one Java regular
     * expression.
     *
     * <p>An empty collection disables this filter. It is mutually exclusive with {@link
     * #tableExcludeList(Collection)}.
     *
     * @param patterns the Java regular expressions, each matching a complete table name
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> tableIncludeList(Collection<String> patterns) {
        this.tableIncludeList = compilePatterns(patterns, "tableIncludeList");
        return this;
    }

    /**
     * Excludes tables whose Spanner-reported names fully match at least one Java regular
     * expression.
     *
     * <p>An empty collection disables this filter. It is mutually exclusive with {@link
     * #tableIncludeList(Collection)}.
     *
     * @param patterns the Java regular expressions, each matching a complete table name
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> tableExcludeList(Collection<String> patterns) {
        this.tableExcludeList = compilePatterns(patterns, "tableExcludeList");
        return this;
    }

    /**
     * Includes non-key columns whose {@code table.column} identifiers fully match at least one Java
     * regular expression.
     *
     * <p>Primary-key columns are always retained. An empty collection disables this filter. It is
     * mutually exclusive with {@link #columnExcludeList(Collection)}.
     *
     * @param patterns the Java regular expressions, each matching a complete {@code table.column}
     *     identifier
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> columnIncludeList(Collection<String> patterns) {
        this.columnIncludeList = compilePatterns(patterns, "columnIncludeList");
        return this;
    }

    /**
     * Excludes non-key columns whose {@code table.column} identifiers fully match at least one Java
     * regular expression.
     *
     * <p>Primary-key columns are always retained. An empty collection disables this filter. It is
     * mutually exclusive with {@link #columnIncludeList(Collection)}.
     *
     * @param patterns the Java regular expressions, each matching a complete {@code table.column}
     *     identifier
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> columnExcludeList(Collection<String> patterns) {
        this.columnExcludeList = compilePatterns(patterns, "columnExcludeList");
        return this;
    }

    /**
     * Skips a data-change record when column filtering removes every non-key value it reported.
     *
     * <p>The default is {@code false}, which delivers the record with empty projected value objects
     * so that transaction activity remains visible.
     *
     * @param skip whether to skip such a record
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> skipMessagesWithoutChange(boolean skip) {
        this.skipMessagesWithoutChange = skip;
        return this;
    }

    /**
     * Authenticates the source with the service-account JSON key at the given path instead of
     * application-default credentials.
     *
     * <p>The JobManager reads the file when the coordinator initializes, and each TaskManager reads
     * it when its reader opens. Only the path is serialized. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel carries no credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public SpannerChangeStreamSourceBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the source at a Spanner emulator, over a plaintext channel with no credentials. Never
     * production.
     *
     * <p>The endpoint is parsed here rather than when the source connects, so a malformed value
     * fails where the job is assembled instead of on a TaskManager.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public SpannerChangeStreamSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
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
        Preconditions.checkState(
                tableIncludeList.isEmpty() || tableExcludeList.isEmpty(),
                "tableIncludeList(...) and tableExcludeList(...) must not both be set.");
        Preconditions.checkState(
                columnIncludeList.isEmpty() || columnExcludeList.isEmpty(),
                "columnIncludeList(...) and columnExcludeList(...) must not both be set.");
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        SpannerChangeStreamCoordinatorClientFactory coordinatorFactory =
                coordinatorClientFactory != null
                        ? coordinatorClientFactory
                        : new DefaultSpannerChangeStreamCoordinatorClientFactory(
                                database,
                                changeStreamName,
                                absentRetentionFallback,
                                emulatorEndpoint,
                                serviceAccountKeyFile);
        SpannerChangeStreamQueryClientFactory readerFactory =
                queryClientFactory != null
                        ? queryClientFactory
                        : new DefaultSpannerChangeStreamQueryClientFactory(
                                database,
                                changeStreamName,
                                rpcPriority,
                                maxConcurrentQueriesPerSubtask,
                                emulatorEndpoint,
                                serviceAccountKeyFile);
        return new SpannerChangeStreamSource<>(
                new SpannerChangeStreamSourceConfig<>(
                        database,
                        changeStreamName,
                        deserializer,
                        startPosition,
                        resumeFallback,
                        absentRetentionFallback,
                        heartbeatInterval.toMillis(),
                        rpcPriority,
                        maxConcurrentQueriesPerSubtask,
                        serviceAccountKeyFile,
                        new SpannerChangeStreamRecordFilter(
                                tableIncludeList,
                                tableExcludeList,
                                columnIncludeList,
                                columnExcludeList,
                                skipMessagesWithoutChange),
                        endTimestamp,
                        coordinatorFactory,
                        readerFactory));
    }

    private static List<Pattern> compilePatterns(Collection<String> patterns, String option) {
        Preconditions.checkNotNull(patterns, option + " must not be null");
        List<Pattern> compiled = new ArrayList<>(patterns.size());
        int index = 0;
        for (String pattern : patterns) {
            Preconditions.checkNotNull(pattern, option + " must not contain null");
            try {
                compiled.add(Pattern.compile(pattern));
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(
                        option
                                + " pattern at index "
                                + index
                                + " is invalid: "
                                + e.getDescription(),
                        e);
            }
            index++;
        }
        return Collections.unmodifiableList(compiled);
    }
}
