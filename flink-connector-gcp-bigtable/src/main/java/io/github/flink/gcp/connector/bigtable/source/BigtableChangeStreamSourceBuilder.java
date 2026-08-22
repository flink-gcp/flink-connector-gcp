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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.DataClientChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.DefaultChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Builds a {@link BigtableChangeStreamSource}. */
@Public
public final class BigtableChangeStreamSourceBuilder<T> {

    /** Default maximum number of open partition streams in one source subtask. */
    public static final int DEFAULT_MAX_CONCURRENT_STREAMS_PER_SUBTASK = 2;

    @Nullable private TableDestination table;
    @Nullable private BigtableChangeStreamDeserializationSchema<T> deserializer;
    @Nullable private String appProfileId;
    @Nullable private String serviceAccountKeyFile;
    private StartPosition startPosition = StartPosition.latest();
    @Nullable private StartPosition resumeFallback;
    @Nullable private Instant endTime;
    private int maxConcurrentStreamsPerSubtask = DEFAULT_MAX_CONCURRENT_STREAMS_PER_SUBTASK;
    private List<Pattern> familyIncludeList = Collections.emptyList();
    private List<Pattern> familyExcludeList = Collections.emptyList();
    private List<Pattern> qualifierIncludeList = Collections.emptyList();
    private List<Pattern> qualifierExcludeList = Collections.emptyList();
    private boolean skipMessagesWithoutChange;
    @Nullable private ChangeStreamOpener opener;
    @Nullable private ChangeStreamRestoreResolver restoreResolver;
    @Nullable private ChangeStreamCoordinatorClientFactory coordinatorClientFactory;

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
        Preconditions.checkArgument(!appProfileId.isBlank(), "appProfileId must not be blank");
        this.appProfileId = appProfileId;
        return this;
    }

    /**
     * Authenticates Change Streams with the service-account JSON key at the given path instead of
     * application-default credentials. The JobManager reads it when a fresh or restored coordinator
     * starts. Each TaskManager reads it when its reader is created, before any partition is
     * assigned to it. Every eligible process must therefore see the same path.
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

    /**
     * Sets where a partition restarts when its restored position has fallen outside the table's
     * change-stream retention. Optional; unset means such a restore <em>fails</em> the job rather
     * than advancing over records that can no longer be read.
     *
     * <p>It applies per partition, not to the whole restore: only the partitions whose checkpointed
     * position expired restart from here, while the others resume from their continuation tokens.
     * Setting it is a decision to lose the unavailable interval of those partitions rather than to
     * stop, and a position older than the retained window is moved forward to the earliest readable
     * instant.
     *
     * @param resumeFallback the fallback start position
     * @return this builder
     */
    public BigtableChangeStreamSourceBuilder<T> resumeFallback(StartPosition resumeFallback) {
        this.resumeFallback =
                Preconditions.checkNotNull(resumeFallback, "resumeFallback must not be null");
        return this;
    }

    public BigtableChangeStreamSourceBuilder<T> endTime(Instant endTime) {
        this.endTime = Preconditions.checkNotNull(endTime, "endTime must not be null");
        return this;
    }

    /**
     * Bounds the open {@code ReadChangeStream} RPCs in each source subtask. Source parallelism
     * multiplied by this value is the job's configured read capacity, not a Bigtable quota. The
     * default is {@value #DEFAULT_MAX_CONCURRENT_STREAMS_PER_SUBTASK}.
     *
     * @param maximum positive per-subtask stream limit
     * @return this builder
     */
    public BigtableChangeStreamSourceBuilder<T> maxConcurrentStreamsPerSubtask(int maximum) {
        Preconditions.checkArgument(maximum > 0, "maximum must be positive");
        this.maxConcurrentStreamsPerSubtask = maximum;
        return this;
    }

    /**
     * Includes entries whose column-family names fully match at least one Java regular expression.
     *
     * <p>An empty collection disables this filter. It is mutually exclusive with {@link
     * #familyExcludeList(Collection)}.
     */
    public BigtableChangeStreamSourceBuilder<T> familyIncludeList(Collection<String> patterns) {
        this.familyIncludeList = compilePatterns(patterns, "familyIncludeList");
        return this;
    }

    /**
     * Excludes entries whose column-family names fully match at least one Java regular expression.
     *
     * <p>An empty collection disables this filter. It is mutually exclusive with {@link
     * #familyIncludeList(Collection)}.
     */
    public BigtableChangeStreamSourceBuilder<T> familyExcludeList(Collection<String> patterns) {
        this.familyExcludeList = compilePatterns(patterns, "familyExcludeList");
        return this;
    }

    /**
     * Includes qualified columns that fully match at least one Java regular expression.
     *
     * <p>The matched identifier is {@code family:qualifierBase64}, where the qualifier is canonical
     * padded RFC 4648 standard Base64. An empty qualifier therefore produces {@code family:}.
     * Family-delete entries have no qualifier and are governed only by the family filter. An empty
     * collection disables this filter. It is mutually exclusive with {@link
     * #qualifierExcludeList(Collection)}.
     */
    public BigtableChangeStreamSourceBuilder<T> qualifierIncludeList(Collection<String> patterns) {
        this.qualifierIncludeList = compilePatterns(patterns, "qualifierIncludeList");
        return this;
    }

    /**
     * Excludes qualified columns that fully match at least one Java regular expression.
     *
     * <p>The matched identifier uses the same {@code family:qualifierBase64} representation as
     * {@link #qualifierIncludeList(Collection)}. Family-delete entries have no qualifier and are
     * governed only by the family filter. An empty collection disables this filter. It is mutually
     * exclusive with {@link #qualifierIncludeList(Collection)}.
     */
    public BigtableChangeStreamSourceBuilder<T> qualifierExcludeList(Collection<String> patterns) {
        this.qualifierExcludeList = compilePatterns(patterns, "qualifierExcludeList");
        return this;
    }

    /**
     * Skips a mutation when entry filtering removes every entry it reported.
     *
     * <p>The default is {@code false}, which delivers the mutation with an empty entry list so that
     * the atomic row mutation remains visible.
     */
    public BigtableChangeStreamSourceBuilder<T> skipMessagesWithoutChange(boolean skip) {
        this.skipMessagesWithoutChange = skip;
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

    /**
     * Replaces the factory the source mints the enumerator's coordinator client from. For tests
     * that must not reach a service.
     */
    @VisibleForTesting
    BigtableChangeStreamSourceBuilder<T> coordinatorClientFactory(
            ChangeStreamCoordinatorClientFactory coordinatorClientFactory) {
        this.coordinatorClientFactory = coordinatorClientFactory;
        return this;
    }

    public BigtableChangeStreamSource<T> build() {
        Preconditions.checkState(table != null, "A table is required: set table(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required: set deserializer(...).");
        Preconditions.checkState(
                appProfileId != null, "An app profile is required: set appProfileId(...).");
        Preconditions.checkState(
                familyIncludeList.isEmpty() || familyExcludeList.isEmpty(),
                "familyIncludeList(...) and familyExcludeList(...) must not both be set.");
        Preconditions.checkState(
                qualifierIncludeList.isEmpty() || qualifierExcludeList.isEmpty(),
                "qualifierIncludeList(...) and qualifierExcludeList(...) must not both be set.");
        return new BigtableChangeStreamSource<>(
                new BigtableChangeStreamSourceConfig<>(
                        table,
                        deserializer,
                        appProfileId,
                        serviceAccountKeyFile,
                        startPosition,
                        resumeFallback,
                        endTime,
                        maxConcurrentStreamsPerSubtask,
                        new BigtableChangeStreamMutationFilter(
                                familyIncludeList,
                                familyExcludeList,
                                qualifierIncludeList,
                                qualifierExcludeList,
                                skipMessagesWithoutChange),
                        opener != null ? opener : new DataClientChangeStreamOpener(appProfileId),
                        restoreResolver != null
                                ? restoreResolver
                                : new DefaultChangeStreamRestoreResolver(table, appProfileId),
                        coordinatorClientFactory != null
                                ? coordinatorClientFactory
                                : new DefaultChangeStreamCoordinatorClientFactory(
                                        table, appProfileId, serviceAccountKeyFile)));
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
