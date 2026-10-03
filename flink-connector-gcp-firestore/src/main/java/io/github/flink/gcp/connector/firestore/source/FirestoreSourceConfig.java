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

package io.github.flink.gcp.connector.firestore.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlannerFactory;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Everything the source was built with, assembled by the builder and carried into the job graph.
 *
 * <p>Exactly one of {@link #getCollectionGroup()} and {@link #getQueryFactory()} is set: the first
 * is a partitioned scan of a collection group, the second one query read as one split.
 *
 * @param <T> the record type the deserializer produces
 */
@Internal
public final class FirestoreSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final FirestoreDocumentDeserializationSchema<T> deserializer;
    @Nullable private final String collectionGroup;
    @Nullable private final FirestoreQueryFactory queryFactory;
    private final List<String> fieldMask;
    @Nullable private final Integer partitionCount;
    @Nullable private final Instant readTime;
    private final int pageSize;
    @Nullable private final String serviceAccountKeyFile;
    private final QueryPlannerFactory plannerFactory;
    private final QueryPageReader pageReader;

    FirestoreSourceConfig(
            DatabaseDestination database,
            FirestoreDocumentDeserializationSchema<T> deserializer,
            @Nullable String collectionGroup,
            @Nullable FirestoreQueryFactory queryFactory,
            List<String> fieldMask,
            @Nullable Integer partitionCount,
            @Nullable Instant readTime,
            int pageSize,
            @Nullable String serviceAccountKeyFile,
            QueryPlannerFactory plannerFactory,
            QueryPageReader pageReader) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        Preconditions.checkArgument(
                (collectionGroup == null) != (queryFactory == null),
                "exactly one of collectionGroup and queryFactory must be set");
        this.collectionGroup = collectionGroup;
        this.queryFactory = queryFactory;
        Preconditions.checkNotNull(fieldMask, "fieldMask must not be null");
        Preconditions.checkArgument(
                fieldMask.isEmpty() || collectionGroup != null,
                "a field mask applies to a collection-group scan only");
        this.fieldMask = List.copyOf(fieldMask);
        Preconditions.checkArgument(
                partitionCount == null || partitionCount > 0,
                "partitionCount must be positive: %s",
                partitionCount);
        this.partitionCount = partitionCount;
        this.readTime = readTime;
        Preconditions.checkArgument(pageSize > 0, "pageSize must be positive: %s", pageSize);
        this.pageSize = pageSize;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.plannerFactory =
                Preconditions.checkNotNull(plannerFactory, "plannerFactory must not be null");
        this.pageReader = Preconditions.checkNotNull(pageReader, "pageReader must not be null");
    }

    /** Returns the database being read. */
    public DatabaseDestination getDatabase() {
        return database;
    }

    /** Returns the deserializer turning documents into records. */
    public FirestoreDocumentDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /** Returns the collection group to scan, or {@code null} when a query is read instead. */
    @Nullable
    public String getCollectionGroup() {
        return collectionGroup;
    }

    /** Returns the factory of the query to read, or {@code null} when a scan is read instead. */
    @Nullable
    public FirestoreQueryFactory getQueryFactory() {
        return queryFactory;
    }

    /** Returns the field paths a scan projects, empty for every field. */
    public List<String> getFieldMask() {
        return Collections.unmodifiableList(fieldMask);
    }

    /** Returns the partition-count hint, or {@code null} for the enumerator's parallelism. */
    @Nullable
    public Integer getPartitionCount() {
        return partitionCount;
    }

    /** Returns the configured snapshot time, or {@code null} to take the service's at planning. */
    @Nullable
    public Instant getReadTime() {
        return readTime;
    }

    /** Returns the most documents one request asks for. */
    public int getPageSize() {
        return pageSize;
    }

    /** Returns the service-account key-file path, or {@code null} to use ADC. */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /**
     * Returns the factory the source mints one planner per enumerator from.
     *
     * <p>A factory rather than a planner because the JobManager holds one source object for a job's
     * whole life, so a planner here would be shared by every enumerator a coordinator reset builds
     * and the first teardown would refuse every later one ({@code docs/adr/0128}).
     */
    public QueryPlannerFactory getPlannerFactory() {
        return plannerFactory;
    }

    /** Returns the page reader the readers read through; a reader owns and closes it. */
    public QueryPageReader getPageReader() {
        return pageReader;
    }
}
