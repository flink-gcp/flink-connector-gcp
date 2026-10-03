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

package io.github.flink.gcp.connector.datastore.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.datastore.v1.Query;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlannerFactory;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;

/**
 * Everything the source was built with, assembled by the builder and carried into the job graph.
 *
 * <p>Exactly one of {@link #getQuery()} and {@link #getGqlQuery()} is set: a kind scan is held as
 * the query that reads the kind, and a GQL query is resolved into a query when the read is planned.
 *
 * @param <T> the record type the deserializer produces
 */
@Internal
public final class DatastoreSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final DatastoreEntityDeserializationSchema<T> deserializer;
    @Nullable private final Query query;
    @Nullable private final String gqlQuery;
    private final String namespace;
    @Nullable private final Integer splitCount;
    @Nullable private final Instant readTime;
    private final int pageSize;
    @Nullable private final String serviceAccountKeyFile;
    private final QueryPlannerFactory plannerFactory;
    private final QueryPageReader pageReader;

    DatastoreSourceConfig(
            DatabaseDestination database,
            DatastoreEntityDeserializationSchema<T> deserializer,
            @Nullable Query query,
            @Nullable String gqlQuery,
            String namespace,
            @Nullable Integer splitCount,
            @Nullable Instant readTime,
            int pageSize,
            @Nullable String serviceAccountKeyFile,
            QueryPlannerFactory plannerFactory,
            QueryPageReader pageReader) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        Preconditions.checkArgument(
                (query == null) != (gqlQuery == null),
                "exactly one of query and gqlQuery must be set");
        Preconditions.checkArgument(
                splitCount == null
                        || (splitCount > 0 && splitCount <= DatastoreSourceBuilder.MAX_SPLIT_COUNT),
                "splitCount must be between 1 and %s: %s",
                DatastoreSourceBuilder.MAX_SPLIT_COUNT,
                splitCount);
        Preconditions.checkArgument(
                splitCount == null
                        || query == null
                        || SplittableQueries.whyNotSplittable(query) == null,
                "a split count applies to a query that can be split");
        this.query = query;
        this.gqlQuery = gqlQuery;
        this.namespace = Preconditions.checkNotNull(namespace, "namespace must not be null");
        this.splitCount = splitCount;
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

    /** Returns the deserializer turning entities into records. */
    public DatastoreEntityDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /** Returns the query to read, or {@code null} when a GQL query is read instead. */
    @Nullable
    public Query getQuery() {
        return query;
    }

    /** Returns the GQL query to read, or {@code null} when a query is read instead. */
    @Nullable
    public String getGqlQuery() {
        return gqlQuery;
    }

    /** Returns the namespace read, empty for the default namespace. */
    public String getNamespace() {
        return namespace;
    }

    /** Returns the configured split count, or {@code null} to estimate one. */
    @Nullable
    public Integer getSplitCount() {
        return splitCount;
    }

    /** Returns the configured snapshot time, or {@code null} to take the service's at planning. */
    @Nullable
    public Instant getReadTime() {
        return readTime;
    }

    /** Returns the most entities one request asks for. */
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
