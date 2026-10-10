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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;

import com.google.datastore.v1.Query;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchEnumeratorState;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.DefaultQueryPlannerFactory;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlannerFactory;
import io.github.flink.gcp.connector.datastore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Builds a {@link DatastoreSource}.
 *
 * <p>Set the database, the deserializer, and exactly one of {@link #kind(String)}, {@link
 * #query(Query)} and {@link #gqlQuery(String)}. Everything else is optional.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public class DatastoreSourceBuilder<T> {

    /** The default number of entities one request asks for. */
    public static final int DEFAULT_PAGE_SIZE = 500;

    /**
     * The most key ranges a read is cut into, configured or estimated; Beam's ceiling. The client
     * library's splitter samples 32 keys per range and holds them all, so a count far above it
     * would exhaust the JobManager's memory before the first read.
     */
    public static final int MAX_SPLIT_COUNT = 50_000;

    private @Nullable DatabaseDestination database;
    private @Nullable DatastoreEntityDeserializationSchema<T> deserializer;
    private @Nullable String kind;
    private @Nullable Query query;
    private @Nullable String gqlQuery;
    private @Nullable String namespace;
    private @Nullable Integer splitCount;
    private @Nullable Instant readTime;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private @Nullable String serviceAccountKeyFile;
    private @Nullable EmulatorEndpoint emulatorEndpoint;
    private @Nullable QueryPlannerFactory plannerFactory;
    private @Nullable QueryPageReader pageReader;

    DatastoreSourceBuilder() {}

    /**
     * Sets the database to read. Required.
     *
     * @param database the database
     * @return this builder
     */
    public DatastoreSourceBuilder<T> database(DatabaseDestination database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets the deserializer turning entities into records. Required.
     *
     * @param deserializer the deserializer
     * @return this builder
     */
    public DatastoreSourceBuilder<T> deserializer(
            DatastoreEntityDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Reads every entity of a kind, in the namespace {@link #namespace(String)} names. The read is
     * cut into key ranges, read in parallel. Set exactly one of this, {@link #query(Query)} and
     * {@link #gqlQuery(String)}.
     *
     * <p>The metadata kinds {@code __namespace__}, {@code __kind__} and {@code __property__} are
     * refused. Google documents their entities as generated dynamically from the database's current
     * state, so a result need not be the snapshot at the read time that the source pages through
     * and resumes in, and on the emulator a query of {@code __kind__} or {@code __namespace__}
     * answers without the cursors the source pages and resumes by. Read metadata with the client
     * library instead. The statistics kinds, such as {@code __Stat_Kind__}, are not refused.
     *
     * @param kind the kind, for example {@code Task}
     * @return this builder
     * @throws IllegalArgumentException if the kind is blank or a metadata kind
     */
    public DatastoreSourceBuilder<T> kind(String kind) {
        Preconditions.checkNotNull(kind, "kind must not be null");
        Preconditions.checkArgument(!kind.isBlank(), "kind must not be blank");
        String reason = SplittableQueries.whyKindNotReadable(kind);
        Preconditions.checkArgument(reason == null, "The source cannot read the kind: %s.", reason);
        this.kind = kind;
        return this;
    }

    /**
     * Reads one query, in the namespace {@link #namespace(String)} names. Set exactly one of this,
     * {@link #kind(String)} and {@link #gqlQuery(String)}.
     *
     * <p>The query is the Datastore API's protobuf message, the form the service and the client
     * library's query splitter take. A query that names exactly one kind and filters only with
     * equality ({@code EQUAL}) and ancestor ({@code HAS_ANCESTOR}) filters, combined with {@code
     * AND}, is cut into key ranges and read in parallel. Any other query is read as one split: one
     * with an ordering, a {@code limit}, an {@code offset}, a cursor, a {@code DISTINCT ON}, an
     * {@code OR}, or a filter with another operator or of no type, or one naming no kind or
     * several. Projections are read as the query states them. A nearest-neighbour search ({@code
     * find_nearest}) is refused: the service applies a cursor and a limit before the search, so the
     * source's paging would change what it finds. A query of a metadata kind is refused, as {@link
     * #kind(String)} refuses one.
     *
     * <p>This is {@code com.google.datastore.v1.Query}, not the client library's {@code
     * com.google.cloud.datastore.Query}, whose conversion to this form is not public. The {@code
     * com.google.datastore.v1.client.DatastoreHelper} class of the {@code
     * datastore-v1-proto-client} library, which the client library brings, has helpers for building
     * filters and values.
     *
     * @param query the query
     * @return this builder
     * @throws IllegalArgumentException if the query is a nearest-neighbour search or names a
     *     metadata kind
     */
    public DatastoreSourceBuilder<T> query(Query query) {
        Preconditions.checkNotNull(query, "query must not be null");
        String reason = SplittableQueries.whyNotReadable(query);
        Preconditions.checkArgument(
                reason == null, "The source cannot read the query: %s.", reason);
        this.query = query;
        return this;
    }

    /**
     * Reads one GQL query, in the namespace {@link #namespace(String)} names. Set exactly one of
     * this, {@link #kind(String)} and {@link #query(Query)}.
     *
     * <p>The service parses the query when the source plans the read, and the source reads the
     * query it parsed into, by the rules {@link #query(Query)} states, so a query of a metadata
     * kind or a nearest-neighbour search fails the job when the read is planned. Literals are
     * allowed; bindings are not. To parse it without reading any entity, the source asks for the
     * query with {@code LIMIT 0} appended; a query that already ends in a {@code LIMIT} or an
     * {@code OFFSET} clause cannot take that clause, so the source parses it by running it as
     * written, which reads and bills its first batch of entities once more.
     *
     * @param gqlQuery the GQL query, for example {@code SELECT * FROM Task WHERE done = false}
     * @return this builder
     * @throws IllegalArgumentException if the query is blank
     */
    public DatastoreSourceBuilder<T> gqlQuery(String gqlQuery) {
        Preconditions.checkNotNull(gqlQuery, "gqlQuery must not be null");
        Preconditions.checkArgument(!gqlQuery.isBlank(), "gqlQuery must not be blank");
        this.gqlQuery = gqlQuery;
        return this;
    }

    /**
     * Sets the namespace to read. Optional; the default namespace is read when unset.
     *
     * @param namespace the namespace id
     * @return this builder
     * @throws IllegalArgumentException if the namespace is blank; leave it unset for the default
     *     namespace
     */
    public DatastoreSourceBuilder<T> namespace(String namespace) {
        Preconditions.checkNotNull(namespace, "namespace must not be null");
        Preconditions.checkArgument(
                !namespace.isBlank(),
                "namespace must not be blank; leave it unset to read the default namespace");
        this.namespace = namespace;
        return this;
    }

    /**
     * Sets how many key ranges to cut the read into. Optional; applies only to a query that can be
     * split.
     *
     * <p>When unset, the source estimates the count from the kind's size in the database's
     * statistics, one split per 64 MiB, and takes at least 12 and at least the source's
     * parallelism, and at most 50,000. The statistics are updated about once a day and are absent
     * for a kind that is new, so the estimate then falls back to that lower bound.
     *
     * <p>It is an upper bound: the client library's splitter samples the kind's keys through the
     * {@code __scatter__} property and cuts fewer ranges when the kind is small. A count above the
     * parallelism lets a subtask that finishes early take another range while a slower one is still
     * reading.
     *
     * @param splitCount the number of key ranges to ask for, from 1 to {@value #MAX_SPLIT_COUNT}
     * @return this builder
     * @throws IllegalArgumentException if the count is not positive or exceeds {@value
     *     #MAX_SPLIT_COUNT}
     */
    public DatastoreSourceBuilder<T> splitCount(int splitCount) {
        Preconditions.checkArgument(
                splitCount > 0 && splitCount <= MAX_SPLIT_COUNT,
                "splitCount must be between 1 and %s: %s",
                MAX_SPLIT_COUNT,
                splitCount);
        this.splitCount = splitCount;
        return this;
    }

    /**
     * Sets the snapshot time to read at. Optional; when unset, the source takes the service's
     * current time when it plans the read.
     *
     * <p>Every split reads at this one time. The service keeps old versions for an hour, or for
     * seven days with point-in-time recovery enabled, where a time older than an hour must fall on
     * a whole minute. A time outside that window fails the job when it plans the read — and the
     * window keeps moving while the job runs, so a read that outlasts it, or a restore from a
     * checkpoint older than it, fails too. The default is the service's time to the microsecond,
     * good for an hour: a read that may take longer needs point-in-time recovery and a whole-minute
     * time set here.
     *
     * <p>The time is truncated to the microsecond, because the service accepts only a
     * microsecond-precision read time, so a finer instant could never be read as given, and an
     * {@code Instant.now()} can carry nanoseconds. Truncation picks the latest microsecond at or
     * before the one given. The Datastore API does not document the precision of update times; on
     * the emulator they are whole microseconds, so there that time sees every write made at or
     * before the one given.
     *
     * @param readTime the snapshot time, truncated to the microsecond
     * @return this builder
     */
    public DatastoreSourceBuilder<T> readTime(Instant readTime) {
        this.readTime =
                Preconditions.checkNotNull(readTime, "readTime must not be null")
                        .truncatedTo(ChronoUnit.MICROS);
        return this;
    }

    /**
     * Sets how many entities one request asks for. Optional; defaults to {@value
     * #DEFAULT_PAGE_SIZE}.
     *
     * <p>A request's entities are held in memory whole before any is handed on, so the page size
     * times the largest entity bounds what one fetch holds. The service may answer a request with
     * fewer entities, and the next request continues where it stopped. Lower it for large entities;
     * raise it to make fewer requests for small ones.
     *
     * @param pageSize the positive number of entities per request
     * @return this builder
     */
    public DatastoreSourceBuilder<T> pageSize(int pageSize) {
        Preconditions.checkArgument(pageSize > 0, "pageSize must be positive: %s", pageSize);
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Authenticates the source with the service-account JSON key at the given path instead of
     * application-default credentials. The JobManager reads the file when its enumerator is created
     * or restored, and each TaskManager reads it when its reader is created, so every process that
     * can run the source must see the same path. Optional; when unset the real-service path uses
     * application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel carries no credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public DatastoreSourceBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the source at a Firestore emulator running in Datastore mode instead of the real
     * service. Optional; for tests.
     *
     * <p>The emulator needs no credentials, so setting this also stops the client from looking for
     * any. This setting is the only way the source reaches an emulator: the client library's {@code
     * DATASTORE_EMULATOR_HOST} environment variable never chooses the endpoint. The emulator
     * answers the splitter's {@code __scatter__} sampling with no keys and keeps no statistics, so
     * a read against it is never cut into more than one key range.
     *
     * @param emulatorEndpoint the emulator's endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public DatastoreSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /** Replaces the factory the source mints the enumerator's planner from. For tests. */
    @VisibleForTesting
    DatastoreSourceBuilder<T> plannerFactory(QueryPlannerFactory plannerFactory) {
        this.plannerFactory = plannerFactory;
        return this;
    }

    /** Replaces the page reader the readers read through. For tests. */
    @VisibleForTesting
    DatastoreSourceBuilder<T> pageReader(QueryPageReader pageReader) {
        this.pageReader = pageReader;
        return this;
    }

    /**
     * Builds the source.
     *
     * @return the source
     * @throws IllegalStateException if a required option is missing, not exactly one of {@code
     *     kind}, {@code query} and {@code gqlQuery} is set, a split count is set for a query that
     *     cannot be split, or both {@code serviceAccountKeyFile} and {@code emulatorEndpoint} are
     *     set
     */
    public Source<T, QuerySplit, DatastoreBatchEnumeratorState> build() {
        Preconditions.checkState(
                database != null, "A database is required. Set it with database(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required. Set it with deserializer(...).");
        int readShapes =
                (kind != null ? 1 : 0) + (query != null ? 1 : 0) + (gqlQuery != null ? 1 : 0);
        Preconditions.checkState(
                readShapes == 1, "Set exactly one of kind(...), query(...) and gqlQuery(...).");
        Query resolved = kind != null ? SplittableQueries.ofKind(kind) : query;
        if (splitCount != null && resolved != null) {
            String reason = SplittableQueries.whyNotSplittable(resolved);
            Preconditions.checkState(
                    reason == null,
                    "splitCount(...) applies to a query that can be split, and this one is read as"
                            + " one split because %s.",
                    reason);
        }
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        return new DatastoreBatchSource<>(
                new DatastoreSourceConfig<>(
                        database,
                        deserializer,
                        resolved,
                        gqlQuery,
                        namespace != null ? namespace : "",
                        splitCount,
                        readTime,
                        pageSize,
                        serviceAccountKeyFile,
                        plannerFactory != null
                                ? plannerFactory
                                : new DefaultQueryPlannerFactory(emulatorEndpoint),
                        pageReader != null
                                ? pageReader
                                : new ClientQueryPageReader(emulatorEndpoint)));
    }
}
